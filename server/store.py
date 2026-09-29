"""Server-side durable SQLite store for receiving edge synchronization records."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import sqlite3
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger("lifeline.server.store")


class ServerMemoryStore:
    """Server-side transactional store maintaining global truth, revisions, and snapshots."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            check_same_thread=False,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self) -> None:
        with self._lock:
            conn = self._get_connection()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                # Server records table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS server_records (
                        record_id TEXT PRIMARY KEY,
                        current_version INTEGER NOT NULL,
                        entity_id TEXT NOT NULL,
                        context_id TEXT NOT NULL,
                        device_id TEXT NOT NULL,
                        observation_json TEXT NOT NULL,
                        dense_vector_json TEXT,
                        sparse_indices_json TEXT,
                        sparse_values_json TEXT,
                        status TEXT NOT NULL,
                        tombstone INTEGER NOT NULL DEFAULT 0,
                        observed_at TEXT NOT NULL,
                        recorded_at TEXT NOT NULL,
                        server_received_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_srv_context ON server_records(context_id);")

                # Server operation deduplication log
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS server_operations_log (
                        operation_id TEXT PRIMARY KEY,
                        record_id TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        device_id TEXT NOT NULL,
                        received_at TEXT NOT NULL,
                        content_hash TEXT NOT NULL
                    );
                """)

                # Server prepared snapshots catalog
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS server_snapshots (
                        snapshot_id TEXT PRIMARY KEY,
                        context_id TEXT NOT NULL,
                        manifest_version TEXT NOT NULL,
                        archive_path TEXT NOT NULL,
                        checksum_sha256 TEXT NOT NULL,
                        record_count INTEGER NOT NULL,
                        manifest_json TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );
                """)

                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

    def ingest_batch(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Atomically ingest and deduplicate incoming batch of edge operations."""
        receipts = []
        with self._lock:
            conn = self._get_connection()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                now_iso = datetime.now(timezone.utc).isoformat()

                for item in items:
                    op_id = item["operation_id"]
                    rec_id = item["record_id"]
                    device_id = item.get("device_id", "unknown")
                    version = int(item.get("version", 1))
                    tombstone = bool(item.get("tombstone", False))
                    raw_hash = hashlib.sha256(json.dumps(item.get("observation", {}), sort_keys=True).encode()).hexdigest()

                    # Deduplication check
                    existing_op = conn.execute(
                        "SELECT * FROM server_operations_log WHERE operation_id = ?;",
                        (op_id,),
                    ).fetchone()

                    if existing_op:
                        receipts.append({
                            "operation_id": op_id,
                            "record_id": rec_id,
                            "status": "durably_accepted",
                            "server_version": existing_op["version"],
                            "received_at": existing_op["received_at"],
                            "deduplicated": True,
                        })
                        continue

                    # Insert operation log
                    conn.execute(
                        """
                        INSERT INTO server_operations_log (
                            operation_id, record_id, version, device_id, received_at, content_hash
                        ) VALUES (?, ?, ?, ?, ?, ?);
                        """,
                        (op_id, rec_id, version, device_id, now_iso, raw_hash),
                    )

                    # Update server record
                    conn.execute(
                        """
                        INSERT INTO server_records (
                            record_id, current_version, entity_id, context_id, device_id,
                            observation_json, dense_vector_json, sparse_indices_json, sparse_values_json,
                            status, tombstone, observed_at, recorded_at, server_received_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(record_id) DO UPDATE SET
                            current_version = excluded.current_version,
                            observation_json = excluded.observation_json,
                            dense_vector_json = excluded.dense_vector_json,
                            sparse_indices_json = excluded.sparse_indices_json,
                            sparse_values_json = excluded.sparse_values_json,
                            status = excluded.status,
                            tombstone = excluded.tombstone,
                            updated_at = excluded.updated_at
                        WHERE excluded.current_version >= server_records.current_version;
                        """,
                        (
                            rec_id,
                            version,
                            item.get("entity_id", f"entity_{rec_id}"),
                            item.get("context_id", "zone_01"),
                            device_id,
                            json.dumps(item.get("observation", {})),
                            json.dumps(item.get("dense_vector")) if item.get("dense_vector") else None,
                            json.dumps(item.get("sparse_indices")) if item.get("sparse_indices") else None,
                            json.dumps(item.get("sparse_values")) if item.get("sparse_values") else None,
                            "tombstone" if tombstone else "active",
                            1 if tombstone else 0,
                            item.get("observed_at", now_iso),
                            item.get("recorded_at", now_iso),
                            now_iso,
                            now_iso,
                        ),
                    )

                    receipts.append({
                        "operation_id": op_id,
                        "record_id": rec_id,
                        "status": "durably_accepted",
                        "server_version": version,
                        "received_at": now_iso,
                        "deduplicated": False,
                    })

                conn.execute("COMMIT;")
                return receipts
            except Exception:
                conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

    def get_records_for_context(self, context_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._get_connection()
            try:
                rows = conn.execute(
                    "SELECT * FROM server_records WHERE context_id = ? AND tombstone = 0;",
                    (context_id,),
                ).fetchall()
                records = []
                for r in rows:
                    records.append({
                        "record_id": r["record_id"],
                        "version": r["current_version"],
                        "entity_id": r["entity_id"],
                        "context_id": r["context_id"],
                        "observation": json.loads(r["observation_json"]),
                        "dense_vector": json.loads(r["dense_vector_json"]) if r["dense_vector_json"] else None,
                        "sparse_indices": json.loads(r["sparse_indices_json"]) if r["sparse_indices_json"] else None,
                        "sparse_values": json.loads(r["sparse_values_json"]) if r["sparse_values_json"] else None,
                    })
                return records
            finally:
                conn.close()

    def record_snapshot(
        self,
        snapshot_id: str,
        context_id: str,
        manifest_version: str,
        archive_path: str,
        checksum_sha256: str,
        record_count: int,
        manifest_json: str,
    ) -> None:
        with self._lock:
            conn = self._get_connection()
            try:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO server_snapshots (
                        snapshot_id, context_id, manifest_version, archive_path,
                        checksum_sha256, record_count, manifest_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        snapshot_id,
                        context_id,
                        manifest_version,
                        archive_path,
                        checksum_sha256,
                        record_count,
                        manifest_json,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
            finally:
                conn.close()

    def get_latest_snapshot(self, context_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._get_connection()
            try:
                row = conn.execute(
                    """
                    SELECT * FROM server_snapshots
                    WHERE context_id = ?
                    ORDER BY created_at DESC LIMIT 1;
                    """,
                    (context_id,),
                ).fetchone()
                return dict(row) if row else None
            finally:
                conn.close()
