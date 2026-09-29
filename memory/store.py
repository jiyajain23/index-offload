"""Transactional SQLite local store for LIFELINE memory, revisions, outbox, and conflicts."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from shared.contracts import (
    PriorityLevel,
    RecordEnvelope,
    RecordStatus,
    SharingPolicy,
    WriteReceipt,
)

logger = logging.getLogger("lifeline.memory.store")


class StorageCapacityError(RuntimeError):
    """Raised when storage bounds or urgent reserves are breached."""


class IdempotencyConflictError(RuntimeError):
    """Raised when an operation ID is reused with conflicting content."""


class SQLiteMemoryStore:
    """Thread-safe transactional store enforcing atomic acceptance of records and outbox."""

    def __init__(
        self,
        db_path: str | Path,
        max_durable_bytes: int = 50 * 1024 * 1024,  # 50 MB default edge limit
        urgent_reserve_percent: float = 0.20,         # 20% reserved for urgent records
    ):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.max_durable_bytes = max_durable_bytes
        self.urgent_reserve_bytes = int(max_durable_bytes * urgent_reserve_percent)
        self.regular_capacity_bytes = max_durable_bytes - self.urgent_reserve_bytes

        self._lock = threading.RLock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            check_same_thread=False,
            isolation_level=None,  # Autocommit mode; explicit BEGIN/COMMIT used
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_db(self) -> None:
        with self._lock:
            conn = self._get_connection()
            try:
                conn.execute("BEGIN IMMEDIATE;")

                # Schema versioning
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS schema_version (
                        version INTEGER PRIMARY KEY,
                        migrated_at TEXT NOT NULL
                    );
                """)

                # 1. Authoritative Current Records
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS current_records (
                        record_id TEXT PRIMARY KEY,
                        current_version INTEGER NOT NULL,
                        entity_id TEXT NOT NULL,
                        context_id TEXT NOT NULL,
                        device_id TEXT NOT NULL,
                        observation_json TEXT NOT NULL,
                        source_type TEXT NOT NULL,
                        source_reference TEXT,
                        status TEXT NOT NULL,
                        tombstone INTEGER NOT NULL DEFAULT 0,
                        sharing TEXT NOT NULL,
                        priority TEXT NOT NULL,
                        confidence REAL,
                        confidence_source TEXT,
                        observed_at TEXT NOT NULL,
                        recorded_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        approx_bytes INTEGER NOT NULL DEFAULT 0
                    );
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_records_entity ON current_records(entity_id);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_records_context ON current_records(context_id);")

                # 2. Immutable Operations History (Audit Log & Idempotency)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS operations_log (
                        operation_id TEXT PRIMARY KEY,
                        record_id TEXT NOT NULL,
                        entity_id TEXT NOT NULL,
                        context_id TEXT NOT NULL,
                        device_id TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        parent_version INTEGER,
                        status TEXT NOT NULL,
                        tombstone INTEGER NOT NULL DEFAULT 0,
                        sharing TEXT NOT NULL,
                        priority TEXT NOT NULL,
                        observation_json TEXT NOT NULL,
                        dense_vector_json TEXT,
                        sparse_indices_json TEXT,
                        sparse_values_json TEXT,
                        observed_at TEXT NOT NULL,
                        recorded_at TEXT NOT NULL,
                        resolution_of_json TEXT,
                        content_hash TEXT NOT NULL
                    );
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_op_record ON operations_log(record_id);")

                # 3. Contested Divergent Edits / Conflicts
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS conflict_records (
                        conflict_id TEXT PRIMARY KEY,
                        record_id TEXT NOT NULL,
                        operation_id TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        parent_version INTEGER,
                        competing_operation_id TEXT NOT NULL,
                        observation_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'contested'
                    );
                """)

                # 4. Tombstones Registry
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS tombstones (
                        record_id TEXT PRIMARY KEY,
                        version INTEGER NOT NULL,
                        entity_id TEXT NOT NULL,
                        deleted_at TEXT NOT NULL,
                        deleted_by_operation TEXT NOT NULL
                    );
                """)

                # 5. Projection-Pending Queue (for Edge rebuild / retries)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS projection_pending (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        record_id TEXT NOT NULL,
                        operation_id TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        context_id TEXT NOT NULL,
                        is_tombstone INTEGER NOT NULL DEFAULT 0,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        last_error TEXT,
                        created_at TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        UNIQUE(operation_id)
                    );
                """)
                # Older databases keyed pending work by (record_id, version), which
                # silently replaced one branch of a concurrent edit. Preserve all
                # queued operations when migrating that schema.
                table_sql = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='projection_pending'"
                ).fetchone()[0]
                if "UNIQUE(record_id, version)" in table_sql:
                    conn.execute("ALTER TABLE projection_pending RENAME TO projection_pending_old")
                    conn.execute("""CREATE TABLE projection_pending (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        record_id TEXT NOT NULL, operation_id TEXT NOT NULL UNIQUE,
                        version INTEGER NOT NULL, context_id TEXT NOT NULL,
                        is_tombstone INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
                        last_error TEXT, created_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending'
                    )""")
                    conn.execute("""INSERT INTO projection_pending
                        (id, record_id, operation_id, version, context_id, is_tombstone,
                         attempts, last_error, created_at, status)
                        SELECT id, record_id, operation_id, version, context_id, is_tombstone,
                               attempts, last_error, created_at, status
                        FROM projection_pending_old""")
                    conn.execute("DROP TABLE projection_pending_old")

                # 6. Upload Outbox (for Server Sync)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS outbox (
                        operation_id TEXT PRIMARY KEY,
                        record_id TEXT NOT NULL,
                        priority TEXT NOT NULL,
                        sharing TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'queued',
                        attempts INTEGER NOT NULL DEFAULT 0,
                        next_retry_at REAL NOT NULL DEFAULT 0.0,
                        created_at TEXT NOT NULL,
                        sent_at TEXT,
                        acked_at TEXT,
                        last_error TEXT
                    );
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_queue ON outbox(status, priority, created_at);")

                # 7. Installed Server Snapshots & Verified Revisions
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS installed_snapshots (
                        snapshot_id TEXT PRIMARY KEY,
                        target_shard_id TEXT NOT NULL,
                        manifest_version TEXT NOT NULL,
                        installed_at TEXT NOT NULL,
                        record_count INTEGER NOT NULL DEFAULT 0
                    );
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS installed_revisions (
                        record_id TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        snapshot_id TEXT NOT NULL,
                        installed_at TEXT NOT NULL,
                        PRIMARY KEY(record_id, version)
                    );
                """)

                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

    def get_approximate_usage_bytes(self) -> int:
        """Calculate durable stored payload bytes plus database file size."""
        with self._lock:
            conn = self._get_connection()
            try:
                row = conn.execute("SELECT COALESCE(SUM(approx_bytes), 0) as total FROM current_records;").fetchone()
                payload_bytes = row["total"] if row else 0
                file_size = self.db_path.stat().st_size if self.db_path.exists() else 0
                return payload_bytes + file_size
            finally:
                conn.close()

    def commit_record(self, record: RecordEnvelope) -> Tuple[WriteReceipt, bool]:
        """Atomically persist record state, outbox, and projection work in one transaction.

        Returns (WriteReceipt, is_contested_divergent).
        """
        with self._lock:
            # 1. Check Idempotency First
            conn = self._get_connection()
            try:
                conn.execute("BEGIN IMMEDIATE;")

                existing_op = conn.execute(
                    "SELECT content_hash, version FROM operations_log WHERE operation_id = ?;",
                    (record.operation_id,),
                ).fetchone()

                incoming_hash = self._compute_content_hash(record)
                if existing_op:
                    if existing_op["content_hash"] != incoming_hash:
                        raise IdempotencyConflictError(
                            f"Operation ID {record.operation_id} reused with differing content"
                        )
                    # Return previous logical success (idempotent replay)
                    conn.execute("COMMIT;")
                    return (
                        WriteReceipt(
                            record_id=record.record_id,
                            operation_id=record.operation_id,
                            version=existing_op["version"],
                            durable=True,
                            projection_status="ready",
                        ),
                        False,
                    )

                # 2. Check Capacity & Urgent Reserve Policy
                current_usage = self.get_approximate_usage_bytes()
                record_bytes = len(json.dumps(record.observation)) + (len(record.dense_vector or []) * 4) + 512

                if record.priority == PriorityLevel.ROUTINE:
                    if current_usage + record_bytes > self.regular_capacity_bytes:
                        raise StorageCapacityError(
                            f"Routine capacity exhausted ({current_usage / 1024:.1f} KB / "
                            f"{self.regular_capacity_bytes / 1024:.1f} KB). Urgent reserve protected."
                        )
                else:  # URGENT
                    if current_usage + record_bytes > self.max_durable_bytes:
                        raise StorageCapacityError(
                            f"Urgent storage reserve exhausted ({current_usage / 1024:.1f} KB / "
                            f"{self.max_durable_bytes / 1024:.1f} KB)"
                        )

                # 3. Check Revision & Conflict Semantics
                existing_record = conn.execute(
                    "SELECT * FROM current_records WHERE record_id = ?;",
                    (record.record_id,),
                ).fetchone()

                existing_tombstone = conn.execute(
                    "SELECT * FROM tombstones WHERE record_id = ?;",
                    (record.record_id,),
                ).fetchone()

                is_contested = False
                version_to_set = record.version

                if existing_tombstone and not record.tombstone:
                    # Replayed older data cannot resurrect a deleted record
                    if record.version <= existing_tombstone["version"]:
                        conn.execute("COMMIT;")
                        return (
                            WriteReceipt(
                                record_id=record.record_id,
                                operation_id=record.operation_id,
                                version=record.version,
                                durable=True,
                                projection_status="ignored_tombstoned",
                            ),
                            False,
                        )

                if existing_record:
                    curr_ver = existing_record["current_version"]
                    parent = record.parent_version

                    if parent is None:
                        # Direct edit without parent or concurrent branch: contested!
                        is_contested = True
                    elif parent == curr_ver:
                        # Valid sequential revision supersedes parent
                        version_to_set = curr_ver + 1
                    elif parent < curr_ver:
                        # Fork / divergent edit based on an older parent: contested!
                        is_contested = True
                    else:
                        # Out of order future parent: contested
                        is_contested = True

                # If record explicitly resolves previous conflicts
                if record.resolution_of:
                    is_contested = False
                    # Close out resolved conflicts
                    for res_op in record.resolution_of:
                        conn.execute(
                            "UPDATE conflict_records SET status = 'resolved' WHERE operation_id = ? OR competing_operation_id = ?;",
                            (res_op, res_op),
                        )

                record.version = version_to_set
                status_to_store = RecordStatus.CONTESTED.value if is_contested else record.status.value

                # 4. Insert into Operations Log (Audit trail)
                conn.execute(
                    """
                    INSERT INTO operations_log (
                        operation_id, record_id, entity_id, context_id, device_id,
                        version, parent_version, status, tombstone, sharing, priority,
                        observation_json, dense_vector_json, sparse_indices_json, sparse_values_json,
                        observed_at, recorded_at, resolution_of_json, content_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        record.operation_id,
                        record.record_id,
                        record.entity_id,
                        record.context_id,
                        record.device_id,
                        version_to_set,
                        record.parent_version,
                        status_to_store,
                        1 if record.tombstone else 0,
                        record.sharing.value,
                        record.priority.value,
                        json.dumps(record.observation),
                        json.dumps(record.dense_vector) if record.dense_vector else None,
                        json.dumps(record.sparse_indices) if record.sparse_indices else None,
                        json.dumps(record.sparse_values) if record.sparse_values else None,
                        record.observed_at.isoformat(),
                        record.recorded_at.isoformat(),
                        json.dumps(record.resolution_of),
                        incoming_hash,
                    ),
                )

                # 5. Insert/Update Current Records Table
                if is_contested and existing_record:
                    # Record the conflict
                    conflict_id = f"conf_{record.record_id}_{record.operation_id}"
                    competing = conn.execute(
                        "SELECT operation_id FROM operations_log WHERE record_id = ? "
                        "AND operation_id != ? ORDER BY rowid DESC LIMIT 1",
                        (record.record_id, record.operation_id),
                    ).fetchone()
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO conflict_records (
                            conflict_id, record_id, operation_id, version, parent_version,
                            competing_operation_id, observation_json, created_at, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'contested');
                        """,
                        (
                            conflict_id,
                            record.record_id,
                            record.operation_id,
                            version_to_set,
                            record.parent_version,
                            competing["operation_id"] if competing else record.operation_id,
                            json.dumps(record.observation),
                            datetime.now(timezone.utc).isoformat(),
                        ),
                    )
                    # Mark current record contested
                    conn.execute(
                        "UPDATE current_records SET status = 'contested' WHERE record_id = ?;",
                        (record.record_id,),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO current_records (
                            record_id, current_version, entity_id, context_id, device_id,
                            observation_json, source_type, source_reference, status, tombstone,
                            sharing, priority, confidence, confidence_source, observed_at, recorded_at,
                            updated_at, approx_bytes
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(record_id) DO UPDATE SET
                            current_version = excluded.current_version,
                            observation_json = excluded.observation_json,
                            status = excluded.status,
                            tombstone = excluded.tombstone,
                            sharing = excluded.sharing,
                            priority = excluded.priority,
                            confidence = excluded.confidence,
                            confidence_source = excluded.confidence_source,
                            updated_at = excluded.updated_at,
                            approx_bytes = excluded.approx_bytes;
                        """,
                        (
                            record.record_id,
                            version_to_set,
                            record.entity_id,
                            record.context_id,
                            record.device_id,
                            json.dumps(record.observation),
                            record.source_type,
                            record.source_reference,
                            status_to_store,
                            1 if record.tombstone else 0,
                            record.sharing.value,
                            record.priority.value,
                            record.confidence,
                            record.confidence_source,
                            record.observed_at.isoformat(),
                            record.recorded_at.isoformat(),
                            datetime.now(timezone.utc).isoformat(),
                            record_bytes,
                        ),
                    )

                # 6. Tombstone Tracking
                if record.tombstone:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO tombstones (
                            record_id, version, entity_id, deleted_at, deleted_by_operation
                        ) VALUES (?, ?, ?, ?, ?);
                        """,
                        (
                            record.record_id,
                            version_to_set,
                            record.entity_id,
                            datetime.now(timezone.utc).isoformat(),
                            record.operation_id,
                        ),
                    )

                # 7. Projection-Pending Work
                conn.execute(
                    """
                    INSERT OR IGNORE INTO projection_pending (
                        record_id, operation_id, version, context_id, is_tombstone, created_at, status
                    ) VALUES (?, ?, ?, ?, ?, ?, 'pending');
                    """,
                    (
                        record.record_id,
                        record.operation_id,
                        version_to_set,
                        record.context_id,
                        1 if record.tombstone else 0,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )

                # 8. Outbox Entry (Strict Privacy Enforcement)
                # CRITICAL: Local-only records must NEVER enter outbox!
                if record.sharing != SharingPolicy.LOCAL_ONLY:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO outbox (
                            operation_id, record_id, priority, sharing, status, created_at
                        ) VALUES (?, ?, ?, ?, 'queued', ?);
                        """,
                        (
                            record.operation_id,
                            record.record_id,
                            record.priority.value,
                            record.sharing.value,
                            datetime.now(timezone.utc).isoformat(),
                        ),
                    )

                conn.execute("COMMIT;")
                return (
                    WriteReceipt(
                        record_id=record.record_id,
                        operation_id=record.operation_id,
                        version=version_to_set,
                        durable=True,
                        projection_status="pending",
                    ),
                    is_contested,
                )
            except Exception:
                conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

    def mark_projection_completed(self, record_id: str, version: int,
                                  operation_id: Optional[str] = None) -> None:
        with self._lock:
            conn = self._get_connection()
            try:
                if operation_id:
                    conn.execute("DELETE FROM projection_pending WHERE operation_id = ?", (operation_id,))
                else:
                    conn.execute("DELETE FROM projection_pending WHERE record_id = ? AND version <= ?",
                                 (record_id, version))
            finally:
                conn.close()

    def get_pending_projections(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._get_connection()
            try:
                rows = conn.execute(
                    """
                    SELECT p.*, o.dense_vector_json, o.sparse_indices_json, o.sparse_values_json, o.observation_json
                    FROM projection_pending p
                    JOIN operations_log o ON p.operation_id = o.operation_id
                    WHERE p.status = 'pending'
                    ORDER BY p.id ASC LIMIT ?;
                    """,
                    (limit,),
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

    def get_record(self, record_id: str) -> Optional[RecordEnvelope]:
        with self._lock:
            conn = self._get_connection()
            try:
                row = conn.execute("SELECT * FROM current_records WHERE record_id = ?;", (record_id,)).fetchone()
                if not row:
                    return None
                return self._row_to_envelope(row)
            finally:
                conn.close()

    def get_conflicts(self) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._get_connection()
            try:
                rows = conn.execute("SELECT * FROM conflict_records WHERE status = 'contested';").fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

    def get_tombstone(self, record_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._get_connection()
            try:
                row = conn.execute("SELECT * FROM tombstones WHERE record_id = ?;", (record_id,)).fetchone()
                return dict(row) if row else None
            finally:
                conn.close()

    # -------------------------------------------------------------------------
    # Outbox Queue Management
    # -------------------------------------------------------------------------

    def get_next_outbox_batch(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Fetch next outbox batch prioritizing permitted urgent records ahead of routine work."""
        with self._lock:
            conn = self._get_connection()
            try:
                now_ts = time.time()
                # Order by urgent priority first, then oldest creation time
                rows = conn.execute(
                    """
                    SELECT o.*, op.entity_id, op.context_id, op.device_id, op.version,
                           op.parent_version, op.status as op_status, op.tombstone,
                           op.observation_json, op.dense_vector_json, op.sparse_indices_json,
                           op.sparse_values_json, op.observed_at, op.recorded_at
                    FROM outbox o
                    JOIN operations_log op ON o.operation_id = op.operation_id
                    WHERE o.status IN ('queued', 'failed')
                      AND o.next_retry_at <= ?
                      AND o.sharing != 'local_only'
                    ORDER BY (CASE WHEN o.priority = 'urgent' THEN 0 ELSE 1 END) ASC,
                             o.created_at ASC
                    LIMIT ?;
                    """,
                    (now_ts, limit),
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

    def mark_outbox_sent(self, operation_ids: List[str]) -> None:
        with self._lock:
            if not operation_ids:
                return
            conn = self._get_connection()
            try:
                now_iso = datetime.now(timezone.utc).isoformat()
                placeholders = ",".join("?" * len(operation_ids))
                conn.execute(
                    f"UPDATE outbox SET status = 'sent', sent_at = ? WHERE operation_id IN ({placeholders});",
                    [now_iso] + operation_ids,
                )
            finally:
                conn.close()

    def mark_outbox_acknowledged(self, operation_ids: List[str]) -> None:
        with self._lock:
            if not operation_ids:
                return
            conn = self._get_connection()
            try:
                now_iso = datetime.now(timezone.utc).isoformat()
                placeholders = ",".join("?" * len(operation_ids))
                conn.execute(
                    f"UPDATE outbox SET status = 'acknowledged', acked_at = ? WHERE operation_id IN ({placeholders});",
                    [now_iso] + operation_ids,
                )
            finally:
                conn.close()

    def mark_outbox_failed(self, operation_id: str, error: str, next_retry_delay: float) -> None:
        with self._lock:
            conn = self._get_connection()
            try:
                next_retry_at = time.time() + next_retry_delay
                conn.execute(
                    """
                    UPDATE outbox
                    SET status = 'failed',
                        attempts = attempts + 1,
                        last_error = ?,
                        next_retry_at = ?
                    WHERE operation_id = ?;
                    """,
                    (error, next_retry_at, operation_id),
                )
            finally:
                conn.close()

    def sync_status(self) -> Dict[str, Any]:
        with self._lock:
            conn = self._get_connection()
            try:
                queued = conn.execute("SELECT COUNT(*) as c FROM outbox WHERE status = 'queued';").fetchone()["c"]
                sent = conn.execute("SELECT COUNT(*) as c FROM outbox WHERE status = 'sent';").fetchone()["c"]
                acked = conn.execute("SELECT COUNT(*) as c FROM outbox WHERE status = 'acknowledged';").fetchone()["c"]
                urgent_queued = conn.execute("SELECT COUNT(*) as c FROM outbox WHERE status = 'queued' AND priority = 'urgent';").fetchone()["c"]
                conflicts_count = conn.execute("SELECT COUNT(*) as c FROM conflict_records WHERE status = 'contested';").fetchone()["c"]
                proj_backlog = conn.execute("SELECT COUNT(*) as c FROM projection_pending WHERE status = 'pending';").fetchone()["c"]
                local_only = conn.execute("SELECT COUNT(*) as c FROM current_records WHERE sharing = 'LOCAL_ONLY';").fetchone()["c"]
                return {
                    "outbox_queued": queued,
                    "outbox_sent": sent,
                    "outbox_acknowledged": acked,
                    "urgent_queued": urgent_queued,
                    "conflicts_count": conflicts_count,
                    "projection_backlog": proj_backlog,
                    "local_only": local_only,
                    "queued": queued,
                    "queued_uploads": queued,
                    "sent": sent,
                    "acknowledged": acked,
                    "acked": acked,
                    "urgent_queue": urgent_queued,
                    "urgent": urgent_queued,
                    "queue": {
                        "queued": queued,
                        "sent": sent,
                        "acknowledged": acked,
                        "projection_backlog": proj_backlog,
                        "urgent": urgent_queued,
                        "local_only": local_only,
                    },
                    "approx_durable_bytes": self.get_approximate_usage_bytes(),
                    "max_durable_bytes": self.max_durable_bytes,
                }
            finally:
                conn.close()

    # -------------------------------------------------------------------------
    # Snapshot Installation & Reconciliation
    # -------------------------------------------------------------------------

    def record_installed_snapshot(
        self,
        snapshot_id: str,
        shard_id: Optional[str] = None,
        manifest_version: str = "v1",
        installed_records: Optional[List[Dict[str, Any]]] = None,
        target_shard_id: Optional[str] = None,
    ) -> None:
        """Record installed snapshot inventory and reconcile local revisions safely."""
        with self._lock:
            conn = self._get_connection()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                now_iso = datetime.now(timezone.utc).isoformat()
                effective_shard_id = target_shard_id or shard_id or "unknown"
                records = installed_records or []

                conn.execute(
                    """
                    INSERT OR REPLACE INTO installed_snapshots (
                        snapshot_id, target_shard_id, manifest_version, installed_at, record_count
                    ) VALUES (?, ?, ?, ?, ?);
                    """,
                    (snapshot_id, effective_shard_id, manifest_version, now_iso, len(records)),
                )

                for item in records:
                    rec_id = item["record_id"]
                    ver = item["version"]

                    conn.execute(
                        """
                        INSERT OR REPLACE INTO installed_revisions (
                            record_id, version, snapshot_id, installed_at
                        ) VALUES (?, ?, ?, ?);
                        """,
                        (rec_id, ver, snapshot_id, now_iso),
                    )

                    # Reconcile: Only clear projection pending if the exact installed version is >= pending version
                    conn.execute(
                        "DELETE FROM projection_pending WHERE record_id = ? AND version <= ?;",
                        (rec_id, ver),
                    )

                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

    def get_installed_revisions(self) -> Set[Tuple[str, int]]:
        with self._lock:
            conn = self._get_connection()
            try:
                rows = conn.execute("SELECT record_id, version FROM installed_revisions;").fetchall()
                return {(r["record_id"], r["version"]) for r in rows}
            finally:
                conn.close()

    def is_snapshot_installed(self, snapshot_id: str) -> bool:
        with self._lock:
            conn = self._get_connection()
            try:
                return conn.execute("SELECT 1 FROM installed_snapshots WHERE snapshot_id = ?",
                                    (snapshot_id,)).fetchone() is not None
            finally:
                conn.close()

    def get_operation_ids(self, record_id: str) -> List[str]:
        """Return immutable revision identities for projection deletion."""
        with self._lock:
            conn = self._get_connection()
            try:
                rows = conn.execute(
                    "SELECT operation_id FROM operations_log WHERE record_id = ?;", (record_id,)
                ).fetchall()
                return [row["operation_id"] for row in rows]
            finally:
                conn.close()

    def get_operation(self, operation_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._get_connection()
            try:
                row = conn.execute("SELECT * FROM operations_log WHERE operation_id = ?",
                                   (operation_id,)).fetchone()
                return dict(row) if row else None
            finally:
                conn.close()

    def get_active_conflict_operation_ids(self, record_id: str) -> Set[str]:
        with self._lock:
            conn = self._get_connection()
            try:
                rows = conn.execute(
                    "SELECT operation_id, competing_operation_id FROM conflict_records "
                    "WHERE record_id = ? AND status = 'contested'", (record_id,)
                ).fetchall()
                return {op for row in rows for op in row if op}
            finally:
                conn.close()

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _compute_content_hash(self, record: RecordEnvelope) -> str:
        data = {
            "entity_id": record.entity_id,
            "context_id": record.context_id,
            "observation": record.observation,
            "version": record.version,
            "tombstone": record.tombstone,
            "sharing": record.sharing.value,
            "priority": record.priority.value,
        }
        raw = json.dumps(data, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _row_to_envelope(self, row: sqlite3.Row) -> RecordEnvelope:
        return RecordEnvelope(
            record_id=row["record_id"],
            operation_id="",  # Current record represents aggregate current state
            entity_id=row["entity_id"],
            context_id=row["context_id"],
            device_id=row["device_id"],
            observation=json.loads(row["observation_json"]),
            source_type=row["source_type"],
            source_reference=row["source_reference"],
            observed_at=datetime.fromisoformat(row["observed_at"]),
            recorded_at=datetime.fromisoformat(row["recorded_at"]),
            version=row["current_version"],
            status=RecordStatus(row["status"]),
            tombstone=bool(row["tombstone"]),
            sharing=SharingPolicy(row["sharing"]),
            priority=PriorityLevel(row["priority"]),
            confidence=row["confidence"],
            confidence_source=row["confidence_source"],
        )
