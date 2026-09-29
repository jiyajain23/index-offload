"""Snapshot manager for downloading, staging, validating, and reconciling server snapshots."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import httpx

from engine.network_control import NetworkFaultController
from engine.shard_manager import ShardManager
from memory.store import SQLiteMemoryStore
from shared.contracts import ActivationReceipt, SnapshotCandidate
from shared.events import EventBus

logger = logging.getLogger("lifeline.sync.snapshot")


class SnapshotManager:
    """Coordinates snapshot download into staging, handoff to Engine, and local reconciliation."""

    def __init__(
        self,
        store: SQLiteMemoryStore,
        engine: ShardManager,
        server_url: str,
        staging_dir: str | Path,
        network_controller: Optional[NetworkFaultController] = None,
        events: Optional[EventBus] = None,
        custom_client: Optional[httpx.Client] = None,
    ):
        self.store = store
        self.engine = engine
        self.server_url = server_url.rstrip("/")
        self.staging_dir = Path(staging_dir)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.network_controller = network_controller
        self.events = events
        self._custom_client = custom_client

    def _get_http_client(self) -> httpx.Client:
        if self._custom_client is not None:
            return self._custom_client
        transport = (
            self.network_controller.create_transport()
            if self.network_controller
            else httpx.HTTPTransport()
        )
        return httpx.Client(transport=transport, timeout=30.0)

    def fetch_and_activate(self, context_id: str) -> Optional[ActivationReceipt]:
        """Fetch latest snapshot for context from server, stage it, hand off to engine, and reconcile."""
        client = self._get_http_client()
        should_close = (self._custom_client is None)

        try:
            # 1. Fetch Snapshot Metadata from Server
            meta_resp = client.get(f"{self.server_url}/api/v1/snapshots/{context_id}")
            if meta_resp.status_code == 404:
                logger.info("No server snapshot available for context %s", context_id)
                return None
            meta_resp.raise_for_status()
            metadata = meta_resp.json()

            candidate = SnapshotCandidate(**metadata)

            # 2. Download Snapshot Archive into Staging Directory with Bounded Streaming & Checksum
            stage_archive_path = self.staging_dir / f"{candidate.snapshot_id}.tar"
            hasher = hashlib.sha256()

            with client.stream("GET", f"{self.server_url}/api/v1/snapshots/{context_id}/download") as stream:
                stream.raise_for_status()
                with open(stage_archive_path, "wb") as f:
                    for chunk in stream.iter_bytes(chunk_size=65536):
                        f.write(chunk)
                        hasher.update(chunk)

            computed_sha = hasher.hexdigest()
            if computed_sha != candidate.checksum_sha256:
                stage_archive_path.unlink(missing_ok=True)
                raise ValueError(
                    f"Snapshot download checksum mismatch: expected {candidate.checksum_sha256}, got {computed_sha}"
                )
        finally:
            if should_close:
                client.close()

        # Update candidate with local staging path
        candidate.staging_path = str(stage_archive_path)

        # 3. Two-Stage Handoff: Hand off to Engine for activation
        receipt = self.engine.activate_snapshot(candidate)

        # 4. Reconcile Local State after Successful Activation
        if receipt.success:
            self.store.record_installed_snapshot(
                snapshot_id=candidate.snapshot_id,
                target_shard_id=candidate.target_shard_id,
                manifest_version=candidate.manifest_version,
                installed_records=candidate.included_records,
            )

            # Emit typed event
            if self.events:
                self.events.emit(
                    "shard_activated",
                    component="sync",
                    payload={
                        "snapshot_id": candidate.snapshot_id,
                        "shard_id": candidate.target_shard_id,
                        "installed_version": candidate.manifest_version,
                    },
                )

            # Cleanup staging archive
            stage_archive_path.unlink(missing_ok=True)

        return receipt
