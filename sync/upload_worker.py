"""Upload worker implementing prioritized sync, bounded batches, backoff, and privacy enforcement."""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any, Dict, List, Optional

import httpx

from engine.network_control import NetworkFaultController
from memory.store import SQLiteMemoryStore
from shared.contracts import PriorityLevel, SharingPolicy
from shared.events import EventBus

logger = logging.getLogger("lifeline.sync.upload")


class UploadWorker:
    """Outbox worker that prioritizes permitted urgent records and handles connection failures."""

    def __init__(
        self,
        store: SQLiteMemoryStore,
        server_url: str,
        network_controller: Optional[NetworkFaultController] = None,
        events: Optional[EventBus] = None,
        batch_size: int = 10,
        timeout_seconds: float = 5.0,
        base_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 30.0,
        poll_interval: float = 1.0,
        custom_client: Optional[httpx.Client] = None,
    ):
        self.store = store
        self.server_url = server_url.rstrip("/")
        self.network_controller = network_controller
        self.events = events
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.poll_interval = poll_interval
        self._custom_client = custom_client

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def _get_http_client(self) -> httpx.Client:
        if self._custom_client is not None:
            return self._custom_client
        transport = (
            self.network_controller.create_transport()
            if self.network_controller
            else httpx.HTTPTransport()
        )
        return httpx.Client(transport=transport, timeout=self.timeout_seconds)

    def process_outbox_once(self) -> int:
        """Process one batch from outbox. Returns count of successfully acknowledged records."""
        batch = self.store.get_next_outbox_batch(limit=self.batch_size)
        if not batch:
            return 0

        # Strict privacy check right before serialization:
        # LOCAL_ONLY records must NEVER be sent over the wire!
        safe_items = []
        for item in batch:
            if item.get("sharing") == SharingPolicy.LOCAL_ONLY.value:
                logger.error("CRITICAL: Privacy leak prevented. Dropping local_only record %s from transfer", item.get("record_id"))
                continue

            import json
            obs = json.loads(item["observation_json"]) if item.get("observation_json") else {}
            dense = json.loads(item["dense_vector_json"]) if item.get("dense_vector_json") else None
            sparse_idx = json.loads(item["sparse_indices_json"]) if item.get("sparse_indices_json") else None
            sparse_val = json.loads(item["sparse_values_json"]) if item.get("sparse_values_json") else None

            safe_items.append({
                "operation_id": item["operation_id"],
                "record_id": item["record_id"],
                "entity_id": item["entity_id"],
                "context_id": item["context_id"],
                "device_id": item["device_id"],
                "version": item["version"],
                "parent_version": item.get("parent_version"),
                "status": item["op_status"],
                "tombstone": bool(item["tombstone"]),
                "observation": obs,
                "dense_vector": dense,
                "sparse_indices": sparse_idx,
                "sparse_values": sparse_val,
                "observed_at": item["observed_at"],
                "recorded_at": item["recorded_at"],
            })

        if not safe_items:
            return 0

        op_ids = [item["operation_id"] for item in safe_items]
        self.store.mark_outbox_sent(op_ids)

        try:
            if self._custom_client is not None:
                resp = self._custom_client.post(
                    f"{self.server_url}/api/v1/ingest",
                    json={"items": safe_items},
                )
            else:
                transport = (
                    self.network_controller.create_transport()
                    if self.network_controller
                    else httpx.HTTPTransport()
                )
                with httpx.Client(transport=transport, timeout=self.timeout_seconds) as client:
                    resp = client.post(
                        f"{self.server_url}/api/v1/ingest",
                        json={"items": safe_items},
                    )

            if resp.status_code == 200:
                data = resp.json()
                receipts = data.get("receipts", [])
                acked_ids = [r["operation_id"] for r in receipts if r.get("status") == "durably_accepted"]
                self.store.mark_outbox_acknowledged(acked_ids)

                # Check if any urgent record was acknowledged
                for item in batch:
                    if item["operation_id"] in acked_ids and item.get("priority") == PriorityLevel.URGENT.value:
                        self._emit_event(
                            "sos_acknowledged",
                            operation_id=item["operation_id"],
                            record_id=item["record_id"],
                        )

                self._emit_event(
                    "sync_progress",
                    acknowledged_count=len(acked_ids),
                    batch_size=len(safe_items),
                )
                return len(acked_ids)
            else:
                error_msg = f"Server returned status {resp.status_code}: {resp.text}"
                self._handle_failure(op_ids, error_msg)
                return 0

        except Exception as exc:
            error_msg = f"Outbox transfer network error: {exc}"
            logger.info("Outbox sync failed (offline or network error): %s", error_msg)
            self._handle_failure(op_ids, error_msg)
            return 0

    def _handle_failure(self, operation_ids: List[str], error_msg: str) -> None:
        for op_id in operation_ids:
            # Jittered exponential backoff
            jitter = random.uniform(0.8, 1.2)
            delay = min(self.max_backoff_seconds, self.base_backoff_seconds * jitter)
            self.store.mark_outbox_failed(op_id, error=error_msg, next_retry_delay=delay)

    def start(self) -> None:
        """Start upload worker thread."""
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="UploadWorker")
        self._thread.start()

    def stop(self) -> None:
        """Stop upload worker thread."""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=3.0)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.process_outbox_once()
            except Exception as e:
                logger.error("Unhandled exception in upload loop: %s", e)
            self._stop_event.wait(timeout=self.poll_interval)

    def _emit_event(self, event_type: str, **payload: Any) -> None:
        if self.events:
            self.events.emit(event_type, component="sync", payload=payload)
