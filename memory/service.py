"""Authoritative Memory Service implementing IMemory and public search reconciliation."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import time
from typing import Any, Dict, List, Optional
import uuid

from shared.contracts import (
    CandidateHit,
    CoverageSummary,
    EngineSearchResult,
    IEngine,
    IMemory,
    MemorySearchHit,
    PriorityLevel,
    ProjectionDeleteRequest,
    ProjectionUpsertRequest,
    RecordEnvelope,
    RecordStatus,
    SearchRequest,
    SearchResponse,
    SharingPolicy,
    WriteReceipt,
)
from shared.events import EventBus
from .store import (
    IdempotencyConflictError,
    SQLiteMemoryStore,
    StorageCapacityError,
)

logger = logging.getLogger("lifeline.memory.service")


class MemoryService(IMemory):
    """Authoritative memory service enforcing durable commits before projection and truthful search."""

    def __init__(
        self,
        store: SQLiteMemoryStore,
        engine: IEngine,
        events: Optional[EventBus] = None,
    ):
        self.store = store
        self.engine = engine
        self.events = events

    # -------------------------------------------------------------------------
    # Durable Writes & Idempotency
    # -------------------------------------------------------------------------

    def write(self, record: RecordEnvelope) -> WriteReceipt:
        """Atomically commit record to durable store, then project into search."""
        # 1. Durable Commit First
        receipt, is_contested = self.store.commit_record(record)

        if is_contested:
            self._emit_event(
                "conflict_flagged",
                record_id=record.record_id,
                operation_id=record.operation_id,
                entity_id=record.entity_id,
            )

        # 2. Project into Search (non-blocking failure path)
        try:
            if not record.tombstone:
                proj_res = self.engine.upsert_projection(
                    ProjectionUpsertRequest(
                        record_id=record.record_id,
                        operation_id=record.operation_id,
                        version=receipt.version,
                        context_id=record.context_id,
                        dense_vector=record.dense_vector,
                        sparse_indices=record.sparse_indices,
                        sparse_values=record.sparse_values,
                        payload={
                            "entity_id": record.entity_id,
                            "context_id": record.context_id,
                            "observation": record.observation,
                            "observed_at": record.observed_at.isoformat(),
                            "status": RecordStatus.CONTESTED.value if is_contested else record.status.value,
                        },
                    )
                )
            else:
                proj_res = self.engine.delete_projection(
                    ProjectionDeleteRequest(
                        record_id=record.record_id,
                        version=receipt.version,
                        context_id=record.context_id,
                        operation_ids=self.store.get_operation_ids(record.record_id),
                    )
                )

            if proj_res.success:
                self.store.mark_projection_completed(record.record_id, receipt.version,
                                                     record.operation_id)
                receipt.projection_status = "ready"
            else:
                receipt.projection_status = "pending"
                self._emit_event(
                    "projection_pending",
                    record_id=record.record_id,
                    version=receipt.version,
                    error=proj_res.error,
                )

        except Exception as e:
            logger.warning("Projection failed after durable commit: %s", e)
            receipt.projection_status = "pending"
            self._emit_event(
                "projection_pending",
                record_id=record.record_id,
                version=receipt.version,
                error=str(e),
            )

        return receipt

    def delete(
        self,
        record_id: str,
        operation_id: str,
        device_id: str,
        reason: Optional[str] = None,
    ) -> WriteReceipt:
        """Create and commit a tombstone record."""
        existing = self.store.get_record(record_id)
        parent_ver = existing.version if existing else None
        entity_id = existing.entity_id if existing else f"entity_{record_id}"
        context_id = existing.context_id if existing else "general"

        tombstone_rec = RecordEnvelope(
            record_id=record_id,
            operation_id=operation_id,
            entity_id=entity_id,
            context_id=context_id,
            device_id=device_id,
            version=(parent_ver + 1) if parent_ver else 1,
            parent_version=parent_ver,
            status=RecordStatus.TOMBSTONE,
            tombstone=True,
            observation={"deletion_reason": reason or "deleted_by_user"},
            source_type="tombstone",
        )
        return self.write(tombstone_rec)

    def revise(self, new_record: RecordEnvelope) -> WriteReceipt:
        """Submit a revision that supersedes its parent."""
        return self.write(new_record)

    # -------------------------------------------------------------------------
    # Public Search Reconciliation
    # -------------------------------------------------------------------------

    def search(self, request: SearchRequest) -> SearchResponse:
        """Execute engine retrieval and reconcile candidates against authoritative memory."""
        start_time = time.perf_counter()
        deadline_at = (start_time + (request.deadline_ms / 1000.0)) if request.deadline_ms else None

        # Step 1: Query Engine
        engine_res = self.engine.query_shards(request)

        # Step 2: Reconcile candidates against SQLite authoritative state
        reconciled_hits, dropped_count = self._reconcile_candidates(
            engine_res.candidates, request.context, request.filters)

        # Step 3: Over-fetch / refill if filtering created a shortfall and deadline allows
        if len(reconciled_hits) < request.k and dropped_count > 0:
            if not deadline_at or time.perf_counter() < (deadline_at - 0.05):
                refill_k = request.k + dropped_count + 5
                refill_req = request.model_copy(update={"k": refill_k})
                refill_res = self.engine.query_shards(refill_req)
                refill_hits, _ = self._reconcile_candidates(
                    refill_res.candidates, request.context, request.filters)
                reconciled_hits = refill_hits

        final_hits = reconciled_hits[:request.k]
        shortfall = max(0, request.k - len(final_hits))
        elapsed = (time.perf_counter() - start_time) * 1000.0

        is_partial = (
            shortfall > 0
            or len(engine_res.coverage.queued) > 0
            or len(engine_res.coverage.skipped_for_budget) > 0
            or len(engine_res.coverage.failed) > 0
        )

        return SearchResponse(
            hits=final_hits,
            coverage=engine_res.coverage,
            elapsed_ms=elapsed,
            partial=is_partial,
            shortfall=shortfall,
            reasons=engine_res.reasons,
        )

    def _reconcile_candidates(
        self, candidates: List[CandidateHit], target_context: str, filters=None
    ) -> Tuple[List[MemorySearchHit], int]:
        valid_hits: List[MemorySearchHit] = []
        dropped_count = 0
        seen_records: set[str] = set()

        for cand in candidates:
            # Point ID in Qdrant may be payload record_id or point_id
            rec_id = str(cand.payload.get("record_id", cand.point_id))
            op_id = cand.payload.get("operation_id")

            # Check authoritative state in SQLite
            rec = self.store.get_record(rec_id)
            tombstone = self.store.get_tombstone(rec_id)

            # Rule A: Exclude tombstoned records
            if tombstone or (rec and rec.tombstone):
                dropped_count += 1
                continue

            # Rule B: Check if superseded or current
            if rec:
                candidate_version = int(cand.payload.get("version", 0))
                if candidate_version != rec.version:
                    dropped_count += 1
                    continue
                conflicts = self.store.get_active_conflict_operation_ids(rec_id)
                if rec.status == RecordStatus.CONTESTED and op_id in conflicts:
                    operation = self.store.get_operation(op_id)
                    if operation:
                        key = op_id
                        if key in seen_records:
                            continue
                        valid_hits.append(MemorySearchHit(
                            record_id=rec_id, version=operation["version"],
                            entity_id=operation["entity_id"], context_id=operation["context_id"],
                            source_shard=cand.shard_id, installed_version=cand.shard_version,
                            score=cand.score, status=RecordStatus.CONTESTED, is_contested=True,
                            conflict_alternatives=sorted(conflicts - {op_id}),
                            observation=json.loads(operation["observation_json"]),
                            observed_at=datetime.fromisoformat(operation["observed_at"]),
                        ))
                        seen_records.add(key)
                        continue
                if rec_id in seen_records:
                    continue
                # Local authoritative version
                is_contested = (rec.status == RecordStatus.CONTESTED)

                valid_hits.append(
                    MemorySearchHit(
                        record_id=rec.record_id,
                        version=rec.version,
                        entity_id=rec.entity_id,
                        context_id=rec.context_id,
                        source_shard=cand.shard_id,
                        installed_version=cand.shard_version,
                        score=cand.score,
                        status=rec.status,
                        is_contested=is_contested,
                        conflict_alternatives=sorted(conflicts) if is_contested else [],
                        observation=rec.observation,
                        confidence=rec.confidence,
                        confidence_source=rec.confidence_source,
                        observed_at=rec.observed_at,
                        tombstone=False,
                    )
                )
                seen_records.add(rec_id)
            else:
                # Base shard record not yet locally modified
                if rec_id in seen_records:
                    continue
                valid_hits.append(
                    MemorySearchHit(
                        record_id=rec_id,
                        version=int(cand.payload.get("version", 1)),
                        entity_id=str(cand.payload.get("entity_id", rec_id)),
                        context_id=str(cand.payload.get("context_id", target_context)),
                        source_shard=cand.shard_id,
                        installed_version=cand.shard_version,
                        score=cand.score,
                        status=RecordStatus.ACTIVE,
                        is_contested=False,
                        conflict_alternatives=[],
                        observation=cand.payload.get("observation", cand.payload),
                        confidence=float(cand.payload["confidence"]) if "confidence" in cand.payload else None,
                        confidence_source=cand.payload.get("confidence_source"),
                        observed_at=datetime.now(timezone.utc),
                        tombstone=False,
                    )
                )
                seen_records.add(rec_id)

        # The Edge adapter currently does not push arbitrary payload filters into
        # Qdrant. Enforce the public contract against authoritative data here.
        if filters:
            filtered = [hit for hit in valid_hits if self._matches_filters(hit, filters)]
            dropped_count += len(valid_hits) - len(filtered)
            valid_hits = filtered

        # Sort descending by score
        valid_hits.sort(key=lambda h: h.score, reverse=True)
        return valid_hits, dropped_count

    @staticmethod
    def _matches_filters(hit: MemorySearchHit, filters) -> bool:
        def value_for(key: str):
            if key.startswith("observation."):
                return hit.observation.get(key[len("observation."):])
            if key == "status":
                return hit.status.value
            if key in {"record_id", "entity_id", "context_id", "source_shard"}:
                return getattr(hit, key)
            # Unknown keys fail closed for must and must_not.
            raise ValueError(f"Unsupported search filter: {key}")

        for key, expected in (filters.must or {}).items():
            if value_for(key) != expected:
                return False
        for key, forbidden in (filters.must_not or {}).items():
            if value_for(key) == forbidden:
                return False
        return True

    # -------------------------------------------------------------------------
    # Conflict Inspection & Resolution
    # -------------------------------------------------------------------------

    def get_record(self, record_id: str) -> Optional[RecordEnvelope]:
        return self.store.get_record(record_id)

    def get_conflicts(self) -> List[Dict[str, Any]]:
        return self.store.get_conflicts()

    def resolve_conflict(
        self,
        record_id: str,
        entity_id: str,
        context_id: str,
        winning_observation: Dict[str, Any],
        resolved_operations: List[str],
        device_id: str,
        priority: PriorityLevel = PriorityLevel.ROUTINE,
        sharing: SharingPolicy = SharingPolicy.PERMITTED_SHARED,
    ) -> WriteReceipt:
        """Create a superseding resolution record that explicitly references competing operations."""
        existing = self.store.get_record(record_id)
        next_ver = (existing.version + 1) if existing else 1

        resolution_record = RecordEnvelope(
            record_id=record_id,
            operation_id=str(uuid.uuid4()),
            entity_id=entity_id,
            context_id=context_id,
            device_id=device_id,
            version=next_ver,
            parent_version=existing.version if existing else None,
            status=RecordStatus.ACTIVE,
            observation=winning_observation,
            source_type="conflict_resolution",
            sharing=sharing,
            priority=priority,
            resolution_of=resolved_operations,
        )
        return self.write(resolution_record)

    # -------------------------------------------------------------------------
    # Background Projection Rebuilding & Sync Status
    # -------------------------------------------------------------------------

    def replay_pending_projections(self) -> int:
        """Drain and replay pending projection work after crash or startup."""
        pending = self.store.get_pending_projections(limit=50)
        replayed = 0
        for item in pending:
            rec_id = item["record_id"]
            ver = item["version"]
            is_tombstone = bool(item["is_tombstone"])
            ctx = item["context_id"]

            try:
                if not is_tombstone:
                    import json
                    dense = json.loads(item["dense_vector_json"]) if item.get("dense_vector_json") else None
                    sparse_idx = json.loads(item["sparse_indices_json"]) if item.get("sparse_indices_json") else None
                    sparse_val = json.loads(item["sparse_values_json"]) if item.get("sparse_values_json") else None
                    obs = json.loads(item["observation_json"]) if item.get("observation_json") else {}

                    res = self.engine.upsert_projection(
                        ProjectionUpsertRequest(
                            record_id=rec_id,
                            operation_id=item["operation_id"],
                            version=ver,
                            context_id=ctx,
                            dense_vector=dense,
                            sparse_indices=sparse_idx,
                            sparse_values=sparse_val,
                            payload={"observation": obs},
                        )
                    )
                else:
                    res = self.engine.delete_projection(
                        ProjectionDeleteRequest(
                            record_id=rec_id, version=ver, context_id=ctx,
                            operation_ids=self.store.get_operation_ids(rec_id),
                        )
                    )

                if res.success:
                    self.store.mark_projection_completed(rec_id, ver, item["operation_id"])
                    replayed += 1
            except Exception as e:
                logger.warning("Replay projection failed for %s: %s", rec_id, e)

        return replayed

    def sync_status(self) -> Dict[str, Any]:
        return self.store.sync_status()

    def _emit_event(self, event_type: str, **payload: Any) -> None:
        if self.events:
            self.events.emit(event_type, component="memory", payload=payload)
