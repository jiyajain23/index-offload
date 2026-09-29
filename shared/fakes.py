"""Deterministic FakeEngine and FakeMemory implementations matching shared contracts.

Used to test isolated layers without external dependencies.
"""

from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any, Dict, List, Optional
import uuid

from .contracts import (
    ActivationReceipt,
    CandidateHit,
    CoverageSummary,
    EngineSearchResult,
    IEngine,
    IMemory,
    MemorySearchHit,
    PriorityLevel,
    ProjectionDeleteRequest,
    ProjectionReceipt,
    ProjectionUpsertRequest,
    RecordEnvelope,
    RecordStatus,
    SearchRequest,
    SearchResponse,
    SharingPolicy,
    SnapshotCandidate,
    WriteReceipt,
)


class FakeEngine(IEngine):
    """Deterministic fake engine implementing IEngine for testing."""

    def __init__(self):
        self.projections: Dict[str, Dict[str, Any]] = {}  # (record_id) -> data
        self.active_shards: Dict[str, Dict[str, Any]] = {
            "emergency_protocols": {"pinned": True, "version": "v1.0.0"},
            "local_write": {"pinned": True, "version": "local_v1", "is_write": True},
        }
        self.activated_snapshots: List[str] = []
        self.should_fail_activation: bool = False
        self.should_fail_projection: bool = False
        self.simulate_overload: bool = False
        self.latency_ms: float = 0.0

    def query_shards(self, request: SearchRequest) -> EngineSearchResult:
        if self.latency_ms > 0:
            time.sleep(self.latency_ms / 1000.0)

        searched = []
        queued = []
        unavailable = []
        skipped = []
        failed = []

        if request.include_protocols and "emergency_protocols" in self.active_shards:
            searched.append("emergency_protocols")

        if "local_write" in self.active_shards:
            searched.append("local_write")

        if self.simulate_overload:
            queued.append(request.context)
        elif request.context in self.active_shards:
            searched.append(request.context)
        else:
            unavailable.append(request.context)

        candidates: List[CandidateHit] = []
        # Return projections in searched context or local_write
        for rec_id, proj in self.projections.items():
            if proj.get("context_id") in searched or "local_write" in searched:
                candidates.append(CandidateHit(
                    point_id=rec_id,
                    shard_id="local_write",
                    shard_version="local_v1",
                    score=0.95,
                    payload={"record_id": rec_id, **proj.get("payload", {})},
                    dense_vector=proj.get("dense_vector"),
                ))

        # Add a deterministic candidate for protocols if searched
        if "emergency_protocols" in searched:
            candidates.append(CandidateHit(
                point_id="proto-hazard-101",
                shard_id="emergency_protocols",
                shard_version="v1.0.0",
                score=0.88,
                payload={"record_id": "proto-hazard-101", "claim": "Emergency shutdown protocol card"},
                dense_vector=[0.1] * 4,
            ))

        # Sort candidates descending by score
        candidates.sort(key=lambda c: c.score, reverse=True)

        return EngineSearchResult(
            candidates=candidates[:request.k],
            coverage=CoverageSummary(
                searched=searched,
                queued=queued,
                unavailable_offline=unavailable,
                skipped_for_budget=skipped,
                failed=failed,
            ),
            elapsed_ms=self.latency_ms,
            reasons={"overload": "Soft budget exceeded"} if queued else {},
        )

    def upsert_projection(self, request: ProjectionUpsertRequest) -> ProjectionReceipt:
        if self.should_fail_projection:
            return ProjectionReceipt(
                record_id=request.record_id,
                context_id=request.context_id,
                success=False,
                error="Simulated projection failure",
            )
        self.projections[request.record_id] = {
            "version": request.version,
            "context_id": request.context_id,
            "dense_vector": request.dense_vector,
            "payload": request.payload,
        }
        return ProjectionReceipt(
            record_id=request.record_id,
            context_id=request.context_id,
            success=True,
            version_installed=request.version,
        )

    def delete_projection(self, request: ProjectionDeleteRequest) -> ProjectionReceipt:
        self.projections.pop(request.record_id, None)
        return ProjectionReceipt(
            record_id=request.record_id,
            context_id=request.context_id,
            success=True,
            version_installed=request.version,
        )

    def activate_snapshot(self, candidate: SnapshotCandidate) -> ActivationReceipt:
        if self.should_fail_activation:
            return ActivationReceipt(
                snapshot_id=candidate.snapshot_id,
                shard_id=candidate.target_shard_id,
                success=False,
                error="Simulated activation failure",
                reverted_to_previous=True,
            )

        self.activated_snapshots.append(candidate.snapshot_id)
        self.active_shards[candidate.target_shard_id] = {
            "pinned": False,
            "version": candidate.manifest_version,
        }
        return ActivationReceipt(
            snapshot_id=candidate.snapshot_id,
            shard_id=candidate.target_shard_id,
            success=True,
            installed_version=candidate.manifest_version,
        )

    def status(self) -> Dict[str, Any]:
        return {
            "soft_budget_bytes": 100 * 1024 * 1024,
            "usage_bytes": 45 * 1024 * 1024,
            "loaded_shards": list(self.active_shards.keys()),
            "pinned_shards": [k for k, v in self.active_shards.items() if v.get("pinned")],
            "projections_count": len(self.projections),
        }

    def close_all(self) -> None:
        self.active_shards.clear()
        self.projections.clear()


class FakeMemory(IMemory):
    """Deterministic fake memory service implementing IMemory."""

    def __init__(self, engine: Optional[IEngine] = None):
        self.engine = engine or FakeEngine()
        self.records: Dict[str, RecordEnvelope] = {}
        self.operations: Dict[str, WriteReceipt] = {}
        self.conflicts: List[Dict[str, Any]] = []

    def write(self, record: RecordEnvelope) -> WriteReceipt:
        if record.operation_id in self.operations:
            return self.operations[record.operation_id]

        self.records[record.record_id] = record
        receipt = WriteReceipt(
            record_id=record.record_id,
            operation_id=record.operation_id,
            version=record.version,
            durable=True,
            projection_status="ready",
        )
        self.operations[record.operation_id] = receipt

        # Project into engine
        if not record.tombstone:
            self.engine.upsert_projection(ProjectionUpsertRequest(
                record_id=record.record_id,
                version=record.version,
                context_id=record.context_id,
                dense_vector=record.dense_vector,
                payload=record.observation,
            ))
        else:
            self.engine.delete_projection(ProjectionDeleteRequest(
                record_id=record.record_id,
                version=record.version,
                context_id=record.context_id,
            ))

        return receipt

    def search(self, request: SearchRequest) -> SearchResponse:
        engine_res = self.engine.query_shards(request)
        hits: List[MemorySearchHit] = []

        for cand in engine_res.candidates:
            rec = self.records.get(str(cand.point_id))
            if rec and rec.tombstone:
                continue

            hits.append(MemorySearchHit(
                record_id=str(cand.point_id),
                version=rec.version if rec else 1,
                entity_id=rec.entity_id if rec else str(cand.point_id),
                context_id=rec.context_id if rec else request.context,
                source_shard=cand.shard_id,
                installed_version=cand.shard_version,
                score=cand.score,
                status=rec.status if rec else RecordStatus.ACTIVE,
                is_contested=rec.status == RecordStatus.CONTESTED if rec else False,
                conflict_alternatives=rec.conflict_alternatives if rec else [],
                observation=rec.observation if rec else cand.payload,
                confidence=rec.confidence if rec else None,
                confidence_source=rec.confidence_source if rec else None,
                observed_at=rec.observed_at if rec else datetime.now(timezone.utc),
                tombstone=False,
            ))

        return SearchResponse(
            hits=hits[:request.k],
            coverage=engine_res.coverage,
            elapsed_ms=engine_res.elapsed_ms,
            partial=len(engine_res.coverage.queued) > 0 or len(engine_res.coverage.failed) > 0,
            shortfall=max(0, request.k - len(hits)),
            reasons=engine_res.reasons,
        )

    def get_record(self, record_id: str) -> Optional[RecordEnvelope]:
        return self.records.get(record_id)

    def get_conflicts(self) -> List[Dict[str, Any]]:
        return list(self.conflicts)

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
        resolution_rec = RecordEnvelope(
            record_id=record_id,
            operation_id=str(uuid.uuid4()),
            entity_id=entity_id,
            context_id=context_id,
            device_id=device_id,
            observation=winning_observation,
            source_type="conflict_resolution",
            version=100,  # superseding
            status=RecordStatus.ACTIVE,
            sharing=sharing,
            priority=priority,
            resolution_of=resolved_operations,
        )
        return self.write(resolution_rec)

    def sync_status(self) -> Dict[str, Any]:
        return {
            "outbox_count": 0,
            "pending_bytes": 0,
            "acknowledged_count": len(self.records),
            "urgent_count": 0,
            "routine_count": len(self.records),
        }
