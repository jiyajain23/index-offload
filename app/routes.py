"""FastAPI route definitions for LIFELINE Edge Backend."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from shared.contracts import (
    PriorityLevel,
    RecordEnvelope,
    SearchRequest,
    SearchResponse,
    SharingPolicy,
    WriteReceipt,
)
from shared.events import EventBus
from memory.service import MemoryService
from engine.shard_manager import ShardManager
from engine.network_control import NetworkFaultController
from sync.upload_worker import UploadWorker
from sync.snapshot_manager import SnapshotManager
from .telemetry import DetectorTelemetryHub

router = APIRouter()

# Globals set during app setup
MEMORY_SERVICE: Optional[MemoryService] = None
ENGINE: Optional[ShardManager] = None
UPLOAD_WORKER: Optional[UploadWorker] = None
SNAPSHOT_MANAGER: Optional[SnapshotManager] = None
NETWORK_CONTROLLER: Optional[NetworkFaultController] = None
TELEMETRY_HUB: Optional[DetectorTelemetryHub] = None
EVENT_BUS: Optional[EventBus] = None


# -----------------------------------------------------------------------------
# Record CRUD & Revisions
# -----------------------------------------------------------------------------

@router.post("/records", response_model=WriteReceipt, status_code=status.HTTP_201_CREATED)
def write_record(record: RecordEnvelope):
    """Durably accept an observation or revision, enforce privacy/capacity, and project into search."""
    if not MEMORY_SERVICE:
        raise HTTPException(status_code=503, detail="Memory service unavailable")
    try:
        return MEMORY_SERVICE.write(record)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/records/{record_id}", response_model=RecordEnvelope)
def get_record(record_id: str):
    """Fetch current authoritative record revision."""
    if not MEMORY_SERVICE:
        raise HTTPException(status_code=503, detail="Memory service unavailable")
    rec = MEMORY_SERVICE.get_record(record_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Record not found")
    return rec


class DeleteRecordRequest(BaseModel):
    operation_id: str = Field(..., description="Unique operation ID for deletion idempotency")
    device_id: str = Field(..., description="Device submitting deletion")
    reason: Optional[str] = None


@router.delete("/records/{record_id}", response_model=WriteReceipt)
def delete_record(record_id: str, req: DeleteRecordRequest):
    """Create and durably persist a tombstone record."""
    if not MEMORY_SERVICE:
        raise HTTPException(status_code=503, detail="Memory service unavailable")
    return MEMORY_SERVICE.delete(
        record_id=record_id,
        operation_id=req.operation_id,
        device_id=req.device_id,
        reason=req.reason,
    )


# -----------------------------------------------------------------------------
# Public Search
# -----------------------------------------------------------------------------

@router.post("/search", response_model=SearchResponse)
def search_memory(req: SearchRequest):
    """Execute engine retrieval and reconcile candidates against authoritative memory."""
    if not MEMORY_SERVICE:
        raise HTTPException(status_code=503, detail="Memory service unavailable")
    return MEMORY_SERVICE.search(req)


# -----------------------------------------------------------------------------
# Conflict Management
# -----------------------------------------------------------------------------

@router.get("/conflicts")
def get_conflicts():
    """List currently contested divergent alternative revisions."""
    if not MEMORY_SERVICE:
        raise HTTPException(status_code=503, detail="Memory service unavailable")
    return {"conflicts": MEMORY_SERVICE.get_conflicts()}


class ResolveConflictRequest(BaseModel):
    record_id: str
    entity_id: str
    context_id: str
    winning_observation: Dict[str, Any]
    resolved_operations: List[str]
    device_id: str
    priority: PriorityLevel = PriorityLevel.ROUTINE
    sharing: SharingPolicy = SharingPolicy.PERMITTED_SHARED


@router.post("/conflicts/resolve", response_model=WriteReceipt)
def resolve_conflict(req: ResolveConflictRequest):
    """Explicitly resolve contested alternatives with a superseding resolution record."""
    if not MEMORY_SERVICE:
        raise HTTPException(status_code=503, detail="Memory service unavailable")
    return MEMORY_SERVICE.resolve_conflict(
        record_id=req.record_id,
        entity_id=req.entity_id,
        context_id=req.context_id,
        winning_observation=req.winning_observation,
        resolved_operations=req.resolved_operations,
        device_id=req.device_id,
        priority=req.priority,
        sharing=req.sharing,
    )


# -----------------------------------------------------------------------------
# Sync & Outbox Control
# -----------------------------------------------------------------------------

@router.get("/sync/status")
def sync_status():
    """Return outbox queue counts, pending bytes, urgent backlog, and sync state."""
    if not MEMORY_SERVICE:
        raise HTTPException(status_code=503, detail="Memory service unavailable")
    return MEMORY_SERVICE.sync_status()


@router.post("/sync/trigger")
def trigger_sync():
    """Manually process an outbox batch immediately."""
    if not UPLOAD_WORKER:
        raise HTTPException(status_code=503, detail="Upload worker unavailable")
    acked = UPLOAD_WORKER.process_outbox_once()
    return {"status": "success", "acknowledged_count": acked}


# -----------------------------------------------------------------------------
# Snapshots Handoff
# -----------------------------------------------------------------------------

class FetchSnapshotRequest(BaseModel):
    context_id: str


@router.post("/snapshots/fetch")
def fetch_and_activate_snapshot(req: FetchSnapshotRequest):
    """Download latest prepared server snapshot, stage it, hand off to engine, and reconcile."""
    if not SNAPSHOT_MANAGER:
        raise HTTPException(status_code=503, detail="Snapshot manager unavailable")
    try:
        receipt = SNAPSHOT_MANAGER.fetch_and_activate(req.context_id)
        if not receipt:
            raise HTTPException(status_code=404, detail=f"No server snapshot for {req.context_id}")
        return receipt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------------------------
# Engine & Shard Inspector
# -----------------------------------------------------------------------------

@router.get("/engine/status")
def engine_status():
    """Return engine resource usage, soft budget, loaded shards, and queue depths."""
    if not ENGINE:
        raise HTTPException(status_code=503, detail="Engine unavailable")
    return ENGINE.status()


class SetBudgetRequest(BaseModel):
    soft_budget_mb: float = Field(..., gt=0)


@router.post("/engine/budget")
def set_budget(req: SetBudgetRequest):
    """Update engine soft budget with immediate reconciliation."""
    if not ENGINE:
        raise HTTPException(status_code=503, detail="Engine unavailable")
    ENGINE.set_soft_budget_bytes(int(req.soft_budget_mb * 1024 * 1024))
    return ENGINE.status()


# -----------------------------------------------------------------------------
# Offline Fault Control
# -----------------------------------------------------------------------------

class SetOfflineRequest(BaseModel):
    offline: bool


@router.post("/network/offline")
def set_offline_network_state(req: SetOfflineRequest):
    """Control real server-link transport blocking for offline drills."""
    if not NETWORK_CONTROLLER:
        raise HTTPException(status_code=503, detail="Network controller unavailable")
    NETWORK_CONTROLLER.set_offline(req.offline)
    return {
        "is_offline": NETWORK_CONTROLLER.is_offline,
        "dropped_requests_count": NETWORK_CONTROLLER.get_dropped_count(),
    }


@router.get("/network/status")
def get_network_status():
    if not NETWORK_CONTROLLER:
        raise HTTPException(status_code=503, detail="Network controller unavailable")
    return {
        "is_offline": NETWORK_CONTROLLER.is_offline,
        "dropped_requests_count": NETWORK_CONTROLLER.get_dropped_count(),
    }


# -----------------------------------------------------------------------------
# Person C Detector Telemetry Boundary
# -----------------------------------------------------------------------------

class FrameTelemetryRequest(BaseModel):
    frame_latency_ms: float
    dropped_frames: int = 0
    queue_depth: int = 0
    model_name: Optional[str] = None
    is_synthetic: bool = False


@router.post("/telemetry/detector")
def record_detector_telemetry(req: FrameTelemetryRequest):
    """Record live inference telemetry from Person C's detector."""
    if not TELEMETRY_HUB:
        raise HTTPException(status_code=503, detail="Telemetry hub unavailable")
    TELEMETRY_HUB.record_frame(
        frame_latency_ms=req.frame_latency_ms,
        dropped_frames=req.dropped_frames,
        queue_depth=req.queue_depth,
        model_name=req.model_name,
        is_synthetic=req.is_synthetic,
    )
    return {"status": "recorded"}


@router.get("/telemetry/detector")
def get_detector_telemetry():
    """Retrieve aggregated detector health, FPS, and p95 latency."""
    if not TELEMETRY_HUB:
        raise HTTPException(status_code=503, detail="Telemetry hub unavailable")
    return TELEMETRY_HUB.get_metrics()


# -----------------------------------------------------------------------------
# Event Log (Dashboard Reconnection)
# -----------------------------------------------------------------------------

@router.get("/events")
def get_recent_events(limit: int = Query(50, ge=1, le=500)):
    """Fetch recent typed events for a reconnecting dashboard."""
    if not EVENT_BUS:
        return {"events": []}
    events = EVENT_BUS.get_history(limit=limit)
    return {"events": [e.model_dump() for e in events]}
