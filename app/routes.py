"""FastAPI route definitions for LIFELINE Edge Backend."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union
import os
import json
import uuid
import logging
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query, Response, status
from pydantic import BaseModel, Field

from shared.contracts import (
    PriorityLevel,
    RecordEnvelope,
    SearchRequest,
    SearchResponse,
    SharingPolicy,
    ShardRole,
    WriteReceipt,
)
from shared.events import EventBus
from memory.service import MemoryService
from engine.shard_manager import ShardManager
from engine.network_control import NetworkFaultController
from sync.upload_worker import UploadWorker
from sync.snapshot_manager import SnapshotManager
from .telemetry import DetectorTelemetryHub
from perception.embedding import MODEL_VERSION, embed
from fixtures.seed_data import get_demo_procedure_cards, get_zone_history_records

logger = logging.getLogger("lifeline.routes")

router = APIRouter()

# Globals set during app setup
MEMORY_SERVICE: Optional[MemoryService] = None
ENGINE: Optional[ShardManager] = None
UPLOAD_WORKER: Optional[UploadWorker] = None
SNAPSHOT_MANAGER: Optional[SnapshotManager] = None
NETWORK_CONTROLLER: Optional[NetworkFaultController] = None
TELEMETRY_HUB: Optional[DetectorTelemetryHub] = None
EVENT_BUS: Optional[EventBus] = None
PERCEPTION_RUNNER: Optional[Any] = None


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


class TextSearchRequest(BaseModel):
    text: str = Field(min_length=1, max_length=1000)
    context: str = "zone_01"
    k: int = Field(default=5, ge=1, le=20)


@router.post("/search/text")
def search_text(req: TextSearchRequest):
    if not MEMORY_SERVICE:
        raise HTTPException(status_code=503, detail="Memory service unavailable")
    dense, indices, values = embed(req.text)
    res = MEMORY_SERVICE.search(SearchRequest(
        query_text=req.text, context=req.context, k=req.k,
        dense_vector=dense, sparse_indices=indices, sparse_values=values,
        embedding_model_version=MODEL_VERSION, deadline_ms=2500,
    ))
    res_dict = res.model_dump(mode="json")
    # Enrich coverage counts
    cov = res_dict.get("coverage", {})
    coverage_counts = {}
    for state in ["searched", "queued", "unavailable_offline", "skipped_for_budget", "failed"]:
        v = cov.get(state, 0)
        coverage_counts[state] = len(v) if isinstance(v, list) else int(v or 0)
    res_dict["coverage"] = coverage_counts

    # Enrich hits so frontend renders observation text, revision, and contested cleanly
    enriched_hits = []
    for h in res_dict.get("hits", []):
        obs = h.get("observation", {})
        obs_text = ""
        if isinstance(obs, dict):
            obs_text = obs.get("description") or obs.get("summary") or obs.get("text") or json.dumps(obs)
        else:
            obs_text = str(obs)
        ver = h.get("version")
        enriched_hits.append({
            "id": h.get("record_id") or h.get("id"),
            "score": h.get("score"),
            "source_shard": h.get("source_shard"),
            "sourceShard": h.get("source_shard"),
            "revision": f"r{ver}" if ver is not None else "r1",
            "observation": obs_text,
            "text": obs_text,
            "contested": h.get("is_contested", False),
            **h,
        })
    res_dict["hits"] = enriched_hits
    return res_dict


@router.get("/perception/status")
def perception_status():
    if PERCEPTION_RUNNER:
        return PERCEPTION_RUNNER.status()
    return {
        "running": False,
        "configured": False,
        "processed_frames": 0,
        "frames": 0,
        "drops": 0,
        "depth": 0,
        "metrics": {"frames": 0, "drops": 0, "depth": 0},
        "latest_observation": "Perception runner not started; idle.",
    }


class SwitchSourceRequest(BaseModel):
    source: str = Field(..., description="Target source: 'webcam' or 'fixture' (or camera index)")


@router.post("/perception/source")
def switch_perception_source(req: SwitchSourceRequest):
    """Switch perception feed between live webcam and synthetic fixture video."""
    if not PERCEPTION_RUNNER:
        raise HTTPException(status_code=503, detail="Perception runner unavailable")

    src = req.source.strip().lower()
    if src in ("webcam", "camera", "live", "0"):
        target_source: Union[str, int] = "webcam"
        synthetic = False
    else:
        fixture_path = Path("fixtures/inspection_beacon.avi")
        if not fixture_path.exists():
            fixture_path = Path("./edge_data/inspection_beacon.avi")
        target_source = str(fixture_path)
        synthetic = True

    try:
        ok = PERCEPTION_RUNNER.switch_source(target_source, synthetic=synthetic)
        return {
            "status": "switched" if ok else "failed",
            "source": "webcam" if not synthetic else "fixture",
            "synthetic_input": synthetic,
            "is_camera": not synthetic,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed switching perception source: {exc}")


@router.get("/perception/source")
def get_perception_source():
    if not PERCEPTION_RUNNER:
        return {"source": "none", "available": ["webcam", "fixture"], "is_camera": False}
    st = PERCEPTION_RUNNER.status()
    return {
        "source": st.get("source", "fixture"),
        "is_camera": st.get("is_camera", False),
        "synthetic_input": st.get("synthetic_input", True),
        "available": ["webcam", "fixture"],
    }



_FALLBACK_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" width="640" height="360">'
    '<rect width="640" height="360" fill="#0d151c"/>'
    '<rect x="80" y="80" width="480" height="200" rx="6" fill="#182832" stroke="#335060" stroke-width="2"/>'
    '<text x="110" y="130" fill="#a0c8d8" font-family="monospace" font-size="16">LIFELINE INSPECTION BEACON</text>'
    '<text x="110" y="160" fill="#668292" font-family="monospace" font-size="13">Synthetic Live Observation Frame</text>'
    '<circle cx="480" cy="180" r="28" fill="#e63946"/>'
    '<circle cx="480" cy="180" r="38" fill="none" stroke="#e63946" stroke-width="3" stroke-dasharray="6,4"/>'
    '<rect x="420" y="120" width="120" height="120" fill="none" stroke="#f4a261" stroke-width="2" stroke-dasharray="4,4"/>'
    '<text x="110" y="240" fill="#f4a261" font-family="monospace" font-size="13">[CANDIDATE DETECTED: OPERATOR REVIEW REQUIRED]</text>'
    '</svg>'
).encode("utf-8")


@router.get("/perception/frame")
def perception_frame():
    if PERCEPTION_RUNNER and getattr(PERCEPTION_RUNNER, "latest_jpeg", None):
        return Response(PERCEPTION_RUNNER.latest_jpeg, media_type="image/jpeg",
                        headers={"Cache-Control": "no-store"})
    return Response(_FALLBACK_SVG, media_type="image/svg+xml",
                    headers={"Cache-Control": "no-store"})


@router.post("/demo/seed")
def seed_demo():
    if not MEMORY_SERVICE or not ENGINE:
        raise HTTPException(status_code=503, detail="Memory service unavailable")
    # Ensure demo shards exist so seed data can be projected even if LIFELINE_DEMO was not set at startup
    data_path = Path(getattr(ENGINE, "_data_dir", "./edge_data"))
    data_path.mkdir(parents=True, exist_ok=True)
    if "emergency_protocols" not in ENGINE._registrations:
        p = data_path / "emergency_protocols"
        if not p.exists():
            ENGINE.shard_store.close(ENGINE.shard_store.create(str(p)))
        ENGINE.register_shard("emergency_protocols", str(p), pinned=True, estimated_cost_mb=5)
    for z in ("zone_01", "zone_02"):
        if z not in ENGINE._registrations:
            p = data_path / z
            if not p.exists():
                ENGINE.shard_store.close(ENGINE.shard_store.create(str(p)))
            ENGINE.register_shard(z, str(p), pinned=False, estimated_cost_mb=5)

    receipts = []
    for record in get_demo_procedure_cards() + get_zone_history_records():
        try:
            receipts.append(MEMORY_SERVICE.write(record).model_dump(mode="json"))
        except Exception as e:
            logger.warning("Error writing seed record %s: %s", record.record_id, e)
    if EVENT_BUS:
        EVENT_BUS.emit(
            "demo_seed_completed",
            component="memory",
            payload={"message": f"Seeded {len(receipts)} demo records", "severity": "info"}
        )
    return {"synthetic_non_operational": True, "receipts": receipts}


# -----------------------------------------------------------------------------
# Conflict Management
# -----------------------------------------------------------------------------

@router.get("/conflicts")
def get_conflicts():
    """List currently contested divergent alternative revisions formatted for dashboard."""
    if not MEMORY_SERVICE:
        raise HTTPException(status_code=503, detail="Memory service unavailable")
    raw_conflicts = MEMORY_SERVICE.get_conflicts()
    formatted = []
    for c in raw_conflicts:
        if "revisions" in c:
            formatted.append(c)
            continue
        cid = c.get("conflict_id") or c.get("id") or str(uuid.uuid4())
        rec_id = c.get("record_id") or "unknown"
        ver = c.get("version") or 1
        created_at = c.get("created_at") or ""
        obs_json = c.get("observation_json") or ""
        obs_str = ""
        try:
            if isinstance(obs_json, str) and (obs_json.startswith("{") or obs_json.startswith("[")):
                obs_dict = json.loads(obs_json)
                obs_str = obs_dict.get("description") or obs_dict.get("summary") or obs_dict.get("text") or str(obs_dict)
            else:
                obs_str = str(obs_json)
        except Exception:
            obs_str = str(obs_json)

        revisions = [
            {
                "id": c.get("operation_id") or f"r{ver}",
                "source": "local shard",
                "observation": obs_str or "Competing observation",
                "at": created_at,
            },
            {
                "id": c.get("competing_operation_id") or f"r{ver - 1}",
                "source": "server-prepared",
                "observation": "Prior revision state",
                "at": created_at,
            }
        ]
        formatted.append({
            "id": cid,
            "context": f"{rec_id} / v{ver}",
            "status": c.get("status") or "review",
            "revisions": revisions,
            **c,
        })
    return {"conflicts": formatted}


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
    raw = ENGINE.status()
    local_cnt = 0
    server_cnt = 0
    with ENGINE._lock:
        for reg in ENGINE._registrations.values():
            if reg.role == ShardRole.BASE:
                server_cnt += 1
            else:
                local_cnt += 1

    used_b = raw.get("usage_bytes")
    budget_b = raw.get("soft_budget_bytes")
    raw["memory"] = {
        "used_bytes": used_b,
        "budget_bytes": budget_b,
        "local_shards": local_cnt,
        "server_prepared": server_cnt,
    }
    raw["memory_used_bytes"] = used_b
    raw["memory_budget_bytes"] = budget_b
    raw["local_shards"] = local_cnt
    raw["server_prepared"] = server_cnt

    if TELEMETRY_HUB:
        metrics = TELEMETRY_HUB.get_metrics()
        raw["fps"] = metrics.get("fps")
        raw["p95_latency_ms"] = metrics.get("p95_latency_ms")
    return raw


class SetBudgetRequest(BaseModel):
    soft_budget_mb: float = Field(..., gt=0)


@router.post("/engine/budget")
def set_budget(req: SetBudgetRequest):
    """Update engine soft budget with immediate reconciliation."""
    if not ENGINE:
        raise HTTPException(status_code=503, detail="Engine unavailable")
    ENGINE.set_soft_budget_bytes(int(req.soft_budget_mb * 1024 * 1024))
    return engine_status()


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
    is_off = NETWORK_CONTROLLER.is_offline
    return {
        "is_offline": is_off,
        "dropped_requests_count": NETWORK_CONTROLLER.get_dropped_count(),
        "link_state": "fault injected" if is_off else "connected",
        "state": "offline" if is_off else "connected",
    }


@router.get("/network/status")
def get_network_status():
    if not NETWORK_CONTROLLER:
        raise HTTPException(status_code=503, detail="Network controller unavailable")
    is_off = NETWORK_CONTROLLER.is_offline
    return {
        "is_offline": is_off,
        "dropped_requests_count": NETWORK_CONTROLLER.get_dropped_count(),
        "link_state": "fault injected" if is_off else "connected",
        "state": "offline" if is_off else "connected",
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
# Event Log & Shards Inventory
# -----------------------------------------------------------------------------

def _format_event(e: Any) -> Dict[str, Any]:
    if hasattr(e, "model_dump"):
        d = e.model_dump(mode="json")
    elif isinstance(e, dict):
        d = dict(e)
    else:
        d = {}
    payload = d.get("payload") or {}
    msg = payload.get("message") or payload.get("summary") or payload.get("description") or payload.get("error") or str(payload or d.get("event_type", "Event"))
    sev = payload.get("severity") or ("error" if "fail" in str(d.get("event_type", "")).lower() or "error" in str(d.get("event_type", "")).lower() else "info")
    emitted = d.get("emitted_at") or ""
    time_str = str(emitted)
    if "T" in time_str:
        try:
            time_str = time_str.split("T")[1][:8]
        except Exception:
            pass
    return {
        "id": d.get("event_id") or d.get("id") or str(uuid.uuid4()),
        "at": time_str,
        "type": d.get("event_type") or d.get("type") or "system",
        "message": msg,
        "severity": sev,
        **d,
    }


def _get_shard_inventory() -> List[Dict[str, Any]]:
    if not ENGINE:
        return []
    shards = []
    with ENGINE._lock:
        for sid, reg in ENGINE._registrations.items():
            loc = "server_prepared" if reg.role == ShardRole.BASE else "local"
            shards.append({
                "id": sid,
                "protocol": "lifeline/2",
                "mutable": reg.role == ShardRole.LOCAL_WRITE,
                "activeContext": reg.role == ShardRole.CONTEXT,
                "availability": "resident" if sid in ENGINE._loaded else "unloaded",
                "eviction": "budget" if not reg.pinned else "pinned",
                "location": loc,
            })
    return shards


@router.get("/events")
def get_recent_events(limit: int = Query(50, ge=1, le=500)):
    """Fetch recent typed events and shard inventory for a reconnecting dashboard."""
    events = []
    if EVENT_BUS:
        raw_events = EVENT_BUS.get_history(limit=limit)
        events = [_format_event(e) for e in raw_events]
    shards = _get_shard_inventory()
    return {"events": events, "shards": shards}


@router.get("/shards")
def get_shards():
    """Return all registered shards and their residency states."""
    return {"shards": _get_shard_inventory()}
