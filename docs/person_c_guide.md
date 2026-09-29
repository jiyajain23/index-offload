# Person C Integration Guide: Perception & Client Boundaries

This guide describes how Person C (Detector, Camera, Embedding Pipeline, and User Interface) integrates with the completed LIFELINE Edge Backend.

---

## 1. Edge API Overview

The LIFELINE Edge service runs a lightweight FastAPI backend (default port `8000`).

Base URL: `http://localhost:8000/api/v1`

### Summary of Mountable Endpoints

| Category | Method & Path | Description |
| :--- | :--- | :--- |
| **Observation Writes** | `POST /records` | Record a new observation or procedure with vector embedding. |
| **Authoritative Search** | `POST /search` | Execute context-aware semantic search with coverage reporting. |
| **Conflict Management** | `GET /conflicts` | List unresolved concurrent revisions. |
| | `POST /conflicts/resolve` | Submit an explicit resolution record superseding competing edits. |
| **Detector Telemetry** | `POST /detector/telemetry` | Submit real frame time and FPS telemetry from detector models. |
| | `GET /detector/metrics` | Retrieve aggregated detector contention and FPS statistics. |
| **Sync & Outbox** | `GET /sync/status` | Inspect current outbox backlog, urgent items, and acknowledged records. |
| | `POST /sync/trigger` | Manually trigger outbox flush and pending uploads. |
| **Engine & Shards** | `GET /engine/resources` | Query cgroup memory usage, active leases, and loaded shards. |
| **Fault Injection** | `POST /dev/network` | Toggle offline/online link simulation (`{"offline": true}`). |

---

## 2. Ingesting Observations from Detector

When Person C's perception model detects an anomaly, pump defect, or safety violation, call `POST /api/v1/records`:

```python
import httpx
import uuid
from datetime import datetime, timezone

observation_payload = {
    "record_id": f"obs-{uuid.uuid4().hex[:8]}",
    "operation_id": f"op-{uuid.uuid4().hex[:12]}",
    "entity_id": "pump_station_alpha",
    "context_id": "zone_01",
    "device_id": "inspection_robot_01",
    "observation": {
        "label": "bearing_overheat_warning",
        "temperature_c": 78.4,
        "vibration_rms": 4.6,
        "bounding_box": [120, 45, 310, 280]
    },
    "priority": "urgent",             # "routine" | "urgent"
    "sharing": "permitted_shared",     # "permitted_shared" | "local_only"
    "source_type": "detector",
    "embedding_model_version": "minilm-l6-v2",
    "dense_vector": [0.12] * 384,      # 384-dimensional vector from embedding model
    "confidence": 0.94,
    "confidence_source": "yolo-v8s-inspections"
}

resp = httpx.post("http://localhost:8000/api/v1/records", json=observation_payload)
print(resp.json())
# Response:
# {"record_id": "obs-...", "operation_id": "op-...", "version": 1, "durable": true, "projection_status": "ready"}
```

> **Privacy Rule**: If an observation contains sensitive or confidential operational data, set `"sharing": "local_only"`. LIFELINE guarantees this record will **never** enter the outbound upload queue or be sent to the remote server.

---

## 3. Querying Procedures and Incident History

When the operator or autonomous planner needs emergency instructions or past incident cards for a zone:

```python
search_req = {
    "context": "zone_01",
    "dense_vector": [0.12] * 384,
    "include_protocols": True,        # Also search pinned emergency procedures
    "k": 5,
    "deadline_ms": 500.0              # Strict deadline
}

resp = httpx.post("http://localhost:8000/api/v1/search", json=search_req)
data = resp.json()

print(f"Elapsed: {data['elapsed_ms']} ms, Partial: {data['partial']}")
print("Coverage:", data["coverage"])

for hit in data["hits"]:
    print(f"[{hit['score']:.3f}] {hit['record_id']} (v{hit['version']}) Contested: {hit['is_contested']}")
    print("Observation / Procedure:", hit["observation"])
```

### Understanding Coverage Output

Search responses always report truthful shard coverage:
```json
{
  "coverage": {
    "searched": ["local_write", "emergency_protocols", "zone_01"],
    "queued": [],
    "unavailable_offline": [],
    "skipped_for_budget": [],
    "failed": []
  }
}
```
If a context shard was evicted to preserve detector headroom, it will appear in `"skipped_for_budget"`, and `partial` will be `true`. Person C should display a visual indicator indicating that context history was constrained to protect device FPS.

---

## 4. Submitting Detector Telemetry

Person C's inference loop should periodically stream frame processing times:

```python
telemetry_data = {
    "frame_latency_ms": 28.5,
    "dropped_frames": 0,
    "queue_depth": 1,
    "model_name": "yolo-v8s-inspections",
    "is_synthetic": False
}

httpx.post("http://localhost:8000/api/v1/detector/telemetry", json=telemetry_data)
```

The backend aggregates these metrics into sliding percentiles (P50, P95, P99, and instant FPS) accessible via `GET /api/v1/detector/metrics`.

---

## 5. Offline Testing and Stubs

### Deterministic Contention Stub
For benchmarking or headless testing without real cameras or GPUs, use `DeterministicDetectorStub`:

```python
from app.telemetry import DetectorTelemetryHub, DeterministicDetectorStub

hub = DetectorTelemetryHub()
stub = DeterministicDetectorStub(hub=hub, target_fps=30.0)
stub.start()

# Metrics will automatically report is_synthetic=True
metrics = hub.get_metrics()
print("Synthetic FPS:", metrics["fps"], "Provenance:", metrics["provenance"])

stub.stop()
```

### In-Memory Fakes (`shared/fakes.py`)
For testing UI components or edge workflows in unit tests without disk dependencies:
- `FakeEngine`: Deterministic in-memory `IEngine` supporting leases, simulated budgets, and mock projections.
- `FakeMemory`: Deterministic in-memory `IMemory` simulating record versioning, conflict tracking, and search reconciliation.
