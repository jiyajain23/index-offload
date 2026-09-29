# LIFELINE: Offline-First Memory & Resource Control System

LIFELINE is an offline-first memory and retrieval management system for inspection robotics and rugged mobile devices operating under strict hardware constraints (RAM and CPU).

The edge device keeps a detector model running without dropping frames, records observations while offline, preserves accepted writes across crashes and restarts, queries pinned emergency procedures and local context shards, uploads records when network connectivity returns, and safely stages and activates server-prepared vector index updates.

---

## 1. Supported Runtimes & Dependency Baseline

The implementation pins the tested baseline in `requirements.txt`:

```text
qdrant-edge-py==0.8.0
qdrant-client==1.19.1
numpy>=2.0.0
psutil>=7.0.0
pytest>=9.0.0
fastapi>=0.140.0
pydantic>=2.10.0
uvicorn>=0.30.0
httpx>=0.28.0
```

Supported Environments:
- **Operating Systems**: Linux (cgroup v2), Windows 10/11 (PowerShell, `py` launcher, local process fallback), and WSL2.
- **Python Runtime**: Python 3.10 through 3.14 (Tested on Python 3.14.6 AMD64).
- **Qdrant Edge Baseline**: `qdrant-edge-py 0.8.0` with standard `.tar` archive unpacking.

---

## 2. Architecture & Ownership Boundaries

- **Person A (Engine)**: Shard registry, active-operation leases (`lease_shard`), soft working-set budget enforcement, LRU eviction with cooldown/hysteresis, dedicated non-evictable `local_write` shard, pinned protocol shards, bounded query queue, cross-shard ranking, and atomic snapshot activation with rollback.
- **Person B (Durable Memory & Sync)**: SQLite transactional persistence, revision/conflict semantics, urgent storage reserve protection, privacy filters (`LOCAL_ONLY` records never leave edge), public search reconciliation (tombstones, supersedes, refill), server ingestion deduplication, urgent-priority upload worker, and server snapshot staging.
- **Person C (Boundary & Telemetry)**: Fast HTTP endpoints, detector telemetry ingestion, sliding-percentile contention tracking, and deterministic contention stubs (`is_synthetic=True`).

For complete architecture details and data flows, see [docs/architecture.md](docs/architecture.md).  
For client integration instructions and examples, see [docs/person_c_guide.md](docs/person_c_guide.md).

---

## 3. Quickstart & Local Startup

### 3.1 Local Environment Setup

```powershell
# 1. Install dependencies
py -m pip install -r requirements.txt

# 2. Run test suite to verify installation
py -m pytest -v
```

### 3.2 Launching the Edge Backend

Start the LIFELINE Edge FastAPI server locally:

```powershell
py -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

The service initializes:
- SQLite durable database at `./data/edge_memory.db`
- Dedicated mutable projection shard at `./data/shards/local_write`
- Default procedure cards and multi-zone seed fixtures
- Event bus and background sync workers

### 3.3 Running in Docker (512MB Capped Edge Environment)

Build the edge image:

```powershell
docker build -t lifeline-edge .
```

Run with a 512 MB memory limit and swap disabled to simulate rugged edge constraints:

```powershell
docker run --rm --name lifeline-device `
  --memory=512m --memory-swap=512m `
  -p 8000:8000 `
  lifeline-edge
```

---

## 4. Edge Resource Limits & Server Separation

### Edge Resource Accounting
The edge device monitors memory via `engine/monitor.py`:
- **cgroup v2**: Reads `/sys/fs/cgroup/memory.current`, `memory.stat` (`inactive_file`), and `memory.events`. The edge working set is computed as `memory.current - inactive_file`.
- **Local Fallback**: On Windows/non-cgroup systems, uses `psutil.Process().memory_info().rss` and working set. All telemetry explicitly reports `source="cgroup_v2"` or `source="local_process"`.

### Soft Budget & Eviction Policy
- **Pinned Shards & `local_write`**: Non-evictable. Guaranteed to remain loaded in memory.
- **On-Demand Context Shards**: Evicted using deterministic LRU order when projected load exceeds `soft_budget_mb`.
- **Hysteresis & Cooldown**: Eviction triggers a cooldown timer and recovery band, preventing thrashing under rapid context switching.
- **Bounded Query Admission**: Queries waiting on busy handles enter a bounded queue (`max_queue_depth=50`) with deadline awareness (`deadline_ms`).

### Server Separation
The LIFELINE Server runs as an independent service outside the edge device's memory and CPU budget. Heavy operations—such as multi-point indexing and `shard.optimize()`—are executed strictly on the server during snapshot compilation, never on the constrained edge device.

---

## 5. Offline Fault Control & Sync

LIFELINE includes a transport-level network fault controller (`engine/network_control.py`) to simulate connectivity drops without modifying host networking:

```python
import httpx

# 1. Simulate entering a network-dead zone
httpx.post("http://localhost:8000/api/v1/dev/network", json={"offline": True})

# 2. Writes continue normally and durably offline
# Permitted and local-only records are accepted into SQLite

# 3. Restore connectivity
httpx.post("http://localhost:8000/api/v1/dev/network", json={"offline": False})

# 4. Trigger sync flush
httpx.post("http://localhost:8000/api/v1/sync/trigger")
```

---

## 6. Running Tests

Run the complete test suite:

```powershell
py -m pytest -v
```

### Test Coverage Highlights:
- `tests/test_engine_advanced.py`: Concurrency safety (active operation leases protect readers from eviction), startup configuration checks, queue bounds, deadline enforcement, cross-shard ranking, version guards, and safe snapshot activation with checksum verification and rollback.
- `tests/test_memory_advanced.py`: Transactional durability across restarts, crash recovery between commit and projection, idempotency, concurrent conflict flagging, explicit conflict resolution, tombstone exclusions, privacy guarantees (`LOCAL_ONLY` outbox exclusion), urgent reserve capacity, and event bus error isolation.
- `tests/test_sync_and_server.py`: Server ingestion deduplication, urgent-first outbox worker with jittered backoff, server index compilation, and atomic snapshot download/activation.
- `tests/test_smoke_scenario.py`: 15-step end-to-end integration test exercising edge persistence, real offline transport breakage, urgent outbox prioritization, server preparation, concurrent edit survival, and privacy verification.

---

## 7. Running Reproducible Benchmarks

Execute the benchmark suite comparing **Naive full-load**, **Tuned on-disk unmanaged**, and **Managed ShardManager** under concurrent detector contention:

```powershell
py benchmark/runner.py --queries 40 --shards 8 --points 500 --dim 64 --budget-mb 250
```

Results are saved to `benchmark/results.csv` and `benchmark/results.json`.

### Measured Benchmark Results (Summary)

*Hold-Constant: seed=42, 64-dim, 8 shards, 500 pts/shard, 30 FPS synthetic detector contention stub.*

| Mode | Search P95 Latency | Queue P95 Time | Detector Contention FPS | Exact Recall | Relevant Shard Coverage | Peak RSS | Architectural Characteristics |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Naive** | 191.19 ms | 0.00 ms | 15.0 fps | 1.000 (100%) | 100.0% | 261.5 MB | Eagerly loads all shards; high memory footprint, degrades detector frame rate. |
| **Unmanaged** | 166.24 ms | 0.00 ms | 12.3 fps | 1.000 (100%) | 100.0% | 81.0 MB | Opens/closes shard per query; causes severe disk I/O churn that starves detector. |
| **Managed** | **100.17 ms** | **0.11 ms** | **21.0 fps** | **1.000 (100%)** | **100.0%** | **226.4 MB** | **LRU retention + active leases + soft budget; achieves fastest latency and highest FPS.** |

---

## 8. Projection Rebuild & Snapshot Recovery

### Rebuilding Projections from Authoritative SQLite State
The SQLite database (`SQLiteMemoryStore`) is the authoritative source of truth. If the Qdrant Edge shard files become corrupted or are deleted:
```python
from memory.service import MemoryService
# Replay all pending or current records from SQLite into ShardManager
replayed = memory_service.replay_pending_projections()
print(f"Rebuilt {replayed} projected records into search index.")
```

### Snapshot Compatibility & Atomic Rollback
- Server snapshots are packaged as `.tar` archives containing a full `EdgeShard` index created with `EdgeConfig(vectors={"embedding": ...})`.
- Before activating a snapshot, `ShardManager` validates the archive's SHA256 checksum and extracts it to a temporary staging folder with path traversal guards.
- The active shard directory is backed up before swap. If loading the new shard fails, `ShardManager` immediately restores the previous base from backup and reports `reverted_to_previous=True`.

---

## 9. Known Limitations & Constraints

1. **Docker Cgroup on Windows Host**: Running native Windows Python relies on `psutil` process working-set metrics because `/sys/fs/cgroup` is only present in Linux/container environments. Production deployments should run in the provided Linux container.
2. **Qdrant Edge In-Process Snapshots**: In `qdrant-client`, local in-process snapshot creation (`create_snapshot`) is unsupported by the C-core binding. LIFELINE solves this by building and optimizing server snapshots using real `EdgeShard.create` outside the edge budget.
3. **Sparse Vector Hardware**: Sparse vector inference requires compatible sparse embedding models. When sparse vectors are not provided, LIFELINE gracefully operates in dense-only retrieval mode.
4. **Real Perception Telemetry**: Detector frame metrics currently default to the deterministic contention stub (`DeterministicDetectorStub`) labeled `is_synthetic=True` until Person C connects a live hardware camera and inference engine.
