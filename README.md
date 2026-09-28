# LIFELINE

LIFELINE is an offline-first edge memory and resource-management system for an
inspection robot or rugged worker device with intermittent connectivity.

The current engine milestone protects detector capacity by keeping critical
memory resident, managing disposable context shards below a soft memory budget,
and making every search's coverage explicit.

## Current Progress

### Previously validated

The following validation work was completed before the engine implementation:

- Qdrant Edge runs in the Windows, WSL2, Linux-container environment with
  `qdrant-edge-py 0.8.0`.
- Local vector insertion works with the validated EdgeShard API.
- Fresh, unindexed vectors are searchable offline.
- A 10,000-vector shard changes from zero indexed vectors to 10,000 indexed
  vectors after `optimize()`.
- Docker's `--memory=512m` limit maps to cgroup `memory.max=536870912`, and an
  over-limit workload was terminated with `OOMKilled=true` and exit code 137.
- In this exact configuration, loading each 10,000-vector / 384-dimensional
  shard added about 37 MB RSS and closing it released approximately that amount.
  This is an observed measurement, not a general Qdrant memory formula.
- The existing `exp2_budget.py` experiment compares naive loading with a small
  managed policy under a hard container limit.

### Added in this milestone

- `engine.monitor.ResourceMonitor` reads process RSS/CPU and cgroup-v2
  `memory.current`, `memory.max`, working set, pressure, and OOM events.
- `engine.shard_manager.ShardManager` registers shards, loads pinned shards,
  enforces a soft budget, and evicts only on-demand shards using deterministic
  LRU order.
- An unexpected real load cost is rejected and immediately closed rather than
  being left resident above the soft budget.
- `engine.events.EventEmitter` records and publishes `shard_loaded`,
  `shard_evicted`, `budget_update`, and load-failure/rejection events.
- `edge.qdrant_adapter.QdrantEdgeShardStore` isolates the already-validated
  Qdrant Edge `load`, `query`, and `close` calls from policy code.
- Searches return honest coverage:

```python
{
    "hits": [...],
    "coverage": {
        "searched": ["emergency_protocols", "zone_01"],
        "queued": ["zone_02"],
        "unavailable_offline": ["zone_20"],
    },
}
```

- Unit tests cover cgroup metrics, pinned protection, LRU eviction, and queued
  / unavailable coverage. An integration test creates, loads, and queries an
  actual temporary Qdrant Edge shard.


## 1. Setup

Clone the repo and install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Generate the local Qdrant Edge test shards:

```powershell
python exp2_budget.py make
```

This creates:

```text
edge_data/
├── exp2_shard00/
├── exp2_shard01/
├── ...
└── exp2_shard19/
```

`edge_data/` is generated locally and should **not** be committed to Git.

Add to `.gitignore` if needed:

```gitignore
edge_data/
```

---

## 2. Run Tests

```powershell
pytest -q
```

Current Engine tests should show:

```text
5 passed
```

---

## 3. Run the Engine Demo

Build the Docker image:

```powershell
docker build -t lifeline-engine .
```

Run under a 512 MB hard memory limit:

```powershell
docker run --rm --memory=512m `
  -v "${PWD}/edge_data:/data" `
  lifeline-engine `
  python -m engine.demo_lifecycle `
  --protocols /data/exp2_shard00 `
  --zone-01 /data/exp2_shard01 `
  --zone-02 /data/exp2_shard02 `
  --estimated-shard-cost-mb 60 `
  --auto-lru-budget
```

The demo should show:

```text
emergency_protocols loaded → pinned
zone_01 loaded
zone_01 evicted when another shard needs space
zone_02 loaded
search results + coverage reported
```

No OOM should occur.

---


## 6. Important

Do **not** modify the Qdrant Edge API based on assumptions. The project currently uses:

```text
qdrant-edge-py==0.8.0
```

The existing adapter contains the validated API usage.

For new work, build against the existing Engine interfaces first and only change Engine code when integration actually requires it.


