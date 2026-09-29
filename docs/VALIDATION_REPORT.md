# LIFELINE — Experiment & Validation Summary

These experiments were completed before and during the Engine implementation to validate the technical assumptions behind LIFELINE.

---

## 1. Qdrant Edge Installation & Environment

**Goal:** Verify that Qdrant Edge works in the development environment.

**Environment:**

```text
Windows
WSL2
Docker Desktop
Python 3.11
qdrant-edge-py 0.8.0
```

**Result:** ✅ Passed

Qdrant Edge successfully ran inside the Linux Docker environment.

---

## 2. Local EdgeShard Creation & Insertion

**Goal:** Verify that vectors can be stored locally without relying on a remote Qdrant server.

**Test:**

* Created a local `EdgeShard`
* Inserted vectors using the validated `upsert_points()` API
* Checked point count

**Result:** ✅ Passed

Local persistent shards can be created and populated offline.

---

## 3. Fresh / Unindexed Vector Search

**Goal:** Determine whether vectors need to be HNSW-indexed before they can be searched.

**Test:**

* Inserted fresh vectors
* Did not immediately run optimization/indexing
* Queried the shard

**Result:** ✅ Passed

Fresh, unindexed vectors were searchable.

This means LIFELINE can defer expensive indexing work without making newly written data completely unsearchable.

---

## 4. Indexing Lifecycle

**Goal:** Understand what happens when a larger shard is optimized.

**Configuration:**

```text
10,000 vectors
384 dimensions
```

Before optimization:

```text
points_count = 10000
indexed_vectors_count = 0
```

After `optimize()`:

```text
points_count = 10000
indexed_vectors_count = 10000
```

**Result:** ✅ Passed

Indexing can be explicitly controlled and deferred.

This supports the design where the edge device can avoid expensive indexing while the server performs heavier indexing work.

---

## 5. Docker Hard Memory Limit

**Goal:** Verify that Docker provides a real hard memory boundary.

**Configuration:**

```text
Docker --memory=512m
```

Inside the container:

```text
memory.max = 536870912
```

An intentionally oversized workload attempted to retain approximately 2 GB.

Result:

```text
OOMKilled = true
ExitCode = 137
```

**Result:** ✅ Passed

The Docker cgroup hard memory limit is enforced.

This provides the **hard safety boundary** underneath LIFELINE's application-level memory policy.

---

## 6. Multi-Shard Memory Cost

**Goal:** Measure the actual memory cost of keeping multiple Edge shards resident.

**Configuration:**

```text
10,000 vectors / shard
384 dimensions
2 shards
qdrant-edge-py 0.8.0
```

Observed RSS:

| State          |      RSS |
| -------------- | -------: |
| Initial        | ~18.4 MB |
| Shard 1 loaded | ~56.7 MB |
| Shard 2 loaded | ~93.6 MB |
| Shard 2 closed | ~57.4 MB |
| Shard 1 closed | ~20.7 MB |

Approximate observed cost:

```text
~37 MB / shard
```

**Result:** ✅ Passed

Loading additional shards creates measurable memory pressure, and closing them releases approximately the same amount.

**Important:** ~37 MB is an observation for this exact configuration, not a universal Qdrant memory formula.

---

## 7. Managed LRU Experiment

**Goal:** Test whether context shards can be managed under a memory budget while protecting pinned data.

The existing `exp2_budget.py` experiment compares:

```text
Naive loading
vs
Managed loading
```

The managed policy:

* keeps pinned shards resident
* tracks loaded context shards
* uses LRU ordering
* estimates shard cost
* evicts non-pinned shards when necessary
* records memory/search measurements

**Result:** ✅ Passed as an initial resource-management experiment.

This experiment became the basis for the actual `ShardManager`.

---

## 8. Resource Monitor Validation

**Goal:** Verify that the engine can observe container resource state.

`ResourceMonitor` reads:

```text
RSS
CPU
memory.current
memory.max
working set
memory pressure
memory events
OOM counters
```

**Result:** ✅ Passed

The lifecycle demo successfully reported values such as:

```json
{
  "hard_memory_limit_bytes": 536870912,
  "usage_bytes": 91332608,
  "rss_bytes": 100061184,
  "working_set_bytes": 19705856,
  "memory_events": {
    "oom": 0,
    "oom_kill": 0
  }
}
```

---

## 9. Pinned Shard Protection

**Goal:** Verify that critical memory cannot be evicted by normal budget pressure.

Test configuration:

```text
emergency_protocols → pinned
zone_01 → on-demand
zone_02 → on-demand
```

When another context shard required space:

```text
zone_01 → evicted
zone_02 → loaded
emergency_protocols → remains loaded
```

**Result:** ✅ Passed

This validates the core LIFELINE invariant:

> Critical pinned memory is protected while disposable context memory is managed.

---

## 10. Actual Post-Load Budget Check

**Goal:** Avoid trusting only estimated shard costs.

The manager loads a shard and measures its actual resource cost.

If the real cost cannot fit within the configured budget, the shard is immediately closed/rejected rather than remaining resident above the soft budget.

**Result:** ✅ Passed

This protects against inaccurate memory estimates.

---

## 11. Search Coverage

**Goal:** Ensure that memory excluded because of resource constraints is not silently ignored.

The search interface returns:

```python
{
    "hits": [...],
    "coverage": {
        "searched": [...],
        "queued": [...],
        "unavailable_offline": [...]
    }
}
```

Example observed result:

```json
{
  "searched": [
    "emergency_protocols",
    "zone_01"
  ],
  "queued": [],
  "unavailable_offline": []
}
```

**Result:** ✅ Passed

This validates the "honest coverage" concept:

> A search result must indicate what memory was actually searched.

---

## 12. Full Docker Engine Lifecycle

**Goal:** Verify that the implemented Engine works together under an actual Docker memory constraint.

Command:

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

Observed:

```text
emergency_protocols loaded
zone_01 loaded
zone_01 evicted
zone_02 loaded
search successful
no OOM
```

The pinned shard remained protected throughout normal budget pressure.

**Result:** ✅ Passed

This is the current end-to-end Engine validation.

---

# Overall Validation Status

| Experiment                     | Result |
| ------------------------------ | ------ |
| Qdrant Edge environment        | ✅      |
| Local EdgeShard creation       | ✅      |
| Local vector insertion         | ✅      |
| Fresh/unindexed search         | ✅      |
| Indexing lifecycle             | ✅      |
| Docker 512 MB hard limit       | ✅      |
| Multi-shard memory measurement | ✅      |
| Managed LRU experiment         | ✅      |
| ResourceMonitor                | ✅      |
| Pinned shard protection        | ✅      |
| Post-load budget validation    | ✅      |
| Honest search coverage         | ✅      |
| Docker Engine lifecycle        | ✅      |

---

# What These Experiments Prove

The experiments establish the technical foundation for LIFELINE:

```text
Qdrant Edge
     ↓
Local offline search works
     ↓
Indexing can be deferred
     ↓
Multiple shards consume measurable memory
     ↓
Docker provides a hard memory boundary
     ↓
Application can manage shards below that boundary
     ↓
Pinned critical memory can be protected
     ↓
Disposable context can be evicted
     ↓
Search can report exactly what was/wasn't searched
```
