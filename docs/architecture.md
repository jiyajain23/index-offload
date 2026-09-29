# LIFELINE Architecture & Engineering Specification

## 1. System Overview

LIFELINE is an offline-first memory and retrieval management system tailored for inspection robotics and rugged mobile devices operating under strict hardware constraints (RAM and CPU). The device continuously runs an object detection workload while inspecting industrial infrastructure, recording observations, querying emergency procedures, and synchronizing with an ingestion/indexing server when connectivity is available.

```
                      +---------------------------------------+
                      |         Person C: Client / UI         |
                      |  (Detector Telemetry, Camera, UI App) |
                      +-------------------+-------------------+
                                          | HTTP / Lifeline API
                                          v
+-----------------------------------------------------------------------------------+
| LIFELINE Edge Device Host (Constrained RAM / CPU Budget: e.g. 512MB RAM, 1 Core)   |
|                                                                                   |
|  +-------------------------------------+   +------------------------------------+  |
|  |     Person B: Durable Memory        |   |      Person A: Engine Correctness  |  |
|  |                                     |   |                                    |  |
|  |  +-------------------------------+  |   |  +------------------------------+  |  |
|  |  | SQLite Transactional Store    |  |   |  | ShardManager (IEngine)       |  |  |
|  |  | - Immutable operations log    |  |   |  | - Active-Operation Leases    |  |  |
|  |  | - Accepted current records    |  |   |  | - Non-evictable local_write  |  |  |
|  |  | - Outbox & urgent priority    |  |   |  | - Pinned protocol shards     |  |  |
|  |  | - Tombstones & conflicts      |  |   |  | - Working-set LRU eviction   |  |  |
|  |  | - Urgent reserve protection   |  |   |  | - Bounded queue & deadlines  |  |  |
|  |  +---------------+---------------+  |   |  +--------------+---------------+  |  |
|  |                  |                  |   |                 |                  |  |
|  |                  v                  |   |                 v                  |  |
|  |  +-------------------------------+  |   |  +------------------------------+  |  |
|  |  | MemoryService (IMemory)       |  |   |  | Qdrant Edge Shard Store     |  |  |
|  |  | - Durable write before proj.  |--+---+->| - Dedicated local_write shard|  |  |
|  |  | - Public search reconciliation|  |   |  | - On-demand context shards   |  |  |
|  |  | - Conflict review & resolve   |  |   |  | - Hybrid / Dense retrieval   |  |  |
|  |  +---------------+---------------+  |   |  +------------------------------+  |  |
|  +------------------|------------------+   +------------------------------------+  |
|                     |                                         ^                   |
|                     v                                         | Activation        |
|  +------------------------------------------------------------+-----------------+ |
|  | Sync & Snapshot Subsystem                                                    | |
|  |  - UploadWorker: Jittered backoff, urgent priority queue, privacy enforcement| |
|  |  - SnapshotManager: Staging download, SHA256 checksums, atomic handoff       | |
|  |  - NetworkFaultController: Real transport fault injection                    | |
|  +------------------------------------------------------------------------------+ |
+-----------------------------------------------------------------------------------+
                                          |
                      Server Outbox Sync  |  Snapshot Download
                      (HTTP / Wire)       |  (Bounded tar stream)
                                          v
+-----------------------------------------------------------------------------------+
| LIFELINE Server (Cloud / Fleet Base Outside Edge Limits)                          |
|  - ServerMemoryStore: Transactional deduplication of incoming operation batches   |
|  - ServerIndexPreparer: EdgeShard.create + optimize() outside device budget       |
|  - Snapshot Distribution: Manifest generation with SHA256 & record inventory      |
+-----------------------------------------------------------------------------------+
```

---

## 2. Ownership & Separation of Concerns

### Person A: Engine Correctness & Resource Controls
- **Shard Registry & Lifecycle**: Registers base context shards, pinned shards, and dedicated `local_write` shard.
- **Active-Operation Leases**: Thread-safe `lease_shard()` prevents handles from being evicted or closed while search queries or projection writes are in flight.
- **Resource Monitor**: Dual-mode cgroup v2 accounting with fallback to process RSS/working set. Enforces soft budget thresholds, hysteresis recovery bands, and eviction cooldowns.
- **Bounded Query Queue**: Thread-safe admission control with configurable depth limit and deadline awareness. Expired requests fail truthfully without touching the disk.
- **Cross-Shard Merging**: Normalizes scores and ranks hits across searched shards. Projects precedence to `local_write` hits over base shards for identical points.
- **Snapshot Activation**: Atomically validates staged candidate archives (checksum verification, path traversal checks, schema compatibility) and swaps shard directories with rollback protection.

### Person B: Durable Memory, Privacy, and Sync
- **Authoritative Persistence**: Transactional SQLite store (`SQLiteMemoryStore`) committing operations log, current records, tombstones, and outbox in single atomic transactions.
- **Write Durability Precedence**: Write operations are committed to SQLite *before* calling the search engine projection. If the projection fails, `durable=True` and `projection_status="pending"` is returned without failing the write.
- **Revision & Conflict Semantics**: Sequential edits increment version and supersede parents. Concurrent branch revisions are flagged as `contested` and stored as alternatives. Explicit resolution records resolve contested states.
- **Public Search Reconciliation**: Reconciles raw candidate hits from Person A with SQLite authoritative state: excludes tombstones, suppresses superseded base records, flags conflicts, and refills within deadline.
- **Privacy Enforcement**: Strict policy distinction between `LOCAL_ONLY` and `PERMITTED_SHARED`. `LOCAL_ONLY` records are blocked from outbox serialization at creation, retry, and sync time.
- **Capacity Policy**: Configurable byte budget with a reserved headroom strictly dedicated to `URGENT` records. Routine writes exceeding regular capacity fail visibly with `StorageCapacityError`.
- **Server Sync & Ingestion**: Idempotent HTTP upload worker prioritizing urgent batches over routine batches. Server index preparer builds optimized `.tar` snapshots with SHA256 checksums.

---

## 3. Shared Contract Models (`shared/contracts.py`)

All components communicate through strictly typed, frozen Pydantic contracts:
- **`RecordEnvelope`**: Canonical representation of an observation or procedure card. Contains `record_id`, `operation_id`, `entity_id`, `context_id`, `version`, `parent_version`, `status`, `tombstone`, `sharing`, `priority`, `dense_vector`, `sparse_indices`, `sparse_values`.
- **`SearchRequest`**: Carries `query_text`, `dense_vector`, `sparse_indices`, `sparse_values`, `context`, `include_protocols`, `k`, and `deadline_ms`.
- **`SearchResponse`**: Returns `hits` (`MemorySearchHit`), `coverage` (`CoverageSummary`), `elapsed_ms`, `partial`, and structured `reasons`.
- **`CoverageSummary`**: Truthfully distinguishes shards that were:
  - `searched`
  - `queued`
  - `unavailable_offline`
  - `skipped_for_budget`
  - `failed`
- **`WriteReceipt`**: Confirms `record_id`, `operation_id`, `version`, `durable: bool`, and `projection_status`.
- **`SnapshotCandidate` & `ActivationReceipt`**: Contract for the two-stage handoff between Person B (download & staging) and Person A (atomic activation).

---

## 4. Failure and Error Semantics

| Scenario | Behavior | State Guaranteed |
| :--- | :--- | :--- |
| **Qdrant projection fails during write** | SQLite commit completes; write returns `durable=True, projection_status="pending"`. Background replay retries projection. | No data loss. Authoritative record is persisted. |
| **Outbox upload fails (offline link)** | UploadWorker logs network failure, records attempt count, applies exponential backoff with jitter. | Outbox state remains `queued`. Unsynchronized records survive restart. |
| **Duplicate upload retry** | Server validates `operation_id` in `ServerMemoryStore`. If previously processed with same hash, returns stored acknowledgment. | Server does not duplicate records or revisions. |
| **Conflicting concurrent edits** | Both edits are accepted into SQLite. Record status transitions to `CONTESTED`. Search returns alternatives flagged as contested. | Neither revision is lost. Both are searchable. |
| **Corrupted / Invalid Snapshot** | Person A verifies SHA256 and validates unpack. If invalid, unpack is wiped and active shard is restored from backup. | No downtime. Previous base shard remains active. |
| **Memory Pressure Exceeds Soft Budget** | ShardManager rejects on-demand loading with `BudgetExceeded`. Search returns truthful `skipped_for_budget` coverage. | Pinned shards and local_write are never evicted. Edge application does not OOM. |
| **Telemetry Listener Throws Exception** | EventBus catches listener exception, logs error, and isolates caller. | Storage or search operation is never aborted due to telemetry failure. |
