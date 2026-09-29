# LIFELINE Implementation Checklist and Completion Matrix

## Scope & Status
- **Person A (Engine Correctness & Lifecycle)**: COMPLETE & VERIFIED
- **Person B (Durable Memory, Sync & Server)**: COMPLETE & VERIFIED
- **Person C Boundary (App Routes & Telemetry)**: COMPLETE & VERIFIED
- **Smoke Scenario (15-step Real Integration)**: COMPLETE & VERIFIED (30/30 pytest passing)
- **Benchmark Runner & Metrics**: COMPLETE & VERIFIED (`benchmark/results.csv`, `benchmark/results.json`)

---

## 1. Audit & Environment Verification
- [x] Python runtime inspected: Python 3.14.6 Windows AMD64 (`py` launcher).
- [x] Dependencies pinned in `requirements.txt`: `qdrant-edge-py==0.8.0`, `qdrant-client==1.19.1`, `numpy>=2.0.0`, `psutil>=7.0.0`, `pytest>=9.0.0`, `fastapi>=0.140.0`, `pydantic>=2.10.0`, `uvicorn>=0.30.0`, `httpx>=0.28.0`.
- [x] `qdrant-edge-py 0.8.0` SDK capabilities tested and verified:
  - `EdgeShard.create`, `load`, `close`, `query`, `search`, `update`, `optimize`.
  - Dense query, sparse vector query, hybrid RRF fusion (`Fusion.Rrf(60)`).
  - Snapshot methods: `unpack_snapshot`, `snapshot_manifest`, `update_from_snapshot`.
  - Point upsert and point delete.
- [x] Test suite passing: 30/30 tests pass.
- [x] OS & cgroup limitations documented: Windows host with automatic local process RSS/working set fallback and clear source labeling (`cgroup_v2` vs `local_process`).

---

## 2. Shared Contracts & Event Bus (`shared/`)
- [x] Record envelope (`shared/contracts.py`):
  - `RecordEnvelope` with stable `record_id`, idempotent `operation_id`, `entity_id`, `context_id`, `device_id`, structured `observation`, `version`, `parent_version`, `status`, `tombstone`, `sharing`, `priority`, vectors, and confidence.
- [x] Search request & response models:
  - `SearchRequest` (query text, dense/sparse vectors, context, include_protocols, k, deadline_ms).
  - `SearchResponse` (hits with provenance, `CoverageSummary`, elapsed_ms, partial, shortfall, structured reasons).
- [x] Engine & Memory abstract interfaces:
  - `IEngine`: `query_shards`, `upsert_projection`, `delete_projection`, `activate_snapshot`, `status`, `close_all`.
  - `IMemory`: `write`, `search`, `delete`, `revise`, `get_conflicts`, `resolve_conflict`, `sync_status`.
- [x] Event bus (`shared/events.py`):
  - Thread-safe `EventBus` with error isolation: listener exceptions never fail caller or corrupt storage operations.
- [x] Deterministic Fakes (`shared/fakes.py`):
  - `FakeEngine` and `FakeMemory` matching real contracts for headless and fast unit testing.

---

## 3. Person A: Engine Correctness (`engine/`, `edge/`)
- [x] Shard registry & lifecycle with context mapping (one context -> multiple shards).
  - *Source*: `engine/shard_manager.py` (`register_shard`, `_registrations`).
  - *Test*: `tests/test_shard_manager.py`, `tests/test_engine_advanced.py`.
- [x] Active-operation leases (`lease_shard`):
  - Concurrency control preventing eviction, close, or snapshot replacement underneath in-flight readers or writers.
  - *Source*: `engine/shard_manager.py` (`lease_shard`).
  - *Test*: `tests/test_engine_advanced.py::test_concurrent_query_and_eviction_protects_reader`.
- [x] Pinned shards protection + dedicated non-evictable `local_write` shard:
  - *Source*: `engine/shard_manager.py` (`_evict_until_room`).
  - *Test*: `tests/test_shard_manager.py::test_lru_evicts_oldest_on_demand_but_never_pinned`.
- [x] Startup validation:
  - Rejects impossible pinned configurations with loud `ConfigurationError`.
  - *Source*: `engine/shard_manager.py` (`_check_pinned_feasibility`).
  - *Test*: `tests/test_engine_advanced.py::test_impossible_pinned_configuration_fails_at_startup`.
- [x] Resource policy & thrashing prevention:
  - Soft budget threshold, recovery hysteresis, and eviction cooldown.
  - *Source*: `engine/shard_manager.py` (`ensure_loaded`, `cooldown_seconds`, `recovery_hysteresis_bytes`).
  - *Test*: `tests/test_engine_advanced.py::test_cooldown_prevents_rapid_thrashing`.
- [x] Bounded query queue & deadline enforcement:
  - Queues requests under concurrency, rejects requests exceeding max queue depth, enforces deadlines.
  - *Source*: `engine/shard_manager.py` (`query_shards`).
  - *Test*: `tests/test_engine_advanced.py::test_query_queue_bounding_and_deadlines`.
- [x] Cross-shard candidate merging & ranking:
  - Deduplicates base and local projection hits, giving precedence to `local_write` projections.
  - *Source*: `engine/shard_manager.py` (`_execute_retrieval`).
  - *Test*: `tests/test_engine_advanced.py::test_cross_shard_candidate_merging_and_ranking`.
- [x] Mutable projections:
  - Idempotent upsert/delete with version guard (`_projected_versions`) preventing stale retries from downgrading newer revisions.
  - *Source*: `engine/shard_manager.py` (`upsert_projection`, `delete_projection`).
  - *Test*: `tests/test_engine_advanced.py::test_projection_upsert_version_guard`, `test_projection_delete_is_idempotent`.
- [x] Snapshot activation with atomic swap & rollback:
  - Validates candidate staging archive, verifies SHA256 checksum, guards against path traversal, tests unpack, safely swaps active directories with rollback on error.
  - *Source*: `engine/shard_manager.py` (`activate_snapshot`).
  - *Test*: `tests/test_engine_advanced.py::test_snapshot_activation_with_real_edge_shard`, `test_snapshot_activation_checksum_mismatch_fails_safe`.
- [x] Offline network transport controller:
  - Real transport fault control injecting connection drops without touching host networking.
  - *Source*: `engine/network_control.py`.
  - *Test*: `tests/test_sync_and_server.py`, `tests/test_smoke_scenario.py`.

---

## 4. Person B: Durable Memory, Privacy & Sync (`memory/`, `sync/`, `server/`)
- [x] Transactional local store (`SQLiteMemoryStore`):
  - ACID transactions persisting operations log, current records, tombstones, outbox, conflicts, and projection queues in single commits.
  - *Source*: `memory/store.py`.
  - *Test*: `tests/test_memory_advanced.py::test_accepted_write_survives_restart`.
- [x] Durable commit precedence over projection:
  - Commits to SQLite first. If projection fails, returns `durable=True, projection_status="pending"` and enqueues for replay.
  - *Source*: `memory/service.py` (`write`).
  - *Test*: `tests/test_memory_advanced.py::test_crash_between_durable_commit_and_projection_recovers`.
- [x] Idempotency:
  - Same operation ID + same content -> cached receipt; same operation ID + different content -> `IdempotencyConflictError`.
  - *Source*: `memory/store.py` (`commit_record`).
  - *Test*: `tests/test_memory_advanced.py::test_idempotency_same_op_returns_cached_result`, `test_idempotency_conflicting_content_fails`.
- [x] Revision & conflict semantics:
  - Sequential edits increment version and supersede parents. Concurrent branch revisions are flagged as contested.
  - Explicit conflict resolution records (`resolve_conflict`) supersede competing revisions.
  - *Source*: `memory/store.py`, `memory/service.py`.
  - *Test*: `tests/test_memory_advanced.py::test_concurrent_edits_become_contested_and_explicitly_resolved`.
- [x] Public search reconciliation:
  - Excludes tombstones, filters superseded revisions, marks contested alternatives, and refills on shortfall within deadline.
  - *Source*: `memory/service.py` (`search`, `_reconcile_candidates`).
  - *Test*: `tests/test_memory_advanced.py::test_tombstoned_record_excluded_from_search`.
- [x] Urgent capacity reserve protection:
  - Routine writes cannot consume urgent reserve; urgent writes fail visibly when reserve exhausted (`StorageCapacityError`).
  - *Source*: `memory/store.py` (`commit_record`).
  - *Test*: `tests/test_memory_advanced.py::test_routine_capacity_cannot_consume_urgent_reserve`.
- [x] Privacy policy:
  - `LOCAL_ONLY` records are strictly barred from outbox entry at creation, retry, and sync.
  - *Source*: `memory/store.py` (`commit_record`), `sync/upload_worker.py`.
  - *Test*: `tests/test_memory_advanced.py::test_private_urgent_record_never_enters_outbox`.
- [x] Server ingestion (`server/`):
  - Transactional SQLite deduplication by operation ID, revision tracking, structured receipts.
  - *Source*: `server/store.py`, `server/server.py`.
  - *Test*: `tests/test_sync_and_server.py::test_server_ingest_and_deduplication`.
- [x] Upload worker (`sync/upload_worker.py`):
  - Prioritizes permitted urgent records before routine, bounded batches, backoff with jitter, retry safety.
  - *Source*: `sync/upload_worker.py`.
  - *Test*: `tests/test_sync_and_server.py::test_upload_worker_prioritizes_urgent_and_handles_outage`.
- [x] Server index preparation & export (`server/index_prep.py`):
  - Builds optimized `EdgeShard` outside edge limits, packages into `.tar` archive with manifest and SHA256 checksums.
  - *Source*: `server/index_prep.py`.
  - *Test*: `tests/test_sync_and_server.py::test_server_index_preparation_and_edge_activation`.
- [x] Snapshot download, staging, activation & reconciliation (`sync/snapshot_manager.py`):
  - Staged download, checksum validation, engine activation handoff, and reconciliation preserving newer local edits and local-only records.
  - *Source*: `sync/snapshot_manager.py`.
  - *Test*: `tests/test_sync_and_server.py::test_newer_local_edit_during_snapshot_transfer_survives`.

---

## 5. Composition, Routes & Person C Boundary (`app/`)
- [x] Standalone FastAPI application (`app/main.py`) with lifecycle hooks.
- [x] Mountable API routes (`app/routes.py`):
  - `/api/v1/records` (Create/write observations)
  - `/api/v1/search` (Authoritative public search)
  - `/api/v1/conflicts` & `/api/v1/conflicts/resolve` (Conflict review and resolution)
  - `/api/v1/detector/telemetry` & `/api/v1/detector/metrics` (Detector contention & FPS)
  - `/api/v1/sync/status` & `/api/v1/sync/trigger` (Outbox sync)
  - `/api/v1/engine/resources` (Memory and shard handles)
  - `/api/v1/dev/network` (Network fault injection)
- [x] Detector telemetry hub & synthetic contention stub (`app/telemetry.py`):
  - `DetectorTelemetryHub` and `DeterministicDetectorStub` with explicit `is_synthetic=True` labeling.

---

## 6. Seed Data & Test Fixtures (`fixtures/seed_data.py`)
- [x] Sourced procedure cards (`get_demo_procedure_cards`) with NFPA-70E / OSHA citations and non-operational drill labels.
- [x] Multi-zone history fixtures (`get_zone_history_records`) covering zone_01, zone_02, local-only scans, and permitted records.
- [x] Deterministic concurrent-edit conflict fixtures (`get_deterministic_conflict_fixture`).

---

## 7. Verification & Benchmarks (`tests/`, `benchmark/`)
- [x] 30 unit and integration tests passing:
  - `tests/test_monitor.py` (1 test)
  - `tests/test_qdrant_adapter_integration.py` (1 test)
  - `tests/test_shard_manager.py` (4 tests)
  - `tests/test_engine_advanced.py` (10 tests)
  - `tests/test_memory_advanced.py` (9 tests)
  - `tests/test_sync_and_server.py` (4 tests)
  - `tests/test_smoke_scenario.py` (1 test: full 15-step scenario)
- [x] Reproducible benchmark runner (`benchmark/runner.py`):
  - Evaluates Naive, Unmanaged on-disk, and Managed (`ShardManager`) modes.
  - Measures detector FPS, search P95 latency, queue P95 time, recall, coverage, and peak RSS.
  - Outputs structured results to `benchmark/results.csv` and `benchmark/results.json`.
