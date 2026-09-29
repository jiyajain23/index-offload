# LIFELINE implementation status

This file tracks the revised ZIP. It does not imply A+B are complete merely because the in-process tests pass.

| Gate | Status | Evidence / remaining work |
| --- | --- | --- |
| Durable local acceptance and outbox | Implemented, locally tested | SQLite transactions and retry tests; capacity estimate lacks WAL/free-space guard. |
| Conflict-safe vector identity | Implemented, locally tested | Immutable operation IDs in projections; regression test with real Qdrant Edge. |
| Search reconciliation | Partial | Tombstones, current revision and supported payload filters; global hybrid ranking and exhaustive refill need validation. |
| Shard lifecycle | Partial | Reader leases and safe timeout behavior; CPU contention and external pressure response need a live bounded run. |
| Snapshot activation | Partial | Checksum, bounded legacy extraction, journaling/rollback test; actual Qdrant Server snapshot compatibility and process-kill tests pending. |
| Privacy | Partial | Local-only records excluded from outbox and upload; permission revocation of already queued records pending. |
| Upload/server service | Partial | SQLite ingestion and in-process outbox tests; Qdrant Server projection/export code requires live verification. |
| Actual transport outage | Pending | Docker network topology supplied; interrupt an in-flight transfer and verify reconnect on a Docker host. |
| Benchmark | Exploratory only | Historical synthetic run uses an unfair open/close baseline, one process, and no OS-enforced cap. |
| Person C demo | Implemented, locally tested | Synthetic fixed AVI, OpenCV red-beacon detector, bounded frame queue, local hash embeddings, durable/searchable local-only record, dashboard. General detector and GPU workload remain pending. |

## Verification carried out in the editing environment

- Real Qdrant Edge was installed and exercised in the regression suite.
- 36 tests passed with the monitor scoped to the test process, including the existing in-process smoke test. This is not a cgroup-limited edge/device run.
- Built the dashboard and exercised the synthetic video observation and retrieval through `TestClient`, including a separate-process restart.
- Docker, Qdrant Server, and a GPU detector were unavailable for end-to-end verification.

## Next release gates

1. Start the three Docker services, ingest permitted records, prepare a snapshot, and load it via `EdgeShard.unpack_snapshot` on the edge. Record exact Server/Edge versions and failures.
2. Disconnect the `lifeline_sync` network during an actual transfer; prove local write/search and retry after reconnect.
3. Add a permission-revocation action and recheck policy immediately before serialization; define behavior for already acknowledged records.
4. Test a terminated process at snapshot switch/receipt boundaries, plus startup reconciliation with a durable receipt.
5. Replace the limited red-beacon demo with a reviewed target detector and embedding model if the project requires real industrial anomaly recognition. Verify the five-act scenario on the intended device.
6. Rerun the benchmark under OS-enforced limits with a fair tuned baseline, fixed data/query/video, independent processes, repetitions, and latency/recall/coverage reports.
