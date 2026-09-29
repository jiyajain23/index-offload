"""Failures discovered by reviewing the first A+B implementation."""

import threading
import hashlib
import tarfile

from engine.monitor import ResourceSnapshot
from engine.shard_manager import ShardManager
from memory.store import SQLiteMemoryStore
from shared.contracts import ProjectionUpsertRequest


class Monitor:
    def snapshot(self):
        return ResourceSnapshot(0, 0, 0, 1024 * 1024 * 1024, None, {}, 0.0)


def test_concurrent_revisions_have_distinct_vector_points(tmp_path):
    from qdrant_edge import Distance, EdgeConfig, EdgeVectorParams, Query, QueryRequest
    from edge.qdrant_adapter import QdrantEdgeShardStore

    config = EdgeConfig(vectors={"embedding": EdgeVectorParams(size=2, distance=Distance.Cosine)})
    adapter = QdrantEdgeShardStore(config)
    shard_path = tmp_path / "local"
    adapter.close(adapter.create(shard_path))
    engine = ShardManager(adapter, Monitor(), soft_budget_mb=512)
    engine.register_shard("local_write", str(shard_path), pinned=True)

    for operation_id, vector in (("op_open", [1.0, 0.0]), ("op_closed", [0.0, 1.0])):
        receipt = engine.upsert_projection(ProjectionUpsertRequest(
            record_id="access", operation_id=operation_id, context_id="zone", version=2,
            dense_vector=vector,
        ))
        assert receipt.success, receipt.error

    with engine.lease_shard("local_write") as shard:
        hits = shard.query(QueryRequest(query=Query.Nearest([1.0, 0.0], using="embedding"),
                                        limit=10, with_payload=True))
        assert {hit.payload["operation_id"] for hit in hits} == {"op_open", "op_closed"}
    engine.close_all()


def test_pending_projection_preserves_same_version_conflict(tmp_path):
    from shared.contracts import RecordEnvelope

    store = SQLiteMemoryStore(tmp_path / "memory.db")
    for op, parent in (("parent", None), ("open", 1), ("closed", 1)):
        store.commit_record(RecordEnvelope(
            record_id="access", operation_id=op, entity_id="door", context_id="zone",
            device_id="edge", version=1 if parent is None else 2,
            parent_version=parent, observation={"state": op},
        ))
    pending = store.get_pending_projections()
    assert {item["operation_id"] for item in pending} == {"parent", "open", "closed"}
    store.mark_projection_completed("access", 2, "open")
    assert {item["operation_id"] for item in store.get_pending_projections()} == {"parent", "closed"}


def test_search_exposes_both_contested_observations(tmp_path):
    from memory.service import MemoryService
    from shared.contracts import (CandidateHit, CoverageSummary, EngineSearchResult,
                                  RecordEnvelope, SearchRequest)

    store = SQLiteMemoryStore(tmp_path / "memory.db")
    for operation, version, parent in (("parent", 1, None), ("open", 2, 1),
                                        ("closed", 2, 1)):
        store.commit_record(RecordEnvelope(
            record_id="access", operation_id=operation, entity_id="door",
            context_id="zone", device_id="edge", version=version,
            parent_version=parent, observation={"state": operation},
        ))

    class Engine:
        def query_shards(self, request):
            return EngineSearchResult(candidates=[
                CandidateHit(point_id=op, shard_id="local_write", score=score,
                             payload={"record_id": "access", "version": 2,
                                      "operation_id": op})
                for op, score in (("open", .9), ("closed", .8))
            ], coverage=CoverageSummary(searched=["local_write"]))

    result = MemoryService(store, Engine()).search(
        SearchRequest(context="zone", dense_vector=[1.0, 0.0], k=2))
    assert {hit.observation["state"] for hit in result.hits} == {"open", "closed"}
    assert all(hit.is_contested for hit in result.hits)


def test_shutdown_keeps_handle_open_until_reader_finishes():
    class Store:
        def load(self, path):
            return object()
        def close(self, shard):
            self.closed = True

    store = Store()
    store.closed = False
    engine = ShardManager(store, Monitor(), soft_budget_mb=512)
    engine.register_shard("zone", "zone")
    entered = threading.Event()
    release = threading.Event()

    def reader():
        with engine.lease_shard("zone"):
            entered.set()
            release.wait(timeout=10)

    thread = threading.Thread(target=reader)
    thread.start()
    assert entered.wait(2)
    try:
        try:
            engine.close_all()
        except TimeoutError:
            pass
        else:
            raise AssertionError("Shutdown closed an active handle")
        assert not store.closed
    finally:
        release.set()
        thread.join(timeout=2)
    engine.close_all()
    assert store.closed


def test_public_search_enforces_requested_filters(tmp_path):
    from memory.service import MemoryService
    from shared.contracts import (CandidateHit, CoverageSummary, EngineSearchResult,
                                  SearchFilter, SearchRequest)

    class Engine:
        def query_shards(self, request):
            return EngineSearchResult(candidates=[
                CandidateHit(point_id="rec_1", shard_id="zone", score=.9,
                             payload={"record_id": "rec_1", "context_id": "zone",
                                      "entity_id": "door", "observation": {"state": "open"}}),
                CandidateHit(point_id="rec_2", shard_id="zone", score=.8,
                             payload={"record_id": "rec_2", "context_id": "zone",
                                      "entity_id": "valve", "observation": {"state": "closed"}}),
            ], coverage=CoverageSummary(searched=["zone"]))

    result = MemoryService(SQLiteMemoryStore(tmp_path / "store.db"), Engine()).search(
        SearchRequest(context="zone", dense_vector=[1.0, 0.0], k=1,
                      filters=SearchFilter(must={"observation.state": "closed"})))
    assert [hit.record_id for hit in result.hits] == ["rec_2"]


def test_activation_recovery_uses_durable_receipt(tmp_path):
    from qdrant_edge import Distance, EdgeConfig, EdgeVectorParams, Point, UpdateOperation
    from edge.qdrant_adapter import QdrantEdgeShardStore
    from shared.contracts import SnapshotCandidate

    config = EdgeConfig(vectors={"embedding": EdgeVectorParams(size=2, distance=Distance.Cosine)})
    adapter = QdrantEdgeShardStore(config)
    base_path, new_path = tmp_path / "base", tmp_path / "new"
    for path, label in ((base_path, "old"), (new_path, "new")):
        shard = adapter.create(path)
        shard.update(UpdateOperation.upsert_points([
            Point(id=1, vector={"embedding": [1.0, 0.0]}, payload={"label": label})]))
        shard.close()
    archive = tmp_path / "snapshot.tar"
    with tarfile.open(archive, "w") as tar:
        for path in new_path.iterdir():
            tar.add(path, arcname=path.name)
    candidate = SnapshotCandidate(snapshot_id="new_snapshot", target_shard_id="zone",
                                  context_id="zone", staging_path=str(archive),
                                  manifest_version="v2",
                                  checksum_sha256=hashlib.sha256(archive.read_bytes()).hexdigest())

    engine = ShardManager(adapter, Monitor(), soft_budget_mb=512)
    engine.register_shard("zone", str(base_path))
    assert engine.activate_snapshot(candidate).success
    engine.close_all()

    # Simulate process death after the directory switch but before B's commit.
    restarted = ShardManager(adapter, Monitor(), soft_budget_mb=512,
                             snapshot_is_installed=lambda snapshot_id: False)
    restarted.register_shard("zone", str(base_path))
    with restarted.lease_shard("zone") as shard:
        from qdrant_edge import Query, QueryRequest
        hits = shard.query(QueryRequest(query=Query.Nearest([1.0, 0.0], using="embedding"),
                                        limit=1, with_payload=True))
        assert hits[0].payload["label"] == "old"
    restarted.close_all()
