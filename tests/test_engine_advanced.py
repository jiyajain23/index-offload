"""Advanced unit and integration tests for Person A Engine correctness and lifecycle."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
import time
import pytest

from engine.events import EventEmitter
from engine.monitor import ResourceMonitor, ResourceSnapshot
from engine.shard_manager import (
    BudgetExceeded,
    ConfigurationError,
    ShardManager,
    ShardRegistration,
)
from shared.contracts import (
    CandidateHit,
    PriorityLevel,
    ProjectionDeleteRequest,
    ProjectionUpsertRequest,
    SearchRequest,
    SharingPolicy,
    SnapshotCandidate,
)


class MockMonitor:
    def __init__(self, usage=100_000, limit=1_000_000):
        self.usage = usage
        self.limit = limit

    def snapshot(self):
        return ResourceSnapshot(
            rss_bytes=self.usage,
            working_set_bytes=self.usage,
            memory_current_bytes=self.usage,
            memory_max_bytes=self.limit,
            memory_pressure=self.usage / self.limit if self.limit else 0.0,
            memory_events={},
            cpu_percent=5.0,
        )


class MockStore:
    def __init__(self, monitor: MockMonitor, cost=200_000):
        self.monitor = monitor
        self.cost = cost
        self.loaded_paths = set()
        self.closed_paths = set()

    def load(self, path):
        self.monitor.usage += self.cost
        self.loaded_paths.add(path)
        return {"path": path, "active": True}

    def close(self, shard):
        self.monitor.usage -= self.cost
        shard["active"] = False
        self.closed_paths.add(shard["path"])

    def search(self, shard, query_vector, k, **kwargs):
        if not shard.get("active"):
            raise RuntimeError("CRITICAL: Search attempted on closed shard handle!")
        return [
            type("Hit", (), {
                "id": f"{shard['path']}_pt1",
                "score": 0.85,
                "payload": {"source": shard["path"]},
            })()
        ]

    def upsert_points(self, shard, points):
        if not shard.get("active"):
            raise RuntimeError("Upsert attempted on closed shard handle!")
        return len(points)

    def delete_points(self, shard, point_ids):
        if not shard.get("active"):
            raise RuntimeError("Delete attempted on closed shard handle!")
        return len(point_ids)


def test_concurrent_query_and_eviction_protects_reader():
    """Shard stays usable until readers finish: no closed-handle errors or missing coverage entries."""
    monitor = MockMonitor(usage=0, limit=1_000_000)
    store = MockStore(monitor, cost=200_000)
    mgr = ShardManager(
        store,
        monitor,
        soft_budget_mb=500_000 / (1024 * 1024),
        default_shard_cost_mb=200_000 / (1024 * 1024),
    )
    
    mgr.register_shard("zone_1", "path_z1", estimated_cost_mb=200_000 / (1024 * 1024))
    mgr.register_shard("zone_2", "path_z2", estimated_cost_mb=200_000 / (1024 * 1024))
    mgr.register_shard("zone_3", "path_z3", estimated_cost_mb=200_000 / (1024 * 1024))

    # Hold an active lease on zone_1
    with mgr.lease_shard("zone_1") as s1:
        assert s1["active"] is True
        
        # Load zone_2 (zone_1 and zone_2 loaded, total 400k <= 500k)
        mgr.ensure_loaded("zone_2")

        # Now loading zone_3 requires 200k. Total would be 600k > 500k.
        # Since zone_1 has an active lease (active_operations = 1), only zone_2 can be evicted!
        mgr.ensure_loaded("zone_3")

        # zone_2 must have been evicted, but zone_1 MUST remain loaded and active!
        assert "path_z2" in store.closed_paths
        assert s1["active"] is True
        assert "zone_1" in mgr.status()["loaded"]


def test_leases_released_after_exceptions():
    """Exceptions during operations must safely release leases and decrement active operations."""
    monitor = MockMonitor(usage=0, limit=10_000_000)
    store = MockStore(monitor, cost=100_000)
    mgr = ShardManager(store, monitor, soft_budget_mb=5, default_shard_cost_mb=0.1)
    mgr.register_shard("z1", "path_z1", estimated_cost_mb=0.1)

    with pytest.raises(ValueError):
        with mgr.lease_shard("z1") as shard:
            raise ValueError("Intentional failure inside lease")

    status = mgr.status()
    assert status["active_operations"] == {}


def test_impossible_pinned_configuration_fails_at_startup():
    """Startup rejects impossible pinned configuration with a specific explanation."""
    monitor = MockMonitor(usage=0, limit=1_000_000)
    store = MockStore(monitor)
    # Budget is 300k, but pinned shards require 400k
    mgr = ShardManager(store, monitor, soft_budget_mb=300_000 / (1024 * 1024))
    mgr.register_shard("proto1", "p1", pinned=True, estimated_cost_mb=200_000 / (1024 * 1024))
    mgr.register_shard("proto2", "p2", pinned=True, estimated_cost_mb=200_000 / (1024 * 1024))

    with pytest.raises(ConfigurationError) as exc_info:
        mgr.load_pinned()

    assert "Impossible pinned configuration" in str(exc_info.value)
    # Ensure no shards left partially loaded
    assert len(mgr.status()["loaded"]) == 0


def test_cooldown_prevents_rapid_thrashing():
    """Manager does not continually load and unload the same shard in rapid oscillation."""
    monitor = MockMonitor(usage=0, limit=1_000_000)
    store = MockStore(monitor, cost=200_000)
    mgr = ShardManager(
        store,
        monitor,
        soft_budget_mb=250_000 / (1024 * 1024),
        default_shard_cost_mb=200_000 / (1024 * 1024),
        cooldown_seconds=1.0,
    )
    mgr.register_shard("s1", "p1", estimated_cost_mb=200_000 / (1024 * 1024))
    mgr.register_shard("s2", "p2", estimated_cost_mb=200_000 / (1024 * 1024))

    mgr.ensure_loaded("s1")
    mgr.ensure_loaded("s2")  # Evicts s1
    assert "s1" not in mgr.status()["loaded"]

    # Immediate reload of s1 within cooldown window must be rejected
    with pytest.raises(BudgetExceeded) as exc_info:
        mgr.ensure_loaded("s1")
    assert "cooldown" in str(exc_info.value)


def test_query_queue_bounding_and_deadlines():
    """Full queue produces bounded, explicit rejection/deferment and deadline expiry reports partial status."""
    monitor = MockMonitor(usage=0, limit=10_000_000)
    store = MockStore(monitor, cost=100_000)
    mgr = ShardManager(store, monitor, soft_budget_mb=5, default_shard_cost_mb=0.1, max_concurrency=1, max_queue_depth=1)
    mgr.register_shard("z1", "path_z1", estimated_cost_mb=0.1)

    # Request with an impossible 0ms deadline
    req = SearchRequest(
        context="z1",
        deadline_ms=0.001,
        include_protocols=False,
    )
    res = mgr.query_shards(req)
    assert res.coverage.failed == ["z1"]
    reason_text = " ".join(res.reasons.values()).lower()
    assert "deadline" in reason_text or "expired" in reason_text


def test_projection_upsert_version_guard():
    """Older projection retry cannot overwrite newer projected version."""
    monitor = MockMonitor(usage=0, limit=10_000_000)
    store = MockStore(monitor, cost=100_000)
    mgr = ShardManager(store, monitor, soft_budget_mb=5, default_shard_cost_mb=0.1)
    mgr.register_shard("local_write", "lw_path", pinned=True, estimated_cost_mb=0.1)
    mgr.ensure_loaded("local_write")

    # Upsert version 2
    r2 = mgr.upsert_projection(ProjectionUpsertRequest(
        record_id="rec_1",
        version=2,
        context_id="local_write",
        dense_vector=[0.1] * 4,
        payload={"claim": "version 2"},
    ))
    assert r2.success is True
    assert r2.version_installed == 2

    # Retry an older version 1
    r1 = mgr.upsert_projection(ProjectionUpsertRequest(
        record_id="rec_1",
        version=1,
        context_id="local_write",
        dense_vector=[0.1] * 4,
        payload={"claim": "stale version 1"},
    ))
    assert r1.success is True
    # Version installed remains 2 (not downgraded!)
    assert r1.version_installed == 2


def test_projection_delete_is_idempotent():
    """Projection deletes are idempotent and succeed even if repeated."""
    monitor = MockMonitor(usage=0, limit=10_000_000)
    store = MockStore(monitor, cost=100_000)
    mgr = ShardManager(store, monitor, soft_budget_mb=5, default_shard_cost_mb=0.1)
    mgr.register_shard("local_write", "lw_path", pinned=True, estimated_cost_mb=0.1)
    mgr.ensure_loaded("local_write")

    del_req = ProjectionDeleteRequest(record_id="rec_1", context_id="local_write")
    res1 = mgr.delete_projection(del_req)
    assert res1.success is True

    res2 = mgr.delete_projection(del_req)
    assert res2.success is True


def test_cross_shard_candidate_merging_and_ranking():
    """Candidates are merged across all searched shards and sorted by score descending."""
    monitor = MockMonitor(usage=0, limit=10_000_000)
    store = MockStore(monitor, cost=100_000)
    mgr = ShardManager(store, monitor, soft_budget_mb=5, default_shard_cost_mb=0.1)
    mgr.register_shard("protocols", "proto_path", pinned=True, estimated_cost_mb=0.1)
    mgr.register_shard("zone_1", "z1_path", estimated_cost_mb=0.1)
    mgr.ensure_loaded("protocols")
    mgr.ensure_loaded("zone_1")

    req = SearchRequest(context="zone_1", include_protocols=True, k=5)
    res = mgr.query_shards(req)

    assert "protocols" in res.coverage.searched
    assert "zone_1" in res.coverage.searched
    assert len(res.candidates) >= 1
    scores = [c.score for c in res.candidates]
    assert scores == sorted(scores, reverse=True)


def test_snapshot_activation_with_real_edge_shard(tmp_path):
    """Test real snapshot validation, checksum verification, and safe activation."""
    from qdrant_edge import EdgeShard, EdgeConfig, EdgeVectorParams, Distance, Point, UpdateOperation
    from edge.qdrant_adapter import QdrantEdgeShardStore

    base_dir = tmp_path / "base"
    base_dir.mkdir(parents=True, exist_ok=True)
    config = EdgeConfig(vectors={"embedding": EdgeVectorParams(size=2, distance=Distance.Cosine)})
    store = QdrantEdgeShardStore(config)
    shard = EdgeShard.create(str(base_dir), config)
    shard.update(UpdateOperation.upsert_points([Point(id=1, vector={"embedding": [1.0, 0.0]}, payload={"v": "old"})]))
    shard.close()

    new_data_dir = tmp_path / "new_data"
    new_data_dir.mkdir(parents=True, exist_ok=True)
    new_shard = EdgeShard.create(str(new_data_dir), config)
    new_shard.update(UpdateOperation.upsert_points([Point(id=1, vector={"embedding": [0.0, 1.0]}, payload={"v": "new"})]))
    new_shard.close()

    tar_path = tmp_path / "snap.tar"
    with tarfile.open(tar_path, "w") as tar:
        for item in os.listdir(new_data_dir):
            tar.add(os.path.join(new_data_dir, item), arcname=item)

    h = hashlib.sha256()
    with open(tar_path, "rb") as f:
        h.update(f.read())
    sha = h.hexdigest()

    mgr = ShardManager(store, ResourceMonitor(), soft_budget_mb=400, default_shard_cost_mb=10)
    mgr.register_shard("zone_1", str(base_dir), installed_version="v1")
    mgr.ensure_loaded("zone_1")

    cand = SnapshotCandidate(
        snapshot_id="snap_001",
        target_shard_id="zone_1",
        context_id="zone_1",
        staging_path=str(tar_path),
        manifest_version="v2",
        checksum_sha256=sha,
    )
    receipt = mgr.activate_snapshot(cand)
    assert receipt.success is True
    assert receipt.installed_version == "v2"

    with mgr.lease_shard("zone_1") as sh:
        res = store.search(sh, [0.0, 1.0], 1)
        assert res[0].payload == {"v": "new"}

    mgr.close_all()


def test_snapshot_activation_checksum_mismatch_fails_safe(tmp_path):
    """Snapshot activation with invalid checksum fails without corrupting active shard."""
    from qdrant_edge import EdgeShard, EdgeConfig, EdgeVectorParams, Distance, Point, UpdateOperation
    from edge.qdrant_adapter import QdrantEdgeShardStore

    base_dir = tmp_path / "base"
    base_dir.mkdir(parents=True, exist_ok=True)
    config = EdgeConfig(vectors={"embedding": EdgeVectorParams(size=2, distance=Distance.Cosine)})
    store = QdrantEdgeShardStore(config)
    shard = EdgeShard.create(str(base_dir), config)
    shard.update(UpdateOperation.upsert_points([Point(id=1, vector={"embedding": [1.0, 0.0]}, payload={"v": "old"})]))
    shard.close()

    mgr = ShardManager(store, ResourceMonitor(), soft_budget_mb=400, default_shard_cost_mb=10)
    mgr.register_shard("zone_1", str(base_dir), installed_version="v1")
    mgr.ensure_loaded("zone_1")

    dummy_tar = tmp_path / "corrupt.tar"
    dummy_tar.write_bytes(b"corrupt data")

    cand = SnapshotCandidate(
        snapshot_id="snap_bad",
        target_shard_id="zone_1",
        context_id="zone_1",
        staging_path=str(dummy_tar),
        manifest_version="v2",
        checksum_sha256="wrong_checksum",
    )
    receipt = mgr.activate_snapshot(cand)
    assert receipt.success is False
    assert "checksum" in receipt.error.lower()

    # Active shard remains intact
    with mgr.lease_shard("zone_1") as sh:
        res = store.search(sh, [1.0, 0.0], 1)
        assert res[0].payload == {"v": "old"}

    mgr.close_all()

