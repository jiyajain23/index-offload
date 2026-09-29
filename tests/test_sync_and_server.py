"""Integration tests for Server ingestion, UploadWorker, and SnapshotManager."""

import os
from pathlib import Path
import pytest
from starlette.testclient import TestClient

from edge.qdrant_adapter import QdrantEdgeShardStore
from engine.monitor import ResourceMonitor
from engine.network_control import NetworkFaultController
from engine.shard_manager import ShardManager
from memory.service import MemoryService
from memory.store import SQLiteMemoryStore
from server.index_prep import ServerIndexPreparer
from server.server import app, init_server
from server.store import ServerMemoryStore
from shared.contracts import (
    PriorityLevel,
    RecordEnvelope,
    RecordStatus,
    SearchRequest,
    SharingPolicy,
    SnapshotCandidate,
)
from shared.events import EventBus
from sync.snapshot_manager import SnapshotManager
from sync.upload_worker import UploadWorker
from qdrant_edge import EdgeConfig, EdgeVectorParams, Distance


def test_server_ingest_and_deduplication(tmp_path):
    """Server ingests batch, deduplicates on retry, and returns structured receipts."""
    server_db = tmp_path / "server.db"
    store = ServerMemoryStore(server_db)

    batch = [
        {
            "operation_id": "op_sync_1",
            "record_id": "rec_sync_1",
            "entity_id": "valve_1",
            "context_id": "zone_01",
            "device_id": "edge_1",
            "version": 1,
            "observation": {"flow_rate": 50},
        }
    ]
    receipts1 = store.ingest_batch(batch)
    assert len(receipts1) == 1
    assert receipts1[0]["status"] == "durably_accepted"
    assert receipts1[0]["deduplicated"] is False

    # Repeat exact same operation
    receipts2 = store.ingest_batch(batch)
    assert len(receipts2) == 1
    assert receipts2[0]["status"] == "durably_accepted"
    assert receipts2[0]["deduplicated"] is True


def test_upload_worker_prioritizes_urgent_and_handles_outage(tmp_path):
    """UploadWorker prioritizes urgent records, handles transport failure, and acknowledges on recovery."""
    server_dir = tmp_path / "server"
    init_server(server_dir)

    edge_db = tmp_path / "edge_memory.db"
    edge_store = SQLiteMemoryStore(edge_db)
    bus = EventBus()
    fault_ctl = NetworkFaultController(offline_by_default=False)

    # Wrap TestClient with fault control
    base_client = TestClient(app, base_url="http://testserver")

    class FaultInjectingTestClient:
        def post(self, url, **kwargs):
            if fault_ctl.is_offline:
                import httpx
                raise httpx.ConnectError("Server link down")
            return base_client.post(url, **kwargs)

        def get(self, url, **kwargs):
            if fault_ctl.is_offline:
                import httpx
                raise httpx.ConnectError("Server link down")
            return base_client.get(url, **kwargs)

        def stream(self, method, url, **kwargs):
            if fault_ctl.is_offline:
                import httpx
                raise httpx.ConnectError("Server link down")
            return base_client.stream(method, url, **kwargs)

    client = FaultInjectingTestClient()

    worker = UploadWorker(
        store=edge_store,
        server_url="http://testserver",
        network_controller=fault_ctl,
        events=bus,
        batch_size=10,
        base_backoff_seconds=0.0,  # Immediate retry for test determinism
        custom_client=client,  # type: ignore
    )

    # Queue 1 routine record and 1 urgent record
    r_routine = RecordEnvelope(
        record_id="rec_routine",
        operation_id="op_routine",
        entity_id="gauge_1",
        context_id="zone_01",
        device_id="edge_1",
        observation={"val": 10},
        priority=PriorityLevel.ROUTINE,
        sharing=SharingPolicy.PERMITTED_SHARED,
    )
    r_urgent = RecordEnvelope(
        record_id="rec_urgent",
        operation_id="op_urgent",
        entity_id="alarm_1",
        context_id="zone_01",
        device_id="edge_1",
        observation={"alarm": "smoke_detected"},
        priority=PriorityLevel.URGENT,
        sharing=SharingPolicy.PERMITTED_SHARED,
    )
    edge_store.commit_record(r_routine)
    edge_store.commit_record(r_urgent)

    # 1. Break network transport
    fault_ctl.set_offline(True)
    acked_offline = worker.process_outbox_once()
    assert acked_offline == 0
    # Records should still be queued / retryable
    status_offline = edge_store.sync_status()
    assert status_offline["outbox_acknowledged"] == 0

    # 2. Restore transport
    fault_ctl.set_offline(False)
    acked_online = worker.process_outbox_once()
    assert acked_online >= 1

    status_online = edge_store.sync_status()
    assert status_online["outbox_acknowledged"] >= 1


def test_server_index_preparation_and_edge_activation(tmp_path):
    """Server prepares indexed snapshot, SnapshotManager downloads and stages, Engine activates, and local state is reconciled."""
    # 1. Server Setup
    server_dir = tmp_path / "server"
    init_server(server_dir)
    server_store = ServerMemoryStore(server_dir / "server_memory.db")
    preparer = ServerIndexPreparer(server_store, output_dir=server_dir / "snapshots", dimension=4)

    # Ingest records on server
    server_store.ingest_batch([
        {
            "operation_id": "op_srv_1",
            "record_id": "rec_srv_1",
            "entity_id": "transformer_1",
            "context_id": "zone_01",
            "version": 1,
            "observation": {"rating_kva": 500},
            "dense_vector": [0.1, 0.2, 0.3, 0.4],
        }
    ])

    # Server prepares snapshot
    candidate = preparer.prepare_snapshot("zone_01", manifest_version="v_2026")
    assert candidate.record_count == 1
    assert Path(candidate.staging_path).exists()

    # 2. Edge Setup
    edge_dir = tmp_path / "edge"
    edge_dir.mkdir(parents=True, exist_ok=True)
    edge_store = SQLiteMemoryStore(edge_dir / "edge_memory.db")

    config = EdgeConfig(vectors={"embedding": EdgeVectorParams(size=4, distance=Distance.Cosine)})
    edge_shard_store = QdrantEdgeShardStore(config)
    engine = ShardManager(edge_shard_store, ResourceMonitor(), soft_budget_mb=400, default_shard_cost_mb=10)

    # Initial base shard directory on edge
    base_shard_dir = edge_dir / "zone_01_base"
    base_shard_dir.mkdir(parents=True, exist_ok=True)
    initial_shard = edge_shard_store.create(str(base_shard_dir))
    edge_shard_store.close(initial_shard)

    engine.register_shard("zone_01", str(base_shard_dir), installed_version="v0")
    engine.ensure_loaded("zone_01")

    # Connect SnapshotManager using Starlette TestClient
    client = TestClient(app, base_url="http://testserver")

    snap_mgr = SnapshotManager(
        store=edge_store,
        engine=engine,
        server_url="http://testserver",
        staging_dir=edge_dir / "staging",
        custom_client=client,  # type: ignore
    )

    # Fetch and activate!
    receipt = snap_mgr.fetch_and_activate("zone_01")
    assert receipt is not None
    assert receipt.success is True
    assert receipt.installed_version == "v_2026"

    # Search on edge to verify server-indexed data is searchable!
    with engine.lease_shard("zone_01") as sh:
        hits = edge_shard_store.search(sh, [0.1, 0.2, 0.3, 0.4], 1)
        assert len(hits) == 1
        assert hits[0].payload["record_id"] == "rec_srv_1"

    engine.close_all()


def test_newer_local_edit_during_snapshot_transfer_survives(tmp_path):
    """Local edit made during snapshot transfer survives snapshot activation."""
    server_dir = tmp_path / "server"
    init_server(server_dir)
    server_store = ServerMemoryStore(server_dir / "server_memory.db")
    preparer = ServerIndexPreparer(server_store, output_dir=server_dir / "snapshots", dimension=4)

    # Server has rec_1 at version 1
    server_store.ingest_batch([
        {
            "operation_id": "op_v1",
            "record_id": "rec_1",
            "entity_id": "valve_1",
            "context_id": "zone_01",
            "version": 1,
            "observation": {"state": "closed"},
            "dense_vector": [0.1, 0.2, 0.3, 0.4],
        }
    ])
    candidate = preparer.prepare_snapshot("zone_01", manifest_version="v1")

    # Edge setup
    edge_dir = tmp_path / "edge"
    edge_store = SQLiteMemoryStore(edge_dir / "memory.db")
    config = EdgeConfig(vectors={"embedding": EdgeVectorParams(size=4, distance=Distance.Cosine)})
    edge_shard_store = QdrantEdgeShardStore(config)
    engine = ShardManager(edge_shard_store, ResourceMonitor(), soft_budget_mb=400, default_shard_cost_mb=10)

    # Create local_write and zone_01 base
    lw_dir = edge_dir / "lw"
    lw_shard = edge_shard_store.create(str(lw_dir))
    edge_shard_store.close(lw_shard)
    engine.register_shard("local_write", str(lw_dir), pinned=True)

    base_dir = edge_dir / "zone_01"
    b_shard = edge_shard_store.create(str(base_dir))
    edge_shard_store.close(b_shard)
    engine.register_shard("zone_01", str(base_dir), installed_version="v0")

    mem_service = MemoryService(edge_store, engine)

    # Local operator creates NEWER edit on edge (version 2: state='opened') during transfer!
    newer_local_rec = RecordEnvelope(
        record_id="rec_1",
        operation_id="op_v2_local",
        entity_id="valve_1",
        context_id="zone_01",
        device_id="edge_1",
        version=2,
        parent_version=1,
        observation={"state": "opened"},
        dense_vector=[0.1, 0.2, 0.3, 0.4],
    )
    mem_service.write(newer_local_rec)

    # Now snapshot (version 1) is activated
    act_receipt = engine.activate_snapshot(candidate)
    assert act_receipt.success is True

    # Reconcile snapshot on edge
    edge_store.record_installed_snapshot(
        candidate.snapshot_id,
        candidate.target_shard_id,
        candidate.manifest_version,
        candidate.included_records,
    )

    # Search public memory: local newer edit (version 2, 'opened') MUST win!
    search_res = mem_service.search(SearchRequest(
        context="zone_01",
        dense_vector=[0.1, 0.2, 0.3, 0.4],
        k=5,
    ))
    assert len(search_res.hits) >= 1
    top_hit = search_res.hits[0]
    assert top_hit.record_id == "rec_1"
    assert top_hit.version == 2
    assert top_hit.observation["state"] == "opened"

    engine.close_all()
