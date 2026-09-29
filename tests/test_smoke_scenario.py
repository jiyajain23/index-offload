"""End-to-end 15-step smoke scenario verifying complete A+B lifecycle, persistence, and privacy."""

import os
from pathlib import Path
import pytest
from starlette.testclient import TestClient

from edge.qdrant_adapter import QdrantEdgeShardStore
from engine.monitor import ResourceMonitor
from engine.network_control import NetworkFaultController
from engine.shard_manager import ShardManager
from fixtures.seed_data import (
    get_demo_procedure_cards,
    get_deterministic_conflict_fixture,
    get_zone_history_records,
)
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
)
from shared.events import EventBus
from sync.snapshot_manager import SnapshotManager
from sync.upload_worker import UploadWorker
from qdrant_edge import EdgeConfig, EdgeVectorParams, Distance


def test_full_fifteen_step_smoke_scenario(tmp_path):
    """Execute the exact 15-step integration sequence specified in the LIFELINE requirements."""
    dim = 384
    captured_outbound_payloads = []

    # =========================================================================
    # Step 1: Start Edge Backend and Server
    # =========================================================================
    server_dir = tmp_path / "server"
    init_server(server_dir)
    server_store = ServerMemoryStore(server_dir / "server_memory.db")
    server_prep = ServerIndexPreparer(server_store, output_dir=server_dir / "snapshots", dimension=dim)

    edge_dir = tmp_path / "edge"
    edge_dir.mkdir(parents=True, exist_ok=True)
    edge_store = SQLiteMemoryStore(edge_dir / "edge_memory.db")
    bus = EventBus()
    fault_ctl = NetworkFaultController(offline_by_default=False)

    edge_config = EdgeConfig(vectors={"embedding": EdgeVectorParams(size=dim, distance=Distance.Cosine)})
    edge_shard_store = QdrantEdgeShardStore(edge_config)
    engine = ShardManager(edge_shard_store, ResourceMonitor(), soft_budget_mb=400, default_shard_cost_mb=10)

    # Initialize dedicated non-evictable local_write shard
    lw_path = edge_dir / "local_write"
    lw_shard = edge_shard_store.create(str(lw_path))
    edge_shard_store.close(lw_shard)
    engine.register_shard("local_write", str(lw_path), pinned=True)
    engine.ensure_loaded("local_write")

    # Initial base shard directory for zone_01
    zone01_base_dir = edge_dir / "zone_01"
    z_shard = edge_shard_store.create(str(zone01_base_dir))
    edge_shard_store.close(z_shard)
    engine.register_shard("zone_01", str(zone01_base_dir), installed_version="v0")
    engine.ensure_loaded("zone_01")

    # Wire HTTP transport with packet capture & fault control
    base_client = TestClient(app, base_url="http://testserver")

    class CapturingFaultClient:
        def post(self, url, **kwargs):
            if fault_ctl.is_offline:
                with fault_ctl._lock:
                    fault_ctl._dropped_count += 1
                import httpx
                raise httpx.ConnectError("LIFELINE link offline: fault injected")
            if "json" in kwargs:
                captured_outbound_payloads.append(kwargs["json"])
            return base_client.post(url, **kwargs)

        def get(self, url, **kwargs):
            if fault_ctl.is_offline:
                with fault_ctl._lock:
                    fault_ctl._dropped_count += 1
                import httpx
                raise httpx.ConnectError("LIFELINE link offline: fault injected")
            return base_client.get(url, **kwargs)

        def stream(self, method, url, **kwargs):
            if fault_ctl.is_offline:
                with fault_ctl._lock:
                    fault_ctl._dropped_count += 1
                import httpx
                raise httpx.ConnectError("LIFELINE link offline: fault injected")
            return base_client.stream(method, url, **kwargs)

    client = CapturingFaultClient()

    memory_service = MemoryService(edge_store, engine, events=bus)
    upload_worker = UploadWorker(
        store=edge_store,
        server_url="http://testserver",
        network_controller=fault_ctl,
        events=bus,
        batch_size=10,
        base_backoff_seconds=0.0,
        custom_client=client,  # type: ignore
    )
    snap_mgr = SnapshotManager(
        store=edge_store,
        engine=engine,
        server_url="http://testserver",
        staging_dir=edge_dir / "staging",
        network_controller=fault_ctl,
        events=bus,
        custom_client=client,  # type: ignore
    )

    # =========================================================================
    # Step 2: Load Reproducible Fixtures
    # =========================================================================
    procedures = get_demo_procedure_cards(dimension=dim)
    for p in procedures:
        memory_service.write(p)

    zone_records = get_zone_history_records(dimension=dim)
    for zr in zone_records:
        memory_service.write(zr)

    # =========================================================================
    # Step 3: Write and Search Locally
    # =========================================================================
    search_res = memory_service.search(SearchRequest(
        context="zone_01",
        dense_vector=[0.1] * dim,
        k=5,
    ))
    assert len(search_res.hits) >= 1
    found_rec_ids = [h.record_id for h in search_res.hits]
    assert "zone01-pump-inspect" in found_rec_ids

    # Search protocols
    proto_res = memory_service.search(SearchRequest(
        context="emergency_protocols",
        dense_vector=[0.05] * dim,
        k=5,
    ))
    assert len(proto_res.hits) >= 1

    # =========================================================================
    # Step 4: Break the Actual Server Transport
    # =========================================================================
    fault_ctl.set_offline(True)
    assert fault_ctl.is_offline is True

    # =========================================================================
    # Step 5: Accept Permitted and Local-Only Records Offline
    # =========================================================================
    # 5a. Permitted Routine Record
    rec_off_routine = RecordEnvelope(
        record_id="rec_offline_routine_1",
        operation_id="op_off_rout_1",
        entity_id="tank_level_gauge",
        context_id="zone_01",
        device_id="edge_bot_1",
        observation={"level_pct": 74.2},
        priority=PriorityLevel.ROUTINE,
        sharing=SharingPolicy.PERMITTED_SHARED,
        dense_vector=[0.2] * dim,
    )
    r1 = memory_service.write(rec_off_routine)
    assert r1.durable is True

    # 5b. Permitted Urgent Record (Alarm)
    rec_off_urgent = RecordEnvelope(
        record_id="rec_offline_urgent_1",
        operation_id="op_off_urg_1",
        entity_id="pressure_relief_valve",
        context_id="zone_01",
        device_id="edge_bot_1",
        observation={"pressure_bar": 12.8, "alert": "overpressure"},
        priority=PriorityLevel.URGENT,
        sharing=SharingPolicy.PERMITTED_SHARED,
        dense_vector=[0.3] * dim,
    )
    r2 = memory_service.write(rec_off_urgent)
    assert r2.durable is True

    # 5c. Local-Only Urgent Record (Must NEVER leave edge!)
    rec_off_private = RecordEnvelope(
        record_id="rec_offline_private_sos",
        operation_id="op_off_priv_1",
        entity_id="internal_security_module",
        context_id="zone_01",
        device_id="edge_bot_1",
        observation={"classified_payload": "CONFIDENTIAL_OPERATOR_SOS"},
        priority=PriorityLevel.URGENT,
        sharing=SharingPolicy.LOCAL_ONLY,
        dense_vector=[0.4] * dim,
    )
    r3 = memory_service.write(rec_off_private)
    assert r3.durable is True

    # Trigger upload while offline: must gracefully fail without crashing
    acked_offline = upload_worker.process_outbox_once()
    assert acked_offline == 0
    assert fault_ctl.get_dropped_count() >= 1

    # =========================================================================
    # Step 6: Restart Edge Services
    # =========================================================================
    engine.close_all()
    # Re-initialize edge components from persisted disk state
    edge_store_restarted = SQLiteMemoryStore(edge_dir / "edge_memory.db")
    engine_restarted = ShardManager(edge_shard_store, ResourceMonitor(), soft_budget_mb=400, default_shard_cost_mb=10)
    engine_restarted.register_shard("local_write", str(lw_path), pinned=True)
    engine_restarted.register_shard("zone_01", str(zone01_base_dir), installed_version="v0")
    engine_restarted.ensure_loaded("local_write")
    engine_restarted.ensure_loaded("zone_01")

    memory_service_restarted = MemoryService(edge_store_restarted, engine_restarted, events=bus)
    upload_worker_restarted = UploadWorker(
        store=edge_store_restarted,
        server_url="http://testserver",
        network_controller=fault_ctl,
        events=bus,
        batch_size=10,
        base_backoff_seconds=0.0,
        custom_client=client,  # type: ignore
    )
    snap_mgr_restarted = SnapshotManager(
        store=edge_store_restarted,
        engine=engine_restarted,
        server_url="http://testserver",
        staging_dir=edge_dir / "staging",
        network_controller=fault_ctl,
        events=bus,
        custom_client=client,  # type: ignore
    )

    # =========================================================================
    # Step 7: Verify Persistence and Local Search
    # =========================================================================
    assert edge_store_restarted.get_record("rec_offline_routine_1") is not None
    assert edge_store_restarted.get_record("rec_offline_urgent_1") is not None
    assert edge_store_restarted.get_record("rec_offline_private_sos") is not None

    post_restart_search = memory_service_restarted.search(SearchRequest(
        context="zone_01",
        dense_vector=[0.3] * dim,
        k=10,
    ))
    restarted_ids = [h.record_id for h in post_restart_search.hits]
    assert "rec_offline_urgent_1" in restarted_ids
    assert "rec_offline_routine_1" in restarted_ids

    # =========================================================================
    # Step 8: Restore Transport
    # =========================================================================
    fault_ctl.set_offline(False)
    assert fault_ctl.is_offline is False

    # =========================================================================
    # Step 9: Verify Urgent Acknowledgment Before Queued Routine Completion
    # =========================================================================
    # Next outbox batch must place ALL URGENT records ahead of routine
    next_batch = edge_store_restarted.get_next_outbox_batch(limit=10)
    assert len(next_batch) >= 2
    
    urgent_indices = [i for i, item in enumerate(next_batch) if item["priority"] == "urgent"]
    routine_indices = [i for i, item in enumerate(next_batch) if item["priority"] == "routine"]
    assert len(urgent_indices) >= 1
    assert len(routine_indices) >= 1
    assert max(urgent_indices) < min(routine_indices), "Urgent records must precede routine records!"
    assert any(item["record_id"] == "rec_offline_urgent_1" for item in next_batch[:len(urgent_indices)])

    # Process outbox
    acked = upload_worker_restarted.process_outbox_once()
    assert acked >= 1
    # Urgent record is now acknowledged
    sync_stat = edge_store_restarted.sync_status()
    assert sync_stat["outbox_acknowledged"] >= 1

    # =========================================================================
    # Step 10: Prepare and Download a Real Compatible Indexed Update
    # =========================================================================
    # Server prepares snapshot for zone_01
    server_candidate = server_prep.prepare_snapshot("zone_01", manifest_version="srv_v1")
    assert server_candidate.record_count >= 1

    # =========================================================================
    # Step 11: Create a Newer Local Edit During Transfer
    # =========================================================================
    # Local worker updates 'zone01-pump-inspect' to version 2 (bearing_warning)
    newer_edit = RecordEnvelope(
        record_id="zone01-pump-inspect",
        operation_id="op_z1_v2_during_transfer",
        entity_id="pump_station_alpha",
        context_id="zone_01",
        device_id="edge_bot_1",
        version=2,
        parent_version=1,
        observation={
            "area": "Zone 01 - North Pumphouse",
            "equipment": "Centrifugal Pump P-101",
            "vibration_rms": 5.8,
            "status": "bearing_warning_critical",
        },
        priority=PriorityLevel.URGENT,
        dense_vector=[0.12 * (i % 4) for i in range(dim)],
    )
    memory_service_restarted.write(newer_edit)

    # =========================================================================
    # Step 12: Activate the Update
    # =========================================================================
    receipt = snap_mgr_restarted.fetch_and_activate("zone_01")
    assert receipt is not None
    assert receipt.success is True
    assert receipt.installed_version == "srv_v1"

    # =========================================================================
    # Step 13: Verify Newer Edit Survives
    # =========================================================================
    res_v2 = memory_service_restarted.search(SearchRequest(
        context="zone_01",
        dense_vector=[0.12 * (i % 4) for i in range(dim)],
        k=10,
    ))
    top_pump_hit = next(h for h in res_v2.hits if h.record_id == "zone01-pump-inspect")
    assert top_pump_hit.version == 2
    assert top_pump_hit.observation["status"] == "bearing_warning_critical"

    # =========================================================================
    # Step 14: Verify Tombstones and Conflicts Remain Correct
    # =========================================================================
    # 14a. Create conflict scenario
    conf_fixture = get_deterministic_conflict_fixture(dimension=dim)
    memory_service_restarted.write(conf_fixture["parent"])
    memory_service_restarted.write(conf_fixture["edit_a"])
    memory_service_restarted.write(conf_fixture["edit_b"])

    active_conflicts = memory_service_restarted.get_conflicts()
    assert any(c["record_id"] == "conflict-valve-v70" for c in active_conflicts)

    # 14b. Verify deletion remains effective
    memory_service_restarted.delete(
        record_id="rec_offline_routine_1",
        operation_id="op_del_rout_1",
        device_id="edge_bot_1",
    )
    del_search = memory_service_restarted.search(SearchRequest(
        context="zone_01",
        dense_vector=[0.2] * dim,
        k=10,
    ))
    assert "rec_offline_routine_1" not in [h.record_id for h in del_search.hits]

    # =========================================================================
    # Step 15: Verify NO Private Record Appeared in Outbound Request Captures or Server
    # =========================================================================
    # 15a. Inspect captured HTTP payloads
    for payload in captured_outbound_payloads:
        items = payload.get("items", [])
        for item in items:
            rec_id = item.get("record_id", "")
            obs = item.get("observation", {})
            assert rec_id != "rec_offline_private_sos", "CRITICAL LEAK: Private SOS was transmitted!"
            assert rec_id != "zone01-local-thermal", "CRITICAL LEAK: Local thermal was transmitted!"
            assert "CONFIDENTIAL_OPERATOR_SOS" not in str(obs)

    # 15b. Inspect server SQLite database directly
    server_records = server_store.get_records_for_context("zone_01")
    server_rec_ids = [r["record_id"] for r in server_records]
    assert "rec_offline_private_sos" not in server_rec_ids
    assert "zone01-local-thermal" not in server_rec_ids

    engine_restarted.close_all()
