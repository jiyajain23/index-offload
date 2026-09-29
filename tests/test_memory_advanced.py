"""Comprehensive tests for Person B Memory persistence, revisions, capacity, and privacy."""

import os
from pathlib import Path
import pytest

from memory.service import MemoryService
from memory.store import (
    IdempotencyConflictError,
    SQLiteMemoryStore,
    StorageCapacityError,
)
from shared.contracts import (
    PriorityLevel,
    RecordEnvelope,
    RecordStatus,
    SearchRequest,
    SharingPolicy,
)
from shared.events import EventBus
from shared.fakes import FakeEngine


def test_accepted_write_survives_restart(tmp_path):
    """Accepted write survives database reconnection / restart."""
    db_file = tmp_path / "memory.db"
    store1 = SQLiteMemoryStore(db_file)
    engine = FakeEngine()
    mem1 = MemoryService(store1, engine)

    rec = RecordEnvelope(
        record_id="rec_001",
        operation_id="op_001",
        entity_id="valve_42",
        context_id="zone_01",
        device_id="edge_dev_1",
        observation={"state": "open", "pressure_psi": 120},
        source_type="detector",
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.ROUTINE,
    )
    receipt1 = mem1.write(rec)
    assert receipt1.durable is True

    # Simulate restart by creating new store instance from same db file
    store2 = SQLiteMemoryStore(db_file)
    mem2 = MemoryService(store2, engine)
    recovered = mem2.get_record("rec_001")
    assert recovered is not None
    assert recovered.record_id == "rec_001"
    assert recovered.observation["state"] == "open"
    assert recovered.version == 1


def test_crash_between_durable_commit_and_projection_recovers(tmp_path):
    """Crash/failure during projection leaves record durable with projection_pending, recoverable on replay."""
    db_file = tmp_path / "memory.db"
    store = SQLiteMemoryStore(db_file)
    
    # Engine that fails projections
    failing_engine = FakeEngine()
    failing_engine.should_fail_projection = True

    mem = MemoryService(store, failing_engine)
    rec = RecordEnvelope(
        record_id="rec_crash",
        operation_id="op_crash",
        entity_id="pump_1",
        context_id="zone_01",
        device_id="edge_dev_1",
        observation={"temp_c": 85},
        source_type="detector",
    )
    receipt = mem.write(rec)
    assert receipt.durable is True
    assert receipt.projection_status == "pending"

    # Now engine recovers
    failing_engine.should_fail_projection = False
    replayed = mem.replay_pending_projections()
    assert replayed == 1
    # Check pending list is now empty
    assert len(store.get_pending_projections()) == 0


def test_idempotency_same_op_returns_cached_result(tmp_path):
    """Repeating an operation with same ID and same content returns cached logical receipt."""
    db_file = tmp_path / "memory.db"
    store = SQLiteMemoryStore(db_file)
    mem = MemoryService(store, FakeEngine())

    rec = RecordEnvelope(
        record_id="rec_idem",
        operation_id="op_same",
        entity_id="door_3",
        context_id="zone_01",
        device_id="edge_dev_1",
        observation={"locked": True},
        source_type="detector",
    )
    r1 = mem.write(rec)
    r2 = mem.write(rec)
    assert r1.record_id == r2.record_id
    assert r1.version == r2.version


def test_idempotency_conflicting_content_fails(tmp_path):
    """Reusing operation ID with differing content is rejected."""
    db_file = tmp_path / "memory.db"
    store = SQLiteMemoryStore(db_file)
    mem = MemoryService(store, FakeEngine())

    rec1 = RecordEnvelope(
        record_id="rec_diff",
        operation_id="op_reuse",
        entity_id="door_3",
        context_id="zone_01",
        device_id="edge_dev_1",
        observation={"locked": True},
        source_type="detector",
    )
    mem.write(rec1)

    rec2 = RecordEnvelope(
        record_id="rec_diff",
        operation_id="op_reuse",  # Reused ID
        entity_id="door_3",
        context_id="zone_01",
        device_id="edge_dev_1",
        observation={"locked": False},  # Differing content!
        source_type="detector",
    )
    with pytest.raises(IdempotencyConflictError):
        mem.write(rec2)


def test_concurrent_edits_become_contested_and_explicitly_resolved(tmp_path):
    """Divergent edits based on same parent remain contested; resolution supersedes both."""
    db_file = tmp_path / "memory.db"
    store = SQLiteMemoryStore(db_file)
    mem = MemoryService(store, FakeEngine())

    # Parent record (Version 1)
    p = RecordEnvelope(
        record_id="access_point_7",
        operation_id="op_parent",
        entity_id="ap_7",
        context_id="zone_01",
        device_id="edge_dev_1",
        observation={"status": "initial_inspection"},
        source_type="operator",
        version=1,
    )
    mem.write(p)

    # Edit A from device A (parent=1)
    edit_a = RecordEnvelope(
        record_id="access_point_7",
        operation_id="op_edit_a",
        entity_id="ap_7",
        context_id="zone_01",
        device_id="edge_dev_A",
        observation={"access_point": "open"},
        source_type="detector",
        version=2,
        parent_version=1,
    )
    mem.write(edit_a)

    # Edit B from device B (also parent=1, concurrent offline conflict!)
    edit_b = RecordEnvelope(
        record_id="access_point_7",
        operation_id="op_edit_b",
        entity_id="ap_7",
        context_id="zone_01",
        device_id="edge_dev_B",
        observation={"access_point": "closed"},
        source_type="detector",
        version=2,
        parent_version=1,
    )
    mem.write(edit_b)

    # Both must be retained, and conflict must be flagged
    conflicts = mem.get_conflicts()
    assert len(conflicts) >= 1
    assert any(c["record_id"] == "access_point_7" for c in conflicts)

    # Explicit conflict resolution
    res_receipt = mem.resolve_conflict(
        record_id="access_point_7",
        entity_id="ap_7",
        context_id="zone_01",
        winning_observation={"access_point": "verified_closed", "reviewer": "supervisor_alice"},
        resolved_operations=["op_edit_a", "op_edit_b"],
        device_id="lead_console",
    )
    assert res_receipt.durable is True
    # Resolved record is active
    resolved_rec = mem.get_record("access_point_7")
    assert resolved_rec.status == RecordStatus.ACTIVE
    assert resolved_rec.observation["access_point"] == "verified_closed"


def test_tombstoned_record_excluded_from_search(tmp_path):
    """Tombstoned record is excluded from search results."""
    db_file = tmp_path / "memory.db"
    store = SQLiteMemoryStore(db_file)
    engine = FakeEngine()
    mem = MemoryService(store, engine)

    rec = RecordEnvelope(
        record_id="rec_del",
        operation_id="op_w1",
        entity_id="gauge_9",
        context_id="zone_01",
        device_id="edge_dev_1",
        observation={"reading": 42},
        source_type="detector",
    )
    mem.write(rec)

    # Delete record
    mem.delete("rec_del", operation_id="op_del1", device_id="edge_dev_1")

    # Search
    res = mem.search(SearchRequest(context="zone_01", k=5))
    hit_ids = [h.record_id for h in res.hits]
    assert "rec_del" not in hit_ids


def test_private_urgent_record_never_enters_outbox(tmp_path):
    """Local-only records must NEVER enter outbound transfer queue even when urgent."""
    db_file = tmp_path / "memory.db"
    store = SQLiteMemoryStore(db_file)
    mem = MemoryService(store, FakeEngine())

    rec = RecordEnvelope(
        record_id="secret_urgent",
        operation_id="op_sec",
        entity_id="classified_hazard",
        context_id="zone_01",
        device_id="edge_dev_1",
        observation={"hazard": "critical_private_incident"},
        source_type="detector",
        sharing=SharingPolicy.LOCAL_ONLY,  # PRIVATE!
        priority=PriorityLevel.URGENT,     # URGENT!
    )
    mem.write(rec)

    # Inspect outbox directly
    outbox_batch = store.get_next_outbox_batch(limit=10)
    outbox_record_ids = [item["record_id"] for item in outbox_batch]
    assert "secret_urgent" not in outbox_record_ids

    status = store.sync_status()
    assert status["outbox_queued"] == 0


def test_routine_capacity_cannot_consume_urgent_reserve(tmp_path):
    """Routine writes fail when regular capacity is filled, but urgent writes succeed."""
    db_file = tmp_path / "memory.db"
    # Set capacity of 150 KB with 30% urgent reserve (45 KB reserve, 105 KB regular)
    store = SQLiteMemoryStore(db_file, max_durable_bytes=150 * 1024, urgent_reserve_percent=0.30)
    mem = MemoryService(store, FakeEngine())

    # Write routine records until regular capacity is exhausted
    filled = False
    for i in range(100):
        try:
            mem.write(RecordEnvelope(
                record_id=f"routine_{i}",
                operation_id=f"op_r_{i}",
                entity_id=f"sensor_{i}",
                context_id="zone_01",
                device_id="edge_dev_1",
                observation={"data": "x" * 1500},
                source_type="detector",
                priority=PriorityLevel.ROUTINE,
            ))
        except StorageCapacityError:
            filled = True
            break

    assert filled is True, "Expected routine capacity to be exhausted"

    # Routine write fails
    with pytest.raises(StorageCapacityError):
        mem.write(RecordEnvelope(
            record_id="routine_extra",
            operation_id="op_r_extra",
            entity_id="sensor_extra",
            context_id="zone_01",
            device_id="edge_dev_1",
            observation={"data": "x" * 2000},
            source_type="detector",
            priority=PriorityLevel.ROUTINE,
        ))

    # But an URGENT write can still use the urgent reserve!
    urgent_receipt = mem.write(RecordEnvelope(
        record_id="urgent_save",
        operation_id="op_u_save",
        entity_id="sos_beacon",
        context_id="zone_01",
        device_id="edge_dev_1",
        observation={"sos": True},
        source_type="detector",
        priority=PriorityLevel.URGENT,
    ))
    assert urgent_receipt.durable is True


def test_listener_failure_does_not_corrupt_storage(tmp_path):
    """Event listener exceptions must never fail a durable commit."""
    db_file = tmp_path / "memory.db"
    store = SQLiteMemoryStore(db_file)
    bus = EventBus()

    # Subscriber that always explodes
    def exploding_listener(event):
        raise RuntimeError("BOOM: Telemetry network crash")

    bus.subscribe(exploding_listener)
    mem = MemoryService(store, FakeEngine(), events=bus)

    rec = RecordEnvelope(
        record_id="rec_safe",
        operation_id="op_safe",
        entity_id="safe_e",
        context_id="zone_01",
        device_id="dev_1",
        observation={"ok": True},
        source_type="detector",
    )
    receipt = mem.write(rec)
    assert receipt.durable is True
    assert mem.get_record("rec_safe") is not None
