"""Reproducible seed dataset and fixtures for LIFELINE demonstrations and testing.

Contains fictional zone history, versioned procedure cards, injected incidents,
and deterministic conflict scenarios. All procedure cards and synthetic incident
triggers are visibly labelled as non-operational demonstration content.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
import uuid

from shared.contracts import (
    PriorityLevel,
    RecordEnvelope,
    RecordStatus,
    SharingPolicy,
)


def get_demo_procedure_cards(dimension: int = 384) -> List[RecordEnvelope]:
    """Versioned emergency and maintenance procedure cards (non-operational demo content)."""
    return [
        RecordEnvelope(
            record_id="proto-hazard-101",
            operation_id="op_seed_proto_101",
            entity_id="emergency_shutdown_procedure",
            context_id="emergency_protocols",
            device_id="safety_board_v1",
            observation={
                "title": "NON-OPERATIONAL DEMO: Emergency Ammonia Leak Response",
                "source": "Synthetic demonstration fixture; no authoritative procedure source",
                "license": "Project-authored demonstration text",
                "revision_status": "UNREVIEWED — NOT FOR OPERATIONAL USE",
                "steps": [
                    "Show locally stored site-approved procedure if one is available",
                    "If no approved procedure is available offline, disclose missing coverage",
                ],
            },
            source_type="procedure_manual",
            source_reference="SOP-HAZ-2026-03",
            version=1,
            status=RecordStatus.ACTIVE,
            sharing=SharingPolicy.PERMITTED_SHARED,
            priority=PriorityLevel.URGENT,
            dense_vector=[0.05 * (i % 5) for i in range(dimension)],
            confidence_source="synthetic_demo_fixture",
        ),
        RecordEnvelope(
            record_id="proto-fire-202",
            operation_id="op_seed_proto_202",
            entity_id="electrical_fire_protocol",
            context_id="emergency_protocols",
            device_id="safety_board_v1",
            observation={
                "title": "NON-OPERATIONAL DEMO: High-Voltage Cabinet Arc Flash / Fire",
                "source": "Synthetic demonstration fixture; no authoritative procedure source",
                "license": "Project-authored demonstration text",
                "revision_status": "UNREVIEWED — NOT FOR OPERATIONAL USE",
                "steps": [
                    "Show locally stored site-approved procedure if one is available",
                    "Escalate to an authorized operator when evidence or procedure coverage is missing",
                ],
            },
            source_type="procedure_manual",
            source_reference="SOP-ELEC-2026-01",
            version=1,
            status=RecordStatus.ACTIVE,
            sharing=SharingPolicy.PERMITTED_SHARED,
            priority=PriorityLevel.URGENT,
            dense_vector=[0.08 * (i % 7) for i in range(dimension)],
            confidence_source="synthetic_demo_fixture",
        ),
    ]


def get_zone_history_records(dimension: int = 384) -> List[RecordEnvelope]:
    """Fictional zone history covering multiple zones, priorities, and sharing policies."""
    records = []

    # Zone 01 History
    records.append(
        RecordEnvelope(
            record_id="zone01-pump-inspect",
            operation_id="op_seed_z1_001",
            entity_id="pump_station_alpha",
            context_id="zone_01",
            device_id="inspection_bot_01",
            observation={
                "area": "Zone 01 - North Pumphouse",
                "equipment": "Centrifugal Pump P-101",
                "vibration_rms": 2.4,
                "bearing_temp_c": 64.2,
                "status": "nominal_routine",
            },
            source_type="detector",
            version=1,
            status=RecordStatus.ACTIVE,
            sharing=SharingPolicy.PERMITTED_SHARED,
            priority=PriorityLevel.ROUTINE,
            dense_vector=[0.12 * (i % 4) for i in range(dimension)],
        )
    )

    # Local-only routine observation in Zone 01 (e.g. preliminary thermal scan)
    records.append(
        RecordEnvelope(
            record_id="zone01-local-thermal",
            operation_id="op_seed_z1_002",
            entity_id="flange_joint_fj12",
            context_id="zone_01",
            device_id="inspection_bot_01",
            observation={
                "area": "Zone 01 - North Pumphouse",
                "equipment": "Flange Joint FJ-12",
                "thermal_delta_c": 4.1,
                "classification": "internal_telemetry_cache",
            },
            source_type="detector",
            version=1,
            status=RecordStatus.ACTIVE,
            sharing=SharingPolicy.LOCAL_ONLY,  # Must never sync to server!
            priority=PriorityLevel.ROUTINE,
            dense_vector=[0.15 * (i % 6) for i in range(dimension)],
        )
    )

    # Zone 02 History
    records.append(
        RecordEnvelope(
            record_id="zone02-conveyor-belt",
            operation_id="op_seed_z2_001",
            entity_id="conveyor_c20",
            context_id="zone_02",
            device_id="inspection_bot_02",
            observation={
                "area": "Zone 02 - Sorting Bay",
                "equipment": "Main Feed Conveyor C-20",
                "belt_alignment": "aligned",
                "motor_current_a": 18.5,
            },
            source_type="detector",
            version=1,
            status=RecordStatus.ACTIVE,
            sharing=SharingPolicy.PERMITTED_SHARED,
            priority=PriorityLevel.ROUTINE,
            dense_vector=[0.09 * (i % 3) for i in range(dimension)],
        )
    )

    return records


def get_deterministic_conflict_fixture(dimension: int = 384) -> Dict[str, Any]:
    """Generates a deterministic concurrent offline conflict on an access point."""
    parent = RecordEnvelope(
        record_id="conflict-valve-v70",
        operation_id="op_conf_parent",
        entity_id="access_point_valve_v70",
        context_id="zone_01",
        device_id="bot_alpha",
        observation={
            "location": "Sub-manifold 7",
            "component": "Isolation Valve V-70",
            "access_hatch": "secured_locked",
            "last_inspected": "2026-09-28T10:00:00Z",
        },
        source_type="operator",
        version=1,
        dense_vector=[0.11 * (i % 5) for i in range(dimension)],
    )

    # Edit A from worker A offline: valve is open
    edit_a = RecordEnvelope(
        record_id="conflict-valve-v70",
        operation_id="op_conf_edit_a",
        entity_id="access_point_valve_v70",
        context_id="zone_01",
        device_id="bot_alpha",
        version=2,
        parent_version=1,
        observation={
            "location": "Sub-manifold 7",
            "component": "Isolation Valve V-70",
            "access_hatch": "open_unlocked_hazard",
            "note": "Worker A reports hatch open for maintenance",
        },
        source_type="detector",
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.URGENT,
        dense_vector=[0.11 * (i % 5) for i in range(dimension)],
    )

    # Edit B from worker B offline: valve is closed
    edit_b = RecordEnvelope(
        record_id="conflict-valve-v70",
        operation_id="op_conf_edit_b",
        entity_id="access_point_valve_v70",
        context_id="zone_01",
        device_id="bot_beta",
        version=2,
        parent_version=1,
        observation={
            "location": "Sub-manifold 7",
            "component": "Isolation Valve V-70",
            "access_hatch": "closed_locked_safe",
            "note": "Worker B reports hatch padlocked shut",
        },
        source_type="detector",
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.URGENT,
        dense_vector=[0.11 * (i % 5) for i in range(dimension)],
    )

    return {
        "parent": parent,
        "edit_a": edit_a,
        "edit_b": edit_b,
        "expected_conflict_record_id": "conflict-valve-v70",
    }
