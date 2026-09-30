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


def _vec(seed: float, dimension: int = 384) -> list:
    """Deterministic pseudo-random dense vector for demo seeding."""
    import math
    return [round(math.sin(seed * (i + 1)) * 0.5 + 0.5, 4) for i in range(dimension)]


def get_demo_procedure_cards(dimension: int = 384) -> List[RecordEnvelope]:
    """Versioned emergency and maintenance procedure cards (non-operational demo content)."""
    return [
        # --- Emergency Protocol 1: Ammonia Leak ---
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
                "hazard_class": "chemical_release",
                "affected_zones": ["Zone 01", "Zone 02"],
                "steps": [
                    "Activate site alarm and broadcast evacuation on all channels",
                    "Don appropriate respiratory PPE before entering affected area",
                    "Locate nearest isolation valve — nominal location Sub-manifold 4",
                    "Show locally stored site-approved procedure if available",
                    "If no approved procedure is available offline, disclose missing coverage",
                    "Do not operate equipment without confirmation from site supervisor",
                ],
            },
            source_type="procedure_manual",
            source_reference="SOP-HAZ-2026-03",
            version=2,
            status=RecordStatus.ACTIVE,
            sharing=SharingPolicy.PERMITTED_SHARED,
            priority=PriorityLevel.URGENT,
            dense_vector=_vec(0.7, dimension),
            confidence_source="synthetic_demo_fixture",
        ),
        # --- Emergency Protocol 2: Electrical Arc Flash ---
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
                "hazard_class": "electrical_fire",
                "affected_zones": ["Zone 02"],
                "steps": [
                    "Immediately de-energise cabinet via remote isolation switch if accessible",
                    "Do NOT use water-based extinguisher on electrical fire",
                    "Use CO2 or dry powder extinguisher located at station E-7",
                    "Evacuate non-essential personnel from the 10-metre exclusion zone",
                    "Escalate to an authorized operator when evidence or procedure coverage is missing",
                ],
            },
            source_type="procedure_manual",
            source_reference="SOP-ELEC-2026-01",
            version=1,
            status=RecordStatus.ACTIVE,
            sharing=SharingPolicy.PERMITTED_SHARED,
            priority=PriorityLevel.URGENT,
            dense_vector=_vec(1.3, dimension),
            confidence_source="synthetic_demo_fixture",
        ),
        # --- Emergency Protocol 3: Pressure Relief Valve ---
        RecordEnvelope(
            record_id="proto-pressure-303",
            operation_id="op_seed_proto_303",
            entity_id="pressure_relief_procedure",
            context_id="emergency_protocols",
            device_id="safety_board_v1",
            observation={
                "title": "NON-OPERATIONAL DEMO: PRV Over-Pressure Activation",
                "source": "Synthetic demonstration fixture",
                "revision_status": "UNREVIEWED — NOT FOR OPERATIONAL USE",
                "hazard_class": "over_pressure",
                "affected_zones": ["Zone 01"],
                "steps": [
                    "Reduce upstream feed pressure by closing feed valve FV-04",
                    "Monitor relief header exhaust to confirm venting",
                    "Do not attempt to manually close an actively lifting PRV",
                    "Notify control room and log event in site CMMS",
                ],
            },
            source_type="procedure_manual",
            source_reference="SOP-PRESS-2026-07",
            version=1,
            status=RecordStatus.ACTIVE,
            sharing=SharingPolicy.PERMITTED_SHARED,
            priority=PriorityLevel.URGENT,
            dense_vector=_vec(2.1, dimension),
            confidence_source="synthetic_demo_fixture",
        ),
        # --- Emergency Protocol 4: Confined Space Rescue ---
        RecordEnvelope(
            record_id="proto-confined-404",
            operation_id="op_seed_proto_404",
            entity_id="confined_space_rescue_procedure",
            context_id="emergency_protocols",
            device_id="safety_board_v1",
            observation={
                "title": "NON-OPERATIONAL DEMO: Confined Space Incapacitation Response",
                "source": "Synthetic demonstration fixture",
                "revision_status": "UNREVIEWED — NOT FOR OPERATIONAL USE",
                "hazard_class": "confined_space",
                "steps": [
                    "Do NOT enter the confined space without a trained rescue team",
                    "Establish atmospheric monitoring at entry point",
                    "Contact emergency services immediately — do not delay for assessment",
                    "Locate rescue harness and tripod at station CS-2",
                ],
            },
            source_type="procedure_manual",
            source_reference="SOP-CS-2026-02",
            version=1,
            status=RecordStatus.ACTIVE,
            sharing=SharingPolicy.PERMITTED_SHARED,
            priority=PriorityLevel.URGENT,
            dense_vector=_vec(2.9, dimension),
            confidence_source="synthetic_demo_fixture",
        ),
    ]


def get_zone_history_records(dimension: int = 384) -> List[RecordEnvelope]:
    """Fictional zone history covering multiple zones, priorities, and sharing policies."""
    records = []

    # -------------------------------------------------------------------------
    # Zone 01 — North Pumphouse
    # -------------------------------------------------------------------------
    records.append(RecordEnvelope(
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
            "inspector_note": "Normal operating envelope — no action required",
        },
        source_type="detector",
        version=1,
        status=RecordStatus.ACTIVE,
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.ROUTINE,
        dense_vector=_vec(0.3, dimension),
    ))

    records.append(RecordEnvelope(
        record_id="zone01-pump-high-vibration",
        operation_id="op_seed_z1_010",
        entity_id="pump_station_alpha",
        context_id="zone_01",
        device_id="inspection_bot_01",
        observation={
            "area": "Zone 01 - North Pumphouse",
            "equipment": "Centrifugal Pump P-101",
            "vibration_rms": 6.8,
            "bearing_temp_c": 81.5,
            "status": "elevated_vibration_warning",
            "inspector_note": "Vibration exceeds 5.0 RMS threshold — schedule bearing inspection within 48h",
        },
        source_type="detector",
        version=1,
        status=RecordStatus.ACTIVE,
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.URGENT,
        dense_vector=_vec(0.55, dimension),
    ))

    records.append(RecordEnvelope(
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
            "inspector_note": "Preliminary scan only — awaiting lab confirmation",
        },
        source_type="detector",
        version=1,
        status=RecordStatus.ACTIVE,
        sharing=SharingPolicy.LOCAL_ONLY,   # Must never sync to server!
        priority=PriorityLevel.ROUTINE,
        dense_vector=_vec(0.42, dimension),
    ))

    records.append(RecordEnvelope(
        record_id="zone01-pressure-anomaly",
        operation_id="op_seed_z1_003",
        entity_id="line_pressure_sensor_lp04",
        context_id="zone_01",
        device_id="inspection_bot_01",
        observation={
            "area": "Zone 01 - Process Header",
            "equipment": "Pressure Transmitter LP-04",
            "pressure_bar": 14.7,
            "upper_limit_bar": 12.0,
            "status": "over_pressure_alert",
            "inspector_note": "Sustained over-pressure detected — PRV check required immediately",
        },
        source_type="detector",
        version=1,
        status=RecordStatus.ACTIVE,
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.URGENT,
        dense_vector=_vec(1.95, dimension),
    ))

    records.append(RecordEnvelope(
        record_id="zone01-valve-v22-inspect",
        operation_id="op_seed_z1_004",
        entity_id="control_valve_v22",
        context_id="zone_01",
        device_id="inspection_bot_01",
        observation={
            "area": "Zone 01 - Valve Gallery",
            "equipment": "Control Valve V-22",
            "actuator_response_ms": 340,
            "stem_position_pct": 72,
            "status": "slow_actuator_warning",
            "inspector_note": "Actuator response 340ms vs 200ms target — flag for predictive maintenance",
        },
        source_type="detector",
        version=1,
        status=RecordStatus.ACTIVE,
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.URGENT,
        dense_vector=_vec(0.88, dimension),
    ))

    # -------------------------------------------------------------------------
    # Zone 02 — Sorting Bay
    # -------------------------------------------------------------------------
    records.append(RecordEnvelope(
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
            "belt_speed_mps": 1.2,
            "status": "nominal_routine",
        },
        source_type="detector",
        version=1,
        status=RecordStatus.ACTIVE,
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.ROUTINE,
        dense_vector=_vec(0.17, dimension),
    ))

    records.append(RecordEnvelope(
        record_id="zone02-conveyor-misaligned",
        operation_id="op_seed_z2_005",
        entity_id="conveyor_c20",
        context_id="zone_02",
        device_id="inspection_bot_02",
        observation={
            "area": "Zone 02 - Sorting Bay",
            "equipment": "Main Feed Conveyor C-20",
            "belt_alignment": "5.2mm_left_drift",
            "motor_current_a": 21.8,
            "status": "misalignment_warning",
            "inspector_note": "Belt tracking drift — activate auto-tracking or schedule manual re-alignment",
        },
        source_type="detector",
        version=1,
        status=RecordStatus.ACTIVE,
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.URGENT,
        dense_vector=_vec(0.25, dimension),
    ))

    records.append(RecordEnvelope(
        record_id="zone02-electrical-cabinet-temp",
        operation_id="op_seed_z2_002",
        entity_id="electrical_cabinet_e07",
        context_id="zone_02",
        device_id="inspection_bot_02",
        observation={
            "area": "Zone 02 - Electrical Room",
            "equipment": "MCC Cabinet E-07",
            "cabinet_temp_c": 58.9,
            "ambient_temp_c": 32.0,
            "delta_t": 26.9,
            "status": "elevated_temperature",
            "inspector_note": "Cabinet delta-T exceeds 20C limit — check ventilation fan and bus bar torque",
        },
        source_type="detector",
        version=1,
        status=RecordStatus.ACTIVE,
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.URGENT,
        dense_vector=_vec(1.25, dimension),
    ))

    records.append(RecordEnvelope(
        record_id="zone02-leak-detector-trigger",
        operation_id="op_seed_z2_003",
        entity_id="leak_sensor_ls09",
        context_id="zone_02",
        device_id="inspection_bot_02",
        observation={
            "area": "Zone 02 - Chemical Store",
            "equipment": "Leak Sensor LS-09",
            "detected_substance": "ammonia_trace",
            "concentration_ppm": 12.4,
            "threshold_ppm": 10.0,
            "status": "threshold_exceeded",
            "inspector_note": "Ammonia trace above safe threshold — activate ventilation and investigate PRV discharge",
        },
        source_type="detector",
        version=1,
        status=RecordStatus.ACTIVE,
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.URGENT,
        dense_vector=_vec(0.68, dimension),
    ))

    records.append(RecordEnvelope(
        record_id="zone02-robot-battery-low",
        operation_id="op_seed_z2_004",
        entity_id="inspection_bot_02",
        context_id="zone_02",
        device_id="inspection_bot_02",
        observation={
            "area": "Zone 02 - Charging Bay",
            "equipment": "Inspection Robot Unit 02",
            "battery_soc_pct": 18,
            "estimated_runtime_min": 22,
            "status": "low_battery_return_to_dock",
            "inspector_note": "SOC < 20% — robot returning to dock, coverage incomplete for sector 2C",
        },
        source_type="detector",
        version=1,
        status=RecordStatus.ACTIVE,
        sharing=SharingPolicy.LOCAL_ONLY,
        priority=PriorityLevel.ROUTINE,
        dense_vector=_vec(0.91, dimension),
    ))

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
        dense_vector=_vec(1.1, dimension),
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
            "note": "Worker A reports hatch open for maintenance access — saw technician inside",
        },
        source_type="detector",
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.URGENT,
        dense_vector=_vec(1.1, dimension),
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
            "note": "Worker B reports hatch padlocked shut — no personnel observed",
        },
        source_type="detector",
        sharing=SharingPolicy.PERMITTED_SHARED,
        priority=PriorityLevel.URGENT,
        dense_vector=_vec(1.1, dimension),
    )

    return {
        "parent": parent,
        "edit_a": edit_a,
        "edit_b": edit_b,
        "expected_conflict_record_id": "conflict-valve-v70",
    }
