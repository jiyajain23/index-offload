"""LIFELINE Shared Contracts (v1).

Defines standard record envelopes, search requests/responses, engine and memory
interfaces, event envelopes, and structured receipts used across all components.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
import time
from typing import Any, Dict, List, Optional
import uuid

from pydantic import BaseModel, Field, model_validator


# -----------------------------------------------------------------------------
# Enums
# -----------------------------------------------------------------------------

class SharingPolicy(str, Enum):
    LOCAL_ONLY = "local_only"
    PERMITTED_SHARED = "permitted_shared"


class PriorityLevel(str, Enum):
    URGENT = "urgent"
    ROUTINE = "routine"


class RecordStatus(str, Enum):
    ACTIVE = "active"
    CONTESTED = "contested"
    TOMBSTONE = "tombstone"
    SUPERSEDED = "superseded"
    RESOLVED = "resolved"


class ShardRole(str, Enum):
    BASE = "base"
    LOCAL_WRITE = "local_write"
    CONTEXT = "context"


class ShardLifecycleState(str, Enum):
    UNLOADED = "unloaded"
    LOADING = "loading"
    LOADED = "loaded"
    CLOSING = "closing"
    EVICTED = "evicted"
    FAILED = "failed"


# -----------------------------------------------------------------------------
# Record Envelope (Person B Lead)
# -----------------------------------------------------------------------------

class RecordEnvelope(BaseModel):
    """Authoritative memory envelope for observations, incident reports and procedures."""

    record_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Stable logical record identity across revisions"
    )
    operation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique operation ID for idempotency and retry deduplication"
    )
    entity_id: str = Field(
        ...,
        description="Logical entity being observed (e.g., equipment ID, valve, zone)"
    )
    context_id: str = Field(
        ...,
        description="Context shard identifier (e.g. 'zone_01', 'emergency_protocols')"
    )
    device_id: str = Field(
        ...,
        description="Identifier of originating device"
    )
    observation: Dict[str, Any] = Field(
        default_factory=dict,
        description="Structured observation or claim payload"
    )
    source_type: str = Field(
        default="detector",
        description="Source type (e.g. 'detector', 'operator_manual', 'server_sync')"
    )
    source_reference: Optional[str] = Field(
        None,
        description="Reference to source frame, procedure document, or incident ID"
    )
    observed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    recorded_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    version: int = Field(
        default=1,
        ge=1,
        description="Monotonic revision counter for this record_id"
    )
    parent_version: Optional[int] = Field(
        None,
        description="Previous version superseded by this edit, if revision"
    )
    status: RecordStatus = Field(
        default=RecordStatus.ACTIVE
    )
    tombstone: bool = Field(
        default=False,
        description="True if this record represents a deletion"
    )
    sharing: SharingPolicy = Field(
        default=SharingPolicy.PERMITTED_SHARED,
        description="Local-only vs permitted for server synchronization"
    )
    priority: PriorityLevel = Field(
        default=PriorityLevel.ROUTINE,
        description="Routine vs urgent (independent of sharing policy)"
    )
    embedding_model_version: str = Field(
        default="minilm-l6-v2",
        description="Model and version used to compute vectors"
    )
    dense_vector: Optional[List[float]] = Field(
        None,
        description="Dense embedding vector"
    )
    sparse_indices: Optional[List[int]] = Field(
        None,
        description="Sparse token indices"
    )
    sparse_values: Optional[List[float]] = Field(
        None,
        description="Sparse token weights"
    )
    confidence: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Observation or detector confidence score"
    )
    confidence_source: Optional[str] = Field(
        None,
        description="Model or operator providing the confidence score"
    )
    conflict_alternatives: List[str] = Field(
        default_factory=list,
        description="Operation or record IDs of concurrent conflicting revisions"
    )
    resolution_of: List[str] = Field(
        default_factory=list,
        description="Record/operation IDs resolved by this record"
    )

    @model_validator(mode="after")
    def validate_tombstone_status(self) -> RecordEnvelope:
        if self.tombstone and self.status != RecordStatus.TOMBSTONE:
            self.status = RecordStatus.TOMBSTONE
        return self


# -----------------------------------------------------------------------------
# Search Request & Response (Shared A/B/C)
# -----------------------------------------------------------------------------

class SearchFilter(BaseModel):
    must: Optional[Dict[str, Any]] = None
    must_not: Optional[Dict[str, Any]] = None


class SearchRequest(BaseModel):
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    query_text: Optional[str] = None
    dense_vector: Optional[List[float]] = None
    sparse_indices: Optional[List[int]] = None
    sparse_values: Optional[List[float]] = None
    embedding_model_version: str = "minilm-l6-v2"
    context: str = Field(..., description="Target context (e.g. 'zone_01')")
    include_protocols: bool = Field(
        default=True,
        description="Whether to also search pinned emergency protocols"
    )
    filters: Optional[SearchFilter] = None
    k: int = Field(default=5, ge=1, le=100)
    deadline_ms: Optional[float] = Field(
        default=None,
        description="Hard deadline in milliseconds from request receipt"
    )


class CoverageSummary(BaseModel):
    searched: List[str] = Field(default_factory=list)
    queued: List[str] = Field(default_factory=list)
    unavailable_offline: List[str] = Field(default_factory=list)
    skipped_for_budget: List[str] = Field(default_factory=list)
    failed: List[str] = Field(default_factory=list)


class CandidateHit(BaseModel):
    """Raw hit returned from an Edge shard."""
    point_id: Any
    shard_id: str
    shard_version: Optional[str] = None
    score: float
    payload: Dict[str, Any] = Field(default_factory=dict)
    dense_vector: Optional[List[float]] = None


class EngineSearchResult(BaseModel):
    candidates: List[CandidateHit] = Field(default_factory=list)
    coverage: CoverageSummary = Field(default_factory=CoverageSummary)
    elapsed_ms: float = 0.0
    reasons: Dict[str, str] = Field(default_factory=dict)


class MemorySearchHit(BaseModel):
    """Authoritative hit returned to application caller after revision/tombstone reconciliation."""
    record_id: str
    version: int
    entity_id: str
    context_id: str
    source_shard: str
    installed_version: Optional[str] = None
    score: float
    status: RecordStatus
    is_contested: bool = False
    conflict_alternatives: List[str] = Field(default_factory=list)
    observation: Dict[str, Any] = Field(default_factory=dict)
    confidence: Optional[float] = None
    confidence_source: Optional[str] = None
    observed_at: datetime
    tombstone: bool = False


class SearchResponse(BaseModel):
    hits: List[MemorySearchHit] = Field(default_factory=list)
    coverage: CoverageSummary = Field(default_factory=CoverageSummary)
    elapsed_ms: float = 0.0
    partial: bool = False
    shortfall: int = 0
    reasons: Dict[str, str] = Field(default_factory=dict)


# -----------------------------------------------------------------------------
# Receipts & Operation Results
# -----------------------------------------------------------------------------

class WriteReceipt(BaseModel):
    record_id: str
    operation_id: str
    version: int
    durable: bool
    projection_status: str  # "ready", "pending", "failed"
    capacity_rejected: bool = False
    rejection_reason: Optional[str] = None
    persisted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ProjectionUpsertRequest(BaseModel):
    record_id: str
    operation_id: Optional[str] = None  # Immutable revision identity; required for new writes.
    version: int
    context_id: str
    dense_vector: Optional[List[float]] = None
    sparse_indices: Optional[List[int]] = None
    sparse_values: Optional[List[float]] = None
    payload: Dict[str, Any] = Field(default_factory=dict)


class ProjectionDeleteRequest(BaseModel):
    record_id: str
    version: Optional[int] = None
    context_id: str
    operation_ids: List[str] = Field(default_factory=list)


class ProjectionReceipt(BaseModel):
    record_id: str
    context_id: str
    success: bool
    version_installed: Optional[int] = None
    error: Optional[str] = None


class SnapshotCandidate(BaseModel):
    """Staged snapshot candidate prepared by B for A to activate."""
    snapshot_id: str
    target_shard_id: str
    context_id: str
    staging_path: str
    manifest_version: str
    schema_version: str = "v1"
    embedding_model_version: str = "minilm-l6-v2"
    checksum_sha256: str
    archive_format: str = "tar"  # tar: legacy demo; qdrant: actual Server shard snapshot
    expected_files: List[str] = Field(default_factory=list)
    record_count: int = 0
    included_records: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Exact record_id -> version inventory installed in snapshot"
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ActivationReceipt(BaseModel):
    snapshot_id: str
    shard_id: str
    success: bool
    installed_version: Optional[str] = None
    reverted_to_previous: bool = False
    activated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error: Optional[str] = None


# -----------------------------------------------------------------------------
# Event Envelope (Shared C/A/B)
# -----------------------------------------------------------------------------

class LifelineEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: str = "v1"
    emitted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    component: str  # "engine", "memory", "sync", "server", "detector"
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str  # "budget_update", "fps_update", "shard_activated", etc.
    payload: Dict[str, Any] = Field(default_factory=dict)


# -----------------------------------------------------------------------------
# Abstract Interfaces
# -----------------------------------------------------------------------------

class IEngine(ABC):
    """Person A Engine interface."""

    @abstractmethod
    def query_shards(self, request: SearchRequest) -> EngineSearchResult:
        """Execute dense and/or sparse retrieval across required shards within deadline."""
        pass

    @abstractmethod
    def upsert_projection(self, request: ProjectionUpsertRequest) -> ProjectionReceipt:
        """Idempotently write a point into the mutable local-write shard."""
        pass

    @abstractmethod
    def delete_projection(self, request: ProjectionDeleteRequest) -> ProjectionReceipt:
        """Idempotently delete or tombstone a point from searchable projection."""
        pass

    @abstractmethod
    def activate_snapshot(self, candidate: SnapshotCandidate) -> ActivationReceipt:
        """Safely switch an active shard to a staged candidate snapshot with rollback."""
        pass

    @abstractmethod
    def status(self) -> Dict[str, Any]:
        """Return engine resource usage, budget, loaded shards, and queue depths."""
        pass

    @abstractmethod
    def close_all(self) -> None:
        """Safely drain in-flight work and close all open EdgeShard handles."""
        pass


class IMemory(ABC):
    """Person B Memory & Sync interface."""

    @abstractmethod
    def write(self, record: RecordEnvelope) -> WriteReceipt:
        """Durably persist record, enforce capacity & privacy, and project into search."""
        pass

    @abstractmethod
    def search(self, request: SearchRequest) -> SearchResponse:
        """Reconcile engine candidates against authoritative revisions and tombstones."""
        pass

    @abstractmethod
    def get_record(self, record_id: str) -> Optional[RecordEnvelope]:
        """Fetch current authoritative record revision."""
        pass

    @abstractmethod
    def get_conflicts(self) -> List[Dict[str, Any]]:
        """List currently contested divergent records."""
        pass

    @abstractmethod
    def resolve_conflict(
        self,
        record_id: str,
        entity_id: str,
        context_id: str,
        winning_observation: Dict[str, Any],
        resolved_operations: List[str],
        device_id: str,
        priority: PriorityLevel = PriorityLevel.ROUTINE,
        sharing: SharingPolicy = SharingPolicy.PERMITTED_SHARED,
    ) -> WriteReceipt:
        """Explicitly resolve contested records with a superseding resolution record."""
        pass

    @abstractmethod
    def sync_status(self) -> Dict[str, Any]:
        """Return outbox queue counts, pending bytes, urgent backlog, and sync state."""
        pass
