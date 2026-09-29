"""Deterministic, budgeted lifecycle management and retrieval execution for local vector shards."""

from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import gc
import hashlib
import logging
import os
from pathlib import Path
import shutil
import tarfile
import threading
import time
from typing import Any, Dict, Generator, List, Optional, Set, Tuple
import uuid

from shared.contracts import (
    ActivationReceipt,
    CandidateHit,
    CoverageSummary,
    EngineSearchResult,
    IEngine,
    PriorityLevel,
    ProjectionDeleteRequest,
    ProjectionReceipt,
    ProjectionUpsertRequest,
    RecordEnvelope,
    RecordStatus,
    SearchRequest,
    ShardLifecycleState,
    ShardRole,
    SnapshotCandidate,
)
from shared.events import EventBus
from .events import EventEmitter
from .monitor import ResourceMonitor

logger = logging.getLogger("lifeline.engine")
MB = 1024 * 1024


class BudgetExceeded(RuntimeError):
    """No eligible on-demand shard can be evicted to make room within the budget."""


class ConfigurationError(ValueError):
    """Impossible or invalid shard configuration at startup."""


@dataclass
class ShardRegistration:
    shard_id: str
    path: str
    contexts: List[str] = field(default_factory=list)
    pinned: bool = False
    role: ShardRole = ShardRole.CONTEXT
    installed_version: str = "v1"
    estimated_cost_bytes: Optional[int] = None
    state: ShardLifecycleState = ShardLifecycleState.UNLOADED
    active_operations: int = 0
    last_accessed: float = 0.0
    failure_reason: Optional[str] = None


class ShardManager(IEngine):
    """Manages live Qdrant Edge shard handles, lifecycle, operation leases, and search execution."""

    def __init__(
        self,
        shard_store: Any,
        resource_monitor: ResourceMonitor,
        soft_budget_mb: Optional[float] = None,
        events: Optional[Any] = None,
        default_shard_cost_mb: float = 40.0,
        max_concurrency: int = 4,
        max_queue_depth: int = 16,
        cooldown_seconds: float = 1.0,
        recovery_hysteresis_mb: float = 10.0,
    ):
        self.shard_store = shard_store
        self.resource_monitor = resource_monitor
        self.events = events or EventEmitter()

        hard_limit = self.resource_monitor.snapshot().memory_max_bytes
        if soft_budget_mb is None:
            if hard_limit is None:
                raise ValueError("soft_budget_mb is required when cgroup memory.max is unavailable")
            self.soft_budget_bytes = int(hard_limit * 0.80)
        else:
            self.soft_budget_bytes = int(soft_budget_mb * MB)

        self.hard_memory_limit_bytes = hard_limit
        self.default_shard_cost_bytes = int(default_shard_cost_mb * MB)
        self.recovery_hysteresis_bytes = int(recovery_hysteresis_mb * MB)
        self.cooldown_seconds = cooldown_seconds
        self.max_concurrency = max_concurrency
        self.max_queue_depth = max_queue_depth

        self._registrations: Dict[str, ShardRegistration] = {}
        self._loaded: OrderedDict[str, Any] = OrderedDict()
        self._observed_costs: List[int] = []
        self._last_eviction_times: Dict[str, float] = {}
        self._projected_versions: Dict[str, int] = {}  # record_id -> highest projected version

        # Thread synchronization
        self._lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._semaphore = threading.BoundedSemaphore(max_concurrency)
        self._queue_condition = threading.Condition(self._lock)
        self._current_queue_depth = 0
        self._accepting_work = True

    # -------------------------------------------------------------------------
    # Registry & Lifecycle Operations
    # -------------------------------------------------------------------------

    def register_shard(
        self,
        shard_id: str,
        path: str,
        pinned: bool = False,
        estimated_cost_mb: Optional[float] = None,
        contexts: Optional[List[str]] = None,
        role: ShardRole = ShardRole.CONTEXT,
        installed_version: str = "v1",
    ) -> None:
        with self._lock:
            if shard_id in self._registrations:
                raise ValueError(f"Shard already registered: {shard_id}")

            ctx_list = list(contexts) if contexts else [shard_id]
            est_cost = int(estimated_cost_mb * MB) if estimated_cost_mb is not None else None

            self._registrations[shard_id] = ShardRegistration(
                shard_id=shard_id,
                path=path,
                contexts=ctx_list,
                pinned=pinned,
                role=role,
                installed_version=installed_version,
                estimated_cost_bytes=est_cost,
                state=ShardLifecycleState.UNLOADED,
            )

    def validate_pinned_configuration(self) -> None:
        """Reject impossible pinned configurations at startup with a specific error."""
        with self._lock:
            pinned_shards = [reg for reg in self._registrations.values() if reg.pinned]
            total_pinned_estimate = sum(self._estimate_cost(reg) for reg in pinned_shards)
            if total_pinned_estimate > self.soft_budget_bytes:
                raise ConfigurationError(
                    f"Impossible pinned configuration: total pinned shards estimated cost "
                    f"({total_pinned_estimate / MB:.1f} MB) exceeds soft budget "
                    f"({self.soft_budget_bytes / MB:.1f} MB)"
                )

    def load_pinned(self) -> None:
        """Load all pinned shards, rolling back completely on any failure."""
        self.validate_pinned_configuration()
        loaded_during_call = []
        try:
            with self._lock:
                for shard_id, registration in list(self._registrations.items()):
                    if registration.pinned:
                        self.ensure_loaded(shard_id)
                        loaded_during_call.append(shard_id)
        except Exception as e:
            logger.error("Failed loading pinned shards; cleaning up loaded shards: %s", e)
            for sid in loaded_during_call:
                try:
                    self._close(sid, reason="startup_failure_cleanup")
                except Exception:
                    pass
            raise

    def set_soft_budget_bytes(self, soft_budget_bytes: int) -> None:
        """Update the application-level budget and reconcile immediately."""
        with self._lock:
            if soft_budget_bytes <= 0:
                raise ValueError("soft_budget_bytes must be positive")
            self.soft_budget_bytes = soft_budget_bytes
            # Immediate safe reconciliation
            self._reconcile_budget_overage()
            self._emit_budget_update()

    def _reconcile_budget_overage(self) -> None:
        """Evict idle unpinned shards if current usage exceeds budget."""
        while self._usage_bytes() > self.soft_budget_bytes:
            victim = next(
                (
                    sid
                    for sid in self._loaded
                    if not self._registrations[sid].pinned
                    and self._registrations[sid].active_operations == 0
                ),
                None,
            )
            if victim is None:
                break
            self._close(victim, reason="budget_reduction")

    # -------------------------------------------------------------------------
    # Operation Leases & Synchronization
    # -------------------------------------------------------------------------

    @contextmanager
    def lease_shard(self, shard_id: str) -> Generator[Any, None, None]:
        """Acquire a read/search lease on a shard. Prevents eviction or close while active."""
        with self._lock:
            if not self._accepting_work:
                raise RuntimeError("ShardManager is shutting down; not accepting new leases")
            self.ensure_loaded(shard_id)
            reg = self._registrations[shard_id]
            reg.active_operations += 1
            reg.last_accessed = time.time()
            shard_handle = self._loaded[shard_id]

        try:
            yield shard_handle
        finally:
            with self._lock:
                reg.active_operations = max(0, reg.active_operations - 1)
                self._queue_condition.notify_all()

    def ensure_loaded(self, shard_id: str) -> bool:
        with self._lock:
            reg = self._registration(shard_id)
            if shard_id in self._loaded:
                self._loaded.move_to_end(shard_id)
                reg.last_accessed = time.time()
                return True

            # Cooldown check: prevent rapid load/unload thrashing of the same shard
            last_evicted = self._last_eviction_times.get(shard_id, 0.0)
            if (time.time() - last_evicted) < self.cooldown_seconds:
                # If memory is still within recovery hysteresis of budget, delay or reject
                if self._usage_bytes() + self.recovery_hysteresis_bytes > self.soft_budget_bytes:
                    raise BudgetExceeded(
                        f"Shard {shard_id} is in eviction cooldown ({self.cooldown_seconds}s) to prevent thrashing"
                    )

            estimate = self._estimate_cost(reg)
            evicted = self._evict_until_room(estimate)
            before = self._usage_bytes()
            reg.state = ShardLifecycleState.LOADING

            try:
                shard = self.shard_store.load(reg.path)
            except Exception as exc:
                reg.state = ShardLifecycleState.FAILED
                reg.failure_reason = str(exc)
                self._emit_event("shard_load_failed", shard_id=shard_id, error=str(exc))
                raise

            after = self._usage_bytes()
            if after > self.soft_budget_bytes:
                # Actual load cost exceeded soft budget. Close immediately and reject.
                self.shard_store.close(shard)
                gc.collect()
                reg.state = ShardLifecycleState.UNLOADED
                self._emit_event(
                    "shard_load_rejected",
                    shard_id=shard_id,
                    usage_bytes=after,
                    soft_budget_bytes=self.soft_budget_bytes,
                )
                self._emit_budget_update()
                raise BudgetExceeded(f"Loading {shard_id} exceeded the soft memory budget")

            observed = max(0, after - before)
            if observed:
                self._observed_costs.append(observed)
            self._loaded[shard_id] = shard
            reg.state = ShardLifecycleState.LOADED
            reg.last_accessed = time.time()

            self._emit_event(
                "shard_loaded",
                shard_id=shard_id,
                pinned=reg.pinned,
                evicted=evicted,
                observed_cost_bytes=observed,
            )
            self._emit_budget_update()
            return True

    def _evict_until_room(self, incoming_cost: int) -> List[str]:
        evicted = []
        while self._usage_bytes() + incoming_cost > self.soft_budget_bytes:
            # Only evict unpinned shards with 0 active operations
            victim = next(
                (
                    sid
                    for sid in self._loaded
                    if not self._registrations[sid].pinned
                    and self._registrations[sid].active_operations == 0
                ),
                None,
            )
            if victim is None:
                raise BudgetExceeded("No eligible unpinned idle shard can be evicted to make room")
            self._close(victim, reason="budget")
            evicted.append(victim)
        return evicted

    def _close(self, shard_id: str, reason: str) -> None:
        shard = self._loaded.pop(shard_id)
        reg = self._registrations[shard_id]
        reg.state = ShardLifecycleState.CLOSING
        self.shard_store.close(shard)
        gc.collect()
        reg.state = ShardLifecycleState.EVICTED
        self._last_eviction_times[shard_id] = time.time()
        self._emit_event("shard_evicted", shard_id=shard_id, reason=reason)
        self._emit_budget_update()

    # -------------------------------------------------------------------------
    # Retrieval Execution (IEngine Interface)
    # -------------------------------------------------------------------------

    def query_shards(self, request: SearchRequest) -> EngineSearchResult:
        """Execute retrieval across required shards within deadline and queue constraints."""
        start_time = time.perf_counter()
        deadline_at = (start_time + (request.deadline_ms / 1000.0)) if request.deadline_ms else None

        # Queue admission control
        with self._lock:
            if not self._accepting_work:
                return EngineSearchResult(
                    candidates=[],
                    coverage=CoverageSummary(failed=[request.context]),
                    elapsed_ms=0.0,
                    reasons={"shutdown": "Engine is stopping"},
                )
            if self._current_queue_depth >= self.max_queue_depth:
                return EngineSearchResult(
                    candidates=[],
                    coverage=CoverageSummary(skipped_for_budget=[request.context]),
                    elapsed_ms=(time.perf_counter() - start_time) * 1000.0,
                    reasons={"overload": "Query queue capacity exceeded"},
                )
            self._current_queue_depth += 1

        try:
            # Wait for search concurrency slot with deadline check
            acquired = False
            while not acquired:
                if deadline_at and time.perf_counter() >= deadline_at:
                    return EngineSearchResult(
                        candidates=[],
                        coverage=CoverageSummary(failed=[request.context]),
                        elapsed_ms=(time.perf_counter() - start_time) * 1000.0,
                        reasons={"deadline": "Expired waiting in query admission queue"},
                    )
                acquired = self._semaphore.acquire(timeout=0.02)
        finally:
            with self._lock:
                self._current_queue_depth = max(0, self._current_queue_depth - 1)

        try:
            return self._execute_retrieval(request, start_time, deadline_at)
        finally:
            self._semaphore.release()

    def _execute_retrieval(
        self, request: SearchRequest, start_time: float, deadline_at: Optional[float]
    ) -> EngineSearchResult:
        searched: List[str] = []
        queued: List[str] = []
        unavailable: List[str] = []
        skipped_for_budget: List[str] = []
        failed: List[str] = []
        reasons: Dict[str, str] = {}

        # Resolve shards to query
        target_shard_ids: List[str] = []
        with self._lock:
            # 1. Dedicated mutable local_write shard is always queried for up-to-date projections
            if "local_write" in self._registrations:
                target_shard_ids.append("local_write")

            # 2. Pinned emergency protocol shards if requested
            if request.include_protocols:
                for sid, reg in self._registrations.items():
                    if reg.pinned and sid != "local_write" and sid not in target_shard_ids:
                        target_shard_ids.append(sid)

            # 3. Context shards
            matched_context_shards = [
                sid for sid, reg in self._registrations.items()
                if request.context in reg.contexts or request.context == sid
            ]
            if not matched_context_shards:
                unavailable.append(request.context)
                reasons[request.context] = "Context not found in shard registry"
            else:
                for sid in matched_context_shards:
                    if sid not in target_shard_ids:
                        target_shard_ids.append(sid)

        # Execute search across each target shard under operation leases
        all_candidates: List[CandidateHit] = []

        for shard_id in target_shard_ids:
            reg = self._registrations.get(shard_id)
            if not reg:
                unavailable.append(shard_id)
                reasons[shard_id] = "Shard not found in registry"
                continue

            # Check deadline before loading/querying
            if deadline_at and time.perf_counter() >= deadline_at:
                failed.append(shard_id)
                reasons[shard_id] = "Search deadline expired before execution"
                continue

            # Check disk availability if real store and shard not already loaded
            is_real_store = hasattr(self.shard_store, "config")
            if is_real_store and not os.path.exists(reg.path) and shard_id not in self._loaded:
                unavailable.append(shard_id)
                reasons[shard_id] = f"Shard directory missing on disk: {reg.path}"
                continue

            try:
                with self.lease_shard(shard_id) as shard_handle:
                    if hasattr(self.shard_store, "sparse_vector_name"):
                        hits = self.shard_store.search(
                            shard=shard_handle,
                            query_vector=request.dense_vector,
                            k=request.k,
                            sparse_indices=request.sparse_indices,
                            sparse_values=request.sparse_values,
                            with_payload=True,
                        )
                    else:
                        hits = self.shard_store.search(shard_handle, request.dense_vector, request.k)
                    searched.append(shard_id)

                    for hit in hits:
                        # Extract hit metadata cleanly from Edge SDK ScoredPoint or dict
                        raw_point_id = getattr(hit, "id", None)
                        point_id = str(raw_point_id) if raw_point_id is not None else "unknown"
                        score = float(getattr(hit, "score", 0.0))
                        payload = getattr(hit, "payload", {}) or {}

                        all_candidates.append(CandidateHit(
                            point_id=point_id,
                            shard_id=shard_id,
                            shard_version=reg.installed_version,
                            score=score,
                            payload=payload,
                        ))

            except BudgetExceeded as be:
                skipped_for_budget.append(shard_id)
                reasons[shard_id] = str(be)
            except Exception as exc:
                failed.append(shard_id)
                reasons[shard_id] = f"Query failure: {exc}"

        # Cross-shard candidate deduplication and ranking:
        # Prefer hits from local_write or higher score
        seen_points: Dict[str, CandidateHit] = {}
        for cand in all_candidates:
            pid = str(cand.point_id)
            if pid not in seen_points:
                seen_points[pid] = cand
            else:
                # If existing is base and new is local_write, prefer local_write
                existing = seen_points[pid]
                if cand.shard_id == "local_write":
                    seen_points[pid] = cand
                elif existing.shard_id != "local_write" and cand.score > existing.score:
                    seen_points[pid] = cand

        final_candidates = list(seen_points.values())
        final_candidates.sort(key=lambda c: c.score, reverse=True)

        elapsed = (time.perf_counter() - start_time) * 1000.0
        return EngineSearchResult(
            candidates=final_candidates[:request.k],
            coverage=CoverageSummary(
                searched=searched,
                queued=queued,
                unavailable_offline=unavailable,
                skipped_for_budget=skipped_for_budget,
                failed=failed,
            ),
            elapsed_ms=elapsed,
            reasons=reasons,
        )

    # Backward-compatible search method for existing tests
    def search(self, query_vector: Any, context: str, k: int = 5) -> Dict[str, Any]:
        req = SearchRequest(
            query_text=None,
            dense_vector=query_vector if isinstance(query_vector, list) else list(query_vector),
            context=context,
            k=k,
        )
        res = self.query_shards(req)
        # Adapt back to legacy dict format for existing tests
        hits = [{"shard_id": c.shard_id, "hit": c} for c in res.candidates]
        return {
            "hits": hits,
            "coverage": {
                "searched": res.coverage.searched,
                "queued": res.coverage.queued + res.coverage.skipped_for_budget,
                "unavailable_offline": res.coverage.unavailable_offline,
            },
        }

    # -------------------------------------------------------------------------
    # Mutable Projection Writes & Deletes (IEngine Interface)
    # -------------------------------------------------------------------------

    @staticmethod
    def to_qdrant_point_id(val: Any) -> str:
        try:
            return str(uuid.UUID(str(val)))
        except (ValueError, AttributeError):
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(val)))

    def upsert_projection(self, request: ProjectionUpsertRequest) -> ProjectionReceipt:
        """Idempotently project an accepted record into the non-evictable local-write shard."""
        with self._write_lock:
            # Version guard: older retries must not overwrite newer projected versions
            curr_ver = self._projected_versions.get(request.record_id, 0)
            if request.version < curr_ver:
                return ProjectionReceipt(
                    record_id=request.record_id,
                    context_id=request.context_id,
                    success=True,
                    version_installed=curr_ver,
                )

            from qdrant_edge import Point, SparseVector

            # Check if local_write shard is loaded
            target_shard = "local_write" if "local_write" in self._registrations else request.context_id
            if target_shard not in self._registrations:
                return ProjectionReceipt(
                    record_id=request.record_id,
                    context_id=request.context_id,
                    success=False,
                    error=f"No writable shard registered for projection ({target_shard})",
                )

            vector_dict: Dict[str, Any] = {}
            if request.dense_vector:
                vector_dict["embedding"] = request.dense_vector
            if request.sparse_indices and request.sparse_values:
                vector_dict["text_sparse"] = SparseVector(
                    indices=request.sparse_indices,
                    values=request.sparse_values,
                )

            point_id = self.to_qdrant_point_id(request.record_id)
            pt = Point(
                id=point_id,
                vector=vector_dict if vector_dict else {"embedding": [0.0] * 384},
                payload={"record_id": request.record_id, "version": request.version, **request.payload},
            )

            try:
                with self.lease_shard(target_shard) as shard_handle:
                    self.shard_store.upsert_points(shard_handle, [pt])
                self._projected_versions[request.record_id] = request.version
                return ProjectionReceipt(
                    record_id=request.record_id,
                    context_id=request.context_id,
                    success=True,
                    version_installed=request.version,
                )
            except Exception as e:
                logger.error("Projection upsert failed for record %s: %s", request.record_id, e)
                return ProjectionReceipt(
                    record_id=request.record_id,
                    context_id=request.context_id,
                    success=False,
                    error=str(e),
                )

    def delete_projection(self, request: ProjectionDeleteRequest) -> ProjectionReceipt:
        """Idempotently delete a record projection."""
        with self._write_lock:
            target_shard = "local_write" if "local_write" in self._registrations else request.context_id
            if target_shard in self._loaded:
                try:
                    point_id = self.to_qdrant_point_id(request.record_id)
                    with self.lease_shard(target_shard) as shard_handle:
                        self.shard_store.delete_points(shard_handle, [point_id])
                except Exception as e:
                    logger.warning("Error deleting projection for %s: %s", request.record_id, e)
            self._projected_versions.pop(request.record_id, None)
            return ProjectionReceipt(
                record_id=request.record_id,
                context_id=request.context_id,
                success=True,
                version_installed=request.version,
            )

    # -------------------------------------------------------------------------
    # Snapshot Activation (IEngine Interface)
    # -------------------------------------------------------------------------

    def activate_snapshot(self, candidate: SnapshotCandidate) -> ActivationReceipt:
        """Validate candidate and safely activate it with full rollback protection."""
        with self._lock:
            shard_id = candidate.target_shard_id
            reg = self._registrations.get(shard_id)
            if not reg:
                return ActivationReceipt(
                    snapshot_id=candidate.snapshot_id,
                    shard_id=shard_id,
                    success=False,
                    error=f"Target shard {shard_id} not registered",
                )

            # 1. Validate candidate archive exists and verify SHA256 checksum
            candidate_path = Path(candidate.staging_path)
            if not candidate_path.exists():
                return ActivationReceipt(
                    snapshot_id=candidate.snapshot_id,
                    shard_id=shard_id,
                    success=False,
                    error=f"Candidate staging path does not exist: {candidate_path}",
                )

            hasher = hashlib.sha256()
            with open(candidate_path, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    hasher.update(chunk)
            computed_checksum = hasher.hexdigest()

            if computed_checksum != candidate.checksum_sha256:
                return ActivationReceipt(
                    snapshot_id=candidate.snapshot_id,
                    shard_id=shard_id,
                    success=False,
                    error=f"Checksum mismatch: expected {candidate.checksum_sha256}, got {computed_checksum}",
                )

            # 2. Extract into safe temporary staging directory, preventing path traversal
            temp_unpack = candidate_path.parent / f"unpack_{candidate.snapshot_id}"
            if temp_unpack.exists():
                shutil.rmtree(temp_unpack, ignore_errors=True)
            temp_unpack.mkdir(parents=True, exist_ok=True)

            try:
                with tarfile.open(candidate_path, "r:*") as tar:
                    for member in tar.getmembers():
                        # Path traversal guard
                        target_member_path = (temp_unpack / member.name).resolve()
                        if not str(target_member_path).startswith(str(temp_unpack.resolve())):
                            raise SecurityError(f"Path traversal detected in snapshot archive: {member.name}")
                    tar.extractall(path=temp_unpack)

                # Validate unpack contains valid Qdrant Edge shard files
                test_shard = self.shard_store.load(str(temp_unpack))
                self.shard_store.close(test_shard)

            except Exception as e:
                shutil.rmtree(temp_unpack, ignore_errors=True)
                return ActivationReceipt(
                    snapshot_id=candidate.snapshot_id,
                    shard_id=shard_id,
                    success=False,
                    error=f"Candidate validation / test load failed: {e}",
                    reverted_to_previous=True,
                )

            # 3. Coordinate readers: wait for active operations on old shard to drain
            while reg.active_operations > 0:
                logger.info("Waiting for %d active operations on %s to finish before activation", reg.active_operations, shard_id)
                self._queue_condition.wait(timeout=0.1)

            # 4. Safe atomic replacement with rollback backup
            target_path = Path(reg.path)
            backup_path = target_path.parent / f"backup_{shard_id}_{int(time.time())}"

            was_loaded = shard_id in self._loaded
            if was_loaded:
                old_handle = self._loaded.pop(shard_id)
                self.shard_store.close(old_handle)

            try:
                if target_path.exists():
                    target_path.rename(backup_path)
                temp_unpack.rename(target_path)

                # Reload new active shard
                new_handle = self.shard_store.load(str(target_path))
                self._loaded[shard_id] = new_handle
                reg.installed_version = candidate.manifest_version
                reg.state = ShardLifecycleState.LOADED

                # Cleanup backup
                if backup_path.exists():
                    shutil.rmtree(backup_path, ignore_errors=True)

                self._emit_event(
                    "shard_activated",
                    shard_id=shard_id,
                    snapshot_id=candidate.snapshot_id,
                    installed_version=candidate.manifest_version,
                )
                self._emit_budget_update()

                return ActivationReceipt(
                    snapshot_id=candidate.snapshot_id,
                    shard_id=shard_id,
                    success=True,
                    installed_version=candidate.manifest_version,
                )

            except Exception as e:
                logger.error("Activation failed during switch; rolling back to previous base: %s", e)
                # Rollback!
                if target_path.exists():
                    shutil.rmtree(target_path, ignore_errors=True)
                if backup_path.exists():
                    backup_path.rename(target_path)
                    try:
                        self._loaded[shard_id] = self.shard_store.load(str(target_path))
                        reg.state = ShardLifecycleState.LOADED
                    except Exception:
                        reg.state = ShardLifecycleState.FAILED

                return ActivationReceipt(
                    snapshot_id=candidate.snapshot_id,
                    shard_id=shard_id,
                    success=False,
                    error=f"Activation error: {e}",
                    reverted_to_previous=True,
                )

    # -------------------------------------------------------------------------
    # Telemetry, Status & Shutdown
    # -------------------------------------------------------------------------

    def status(self) -> Dict[str, Any]:
        with self._lock:
            snapshot = self.resource_monitor.snapshot()
            return {
                "hard_memory_limit_bytes": self.hard_memory_limit_bytes,
                "soft_budget_bytes": self.soft_budget_bytes,
                "usage_bytes": self._usage_from_snapshot(snapshot),
                "rss_bytes": snapshot.rss_bytes,
                "working_set_bytes": snapshot.working_set_bytes,
                "memory_pressure": snapshot.memory_pressure,
                "memory_events": dict(snapshot.memory_events),
                "cpu_percent": snapshot.cpu_percent,
                "loaded": list(self._loaded),
                "pinned": [sid for sid, reg in self._registrations.items() if reg.pinned],
                "active_operations": {sid: reg.active_operations for sid, reg in self._registrations.items() if reg.active_operations > 0},
                "queue_depth": self._current_queue_depth,
                "projected_records_count": len(self._projected_versions),
            }

    def close_all(self) -> None:
        with self._lock:
            self._accepting_work = False
            # Wait for in-flight operations to drain (bounded wait)
            drain_start = time.time()
            while any(reg.active_operations > 0 for reg in self._registrations.values()):
                if time.time() - drain_start > 3.0:
                    logger.warning("Timed out waiting for active operations to drain; forcing shutdown")
                    break
                self._queue_condition.wait(timeout=0.1)

            for shard_id in list(self._loaded):
                try:
                    self._close(shard_id, reason="shutdown")
                except Exception as e:
                    logger.warning("Error closing shard %s on shutdown: %s", shard_id, e)

    # -------------------------------------------------------------------------
    # Internal Helpers
    # -------------------------------------------------------------------------

    def _estimate_cost(self, registration: ShardRegistration) -> int:
        if registration.estimated_cost_bytes is not None:
            return registration.estimated_cost_bytes
        return max(self._observed_costs[-5:], default=self.default_shard_cost_bytes)

    def _usage_bytes(self) -> int:
        return self._usage_from_snapshot(self.resource_monitor.snapshot())

    @staticmethod
    def _usage_from_snapshot(snapshot) -> int:
        return snapshot.memory_current_bytes if snapshot.memory_current_bytes is not None else snapshot.rss_bytes

    def _registration(self, shard_id: str) -> ShardRegistration:
        try:
            return self._registrations[shard_id]
        except KeyError as exc:
            raise KeyError(f"Unknown shard: {shard_id}") from exc

    def _emit_event(self, event_type: str, **payload: Any) -> None:
        if hasattr(self.events, "emit"):
            try:
                # Support both EventBus and EventEmitter
                if isinstance(self.events, EventBus):
                    self.events.emit(event_type, component="engine", payload=payload)
                else:
                    self.events.emit(event_type, **payload)
            except Exception as e:
                logger.warning("Failed emitting engine event %s: %s", event_type, e)

    def _emit_budget_update(self) -> None:
        self._emit_event("budget_update", **self.status())
