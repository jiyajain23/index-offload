"""Deterministic, budgeted lifecycle management for local vector shards."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import gc
from typing import Any

from .events import EventEmitter
from .monitor import ResourceMonitor


MB = 1024 * 1024


class BudgetExceeded(RuntimeError):
    """No eligible on-demand shard can be evicted to make room."""


@dataclass(frozen=True)
class ShardRegistration:
    shard_id: str
    path: str
    pinned: bool = False
    estimated_cost_bytes: int | None = None


class ShardManager:
    """Keeps pinned shards resident and evicts on-demand shards by LRU order."""

    def __init__(self, shard_store: Any, resource_monitor: ResourceMonitor,
                 soft_budget_mb: float | None = None, events: EventEmitter | None = None,
                 default_shard_cost_mb: float = 40):
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
        self._registrations: dict[str, ShardRegistration] = {}
        self._loaded: OrderedDict[str, Any] = OrderedDict()
        self._observed_costs: list[int] = []

    def register_shard(self, shard_id: str, path: str, pinned: bool = False,
                       estimated_cost_mb: float | None = None) -> None:
        if shard_id in self._registrations:
            raise ValueError(f"Shard already registered: {shard_id}")
        self._registrations[shard_id] = ShardRegistration(
            shard_id, path, pinned,
            int(estimated_cost_mb * MB) if estimated_cost_mb is not None else None,
        )

    def load_pinned(self) -> None:
        for shard_id, registration in self._registrations.items():
            if registration.pinned:
                self.ensure_loaded(shard_id)

    def set_soft_budget_bytes(self, soft_budget_bytes: int) -> None:
        """Update the application-level budget; the cgroup hard limit is unchanged."""
        if soft_budget_bytes <= 0:
            raise ValueError("soft_budget_bytes must be positive")
        self.soft_budget_bytes = soft_budget_bytes
        self._emit_budget_update()

    def ensure_loaded(self, shard_id: str) -> bool:
        registration = self._registration(shard_id)
        if shard_id in self._loaded:
            self._loaded.move_to_end(shard_id)
            return True

        estimate = self._estimate_cost(registration)
        evicted = self._evict_until_room(estimate)
        before = self._usage_bytes()
        try:
            shard = self.shard_store.load(registration.path)
        except Exception:
            self.events.emit("shard_load_failed", shard_id=shard_id)
            raise
        after = self._usage_bytes()
        if after > self.soft_budget_bytes:
            # An estimate can be wrong on a new shard type. Do not retain a shard
            # that proved too expensive; the caller gets explicit queued coverage.
            self.shard_store.close(shard)
            gc.collect()
            self.events.emit("shard_load_rejected", shard_id=shard_id,
                             usage_bytes=after, soft_budget_bytes=self.soft_budget_bytes)
            self._emit_budget_update()
            raise BudgetExceeded(f"Loading {shard_id} exceeded the soft memory budget")
        observed = max(0, after - before)
        if observed:
            self._observed_costs.append(observed)
        self._loaded[shard_id] = shard
        self.events.emit("shard_loaded", shard_id=shard_id, pinned=registration.pinned,
                         evicted=evicted, observed_cost_bytes=observed)
        self._emit_budget_update()
        return True

    def search(self, query_vector: Any, context: str, k: int = 5) -> dict[str, Any]:
        """Search pinned shards plus the requested context, with explicit coverage."""
        searched: list[str] = []
        queued: list[str] = []
        unavailable: list[str] = []
        target_ids = [sid for sid, reg in self._registrations.items() if reg.pinned]
        if context in self._registrations and context not in target_ids:
            target_ids.append(context)
        elif context not in self._registrations:
            unavailable.append(context)
        for shard_id in target_ids:
            try:
                self.ensure_loaded(shard_id)
                searched.append(shard_id)
            except BudgetExceeded:
                queued.append(shard_id)
        hits = []
        for shard_id in searched:
            for hit in self.shard_store.search(self._loaded[shard_id], query_vector, k):
                hits.append({"shard_id": shard_id, "hit": hit})
        return {"hits": hits[:k], "coverage": {
            "searched": searched, "queued": queued, "unavailable_offline": unavailable,
        }}

    def status(self) -> dict[str, Any]:
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
        }

    def close_all(self) -> None:
        for shard_id in list(self._loaded):
            self._close(shard_id, reason="shutdown")

    def _evict_until_room(self, incoming_cost: int) -> list[str]:
        evicted = []
        while self._usage_bytes() + incoming_cost > self.soft_budget_bytes:
            victim = next((sid for sid in self._loaded if not self._registrations[sid].pinned), None)
            if victim is None:
                raise BudgetExceeded("No evictable shard can create room within the soft budget")
            self._close(victim, reason="budget")
            evicted.append(victim)
        return evicted

    def _close(self, shard_id: str, reason: str) -> None:
        shard = self._loaded.pop(shard_id)
        self.shard_store.close(shard)
        gc.collect()
        self.events.emit("shard_evicted", shard_id=shard_id, reason=reason)
        self._emit_budget_update()

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

    def _emit_budget_update(self) -> None:
        self.events.emit("budget_update", **self.status())
