from engine.events import EventEmitter
from engine.monitor import ResourceSnapshot
from engine.shard_manager import BudgetExceeded, ShardManager


class FakeMonitor:
    def __init__(self):
        self.usage = 0

    def snapshot(self):
        return ResourceSnapshot(self.usage, self.usage, self.usage, 1_000, None, {}, 0.0)


class FakeStore:
    def __init__(self, monitor, cost=100):
        self.monitor, self.cost = monitor, cost
        self.closed = []

    def load(self, path):
        self.monitor.usage += self.cost
        return {"path": path}

    def close(self, shard):
        self.monitor.usage -= self.cost
        self.closed.append(shard["path"])

    def search(self, shard, query, k):
        return [f"{shard['path']}:{query}"]


def manager(budget=250):
    monitor = FakeMonitor()
    events = EventEmitter()
    mgr = ShardManager(FakeStore(monitor), monitor, soft_budget_mb=budget / (1024 * 1024), events=events,
                       default_shard_cost_mb=100 / (1024 * 1024))
    return mgr, events


def test_lru_evicts_oldest_on_demand_but_never_pinned():
    mgr, events = manager()
    mgr.register_shard("protocols", "p", pinned=True, estimated_cost_mb=100 / (1024 * 1024))
    mgr.register_shard("zone_1", "z1", estimated_cost_mb=100 / (1024 * 1024))
    mgr.register_shard("zone_2", "z2", estimated_cost_mb=100 / (1024 * 1024))
    mgr.load_pinned()
    mgr.ensure_loaded("zone_1")
    mgr.ensure_loaded("zone_2")
    assert mgr.status()["loaded"] == ["protocols", "zone_2"]
    assert [event.type for event in events.history].count("shard_evicted") == 1


def test_search_reports_queued_and_unavailable_contexts():
    mgr, _ = manager(budget=100)
    mgr.register_shard("protocols", "p", pinned=True, estimated_cost_mb=100 / (1024 * 1024))
    mgr.register_shard("zone_1", "z1", estimated_cost_mb=100 / (1024 * 1024))
    mgr.load_pinned()
    result = mgr.search([1], context="zone_1")
    assert result["coverage"] == {"searched": ["protocols"], "queued": ["zone_1"], "unavailable_offline": []}
    assert mgr.search([1], context="zone_missing")["coverage"]["unavailable_offline"] == ["zone_missing"]


def test_pinned_shards_that_exceed_budget_fail_loudly():
    mgr, _ = manager(budget=100)
    mgr.register_shard("one", "1", pinned=True, estimated_cost_mb=100 / (1024 * 1024))
    mgr.register_shard("two", "2", pinned=True, estimated_cost_mb=100 / (1024 * 1024))
    mgr.ensure_loaded("one")
    try:
        mgr.ensure_loaded("two")
    except BudgetExceeded:
        pass
    else:
        raise AssertionError("expected a pinned-budget failure")


def test_soft_budget_can_be_updated_without_changing_hard_limit():
    mgr, _ = manager(budget=250)
    mgr.set_soft_budget_bytes(125)
    assert mgr.status()["soft_budget_bytes"] == 125
    assert mgr.status()["hard_memory_limit_bytes"] == 1_000
