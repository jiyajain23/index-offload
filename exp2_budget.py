"""
exp2_budget.py - Experiment 2: naive vs managed shard loading under a hard
container memory limit.

Modes:
  make     create N_SHARDS shards on disk (run WITHOUT a memory limit)
  naive    load every shard, no management  -> expect the container to be OOM-killed
  managed  minimal ShardManager (pinned + LRU eviction) -> expect it to survive

Usage:  python exp2_budget.py {make|naive|managed} [--budget-mb N]
"""
import argparse
import csv
import gc
import os
import time
from collections import OrderedDict

import numpy as np

from qdrant_edge import (
    EdgeShard,
    EdgeConfig,
    EdgeVectorParams,
    Distance,
    Point,
    UpdateOperation,
    Query,
    QueryRequest,
)
DIM, N_POINTS, N_SHARDS = 384, 10_000, 20

CONFIG = EdgeConfig(
    vectors={
        "embedding": EdgeVectorParams(
            size=DIM,
            distance=Distance.Cosine,
        )
    }
)
DATA_DIR = "./edge_data"
PINNED = [0, 1]                 # shards that must never be evicted
QUERIES_PER_LOAD = 20           # warm-up queries so memory-mapped pages get touched
QUERIES_PER_STEP = 5
STEPS = 300                     # length of the simulated context stream
DEFAULT_COST_MB = 40        # conservative first guess for a shard's cost
CG = "/sys/fs/cgroup"
MB = 1024 * 1024


def shard_path(i):
    return os.path.join(DATA_DIR, f"exp2_shard{i:02d}")


# --------------------------------------------------------------------------
# ADAPTERS - paste from your working Test 1 / Test 2 scripts.
# I don't want to guess the exact qdrant-edge-py 0.8.0 signatures.
# --------------------------------------------------------------------------
def create_and_fill_shard(path, vectors):
    os.makedirs(path, exist_ok=True)

    shard = EdgeShard.create(path, CONFIG)

    points = []

    for i, vector in enumerate(vectors):
        points.append(
            Point(
                id=i,
                vector={"embedding": vector.tolist()},
                payload={"source": "exp2"},
            )
        )

    shard.update(
        UpdateOperation.upsert_points(points)
    )

    shard.optimize()
    shard.close()


def load_shard(path):
    return EdgeShard.load(path, CONFIG)


def close_shard(shard):
    shard.close()


def search_shard(shard, query_vec, k=5):
    return shard.query(
        QueryRequest(
            query=Query.Nearest(
                query_vec.tolist(),
                using="embedding",
            ),
            limit=k,
        )
    )

# --------------------------------------------------------------------------
# Memory measurement (cgroup v2, what the OOM killer actually checks)
# --------------------------------------------------------------------------
def _read(name):
    return open(f"{CG}/{name}").read().strip()


def mem():
    """Return (current, working_set, limit) in bytes.
    working_set = memory.current - inactive_file, the same way `docker stats`
    excludes reclaimable page cache."""
    current = int(_read("memory.current"))
    limit_raw = _read("memory.max")
    limit = None if limit_raw == "max" else int(limit_raw)
    stat = {}
    for line in _read("memory.stat").splitlines():
        k, v = line.split()
        stat[k] = int(v)
    return current, current - stat.get("inactive_file", 0), limit


def warm(shard, n):
    rng = np.random.default_rng()
    for _ in range(n):
        search_shard(shard, rng.random(DIM, dtype=np.float32))


class CsvLog:
    def __init__(self, path, fields):
        self.f = open(path, "w", newline="")
        self.w = csv.DictWriter(self.f, fieldnames=fields)
        self.w.writeheader()

    def row(self, **kw):
        self.w.writerow(kw)
        self.f.flush()  # flush every row so data survives an OOM kill


# --------------------------------------------------------------------------
# Mode: make
# --------------------------------------------------------------------------
def run_make():
    os.makedirs(DATA_DIR, exist_ok=True)
    for i in range(N_SHARDS):
        p = shard_path(i)
        if os.path.exists(p):
            print(f"skip {p} (exists)")
            continue
        rng = np.random.default_rng(i)
        vecs = rng.random((N_POINTS, DIM), dtype=np.float32)
        print(f"creating {p} ...", flush=True)
        create_and_fill_shard(p, vecs)
    print("done")


# --------------------------------------------------------------------------
# Mode: naive - load everything, no management
# --------------------------------------------------------------------------
def run_naive():
    log = CsvLog("naive_log.csv", ["n_loaded", "load_ms", "current_mb", "working_set_mb"])
    loaded = []
    for i in range(N_SHARDS):
        t = time.perf_counter()
        s = load_shard(shard_path(i))
        load_ms = (time.perf_counter() - t) * 1000
        warm(s, QUERIES_PER_LOAD)
        loaded.append(s)
        cur, ws, _ = mem()
        log.row(n_loaded=len(loaded), load_ms=round(load_ms, 1),
                current_mb=round(cur / MB, 1), working_set_mb=round(ws / MB, 1))
        print(f"loaded {len(loaded):2d} shards | working set {ws / MB:6.1f} MB", flush=True)
    print("naive finished WITHOUT being killed")


# --------------------------------------------------------------------------
# Mode: managed - pinned + LRU eviction against a working-set budget
# --------------------------------------------------------------------------
class BudgetExceeded(Exception):
    pass


class MiniManager:
    def __init__(self, budget_bytes, pinned):
        self.budget = budget_bytes
        self.pinned = set(pinned)
        self.active = OrderedDict()   # shard id -> shard, oldest first
        self.costs = []               # observed per-shard working-set deltas

    def _estimate(self):
        return max(self.costs[-5:]) if self.costs else DEFAULT_COST_MB * MB

    def ensure(self, i):
        """Make shard i active. Returns (loaded_now, evicted_ids, load_ms)."""
        if i in self.active:
            self.active.move_to_end(i)
            return False, [], 0.0
        evicted = []
        while mem()[0] + self._estimate() > self.budget:
            victim = next((k for k in self.active if k not in self.pinned), None)
            if victim is None:
                raise BudgetExceeded("pinned shards alone exceed the budget")
            close_shard(self.active.pop(victim))
            gc.collect()
            evicted.append(victim)
        before = mem()[0]
        t = time.perf_counter()
        s = load_shard(shard_path(i))
        load_ms = (time.perf_counter() - t) * 1000
        warm(s, QUERIES_PER_LOAD)
        delta = mem()[0] - before
        if delta > 0:
            self.costs.append(delta)
        self.active[i] = s
        return True, evicted, load_ms


def context_stream(steps, seed=7):
    """Random walk over shards with locality: stay 70%, else jump."""
    rng = np.random.default_rng(seed)
    ctx = int(rng.integers(N_SHARDS))
    for _ in range(steps):
        if rng.random() > 0.7:
            ctx = int(rng.integers(N_SHARDS))
        yield ctx


def run_managed(budget_mb):
    _, _, limit = mem()
    budget = int(budget_mb * MB) if budget_mb else int(0.75 * limit)
    print(f"budget = {budget / MB:.0f} MB (limit = {limit / MB if limit else 'none'} MB)", flush=True)
    mgr = MiniManager(budget, PINNED)
    log = CsvLog("managed_log.csv", ["step", "ctx", "loaded", "evicted", "load_ms",
                                     "search_ms", "active", "current_mb", "working_set_mb"])
    for p in PINNED:
        mgr.ensure(p)
    rng = np.random.default_rng()
    n_loads, n_evictions, peak = 0, 0, 0.0
    for step, ctx in enumerate(context_stream(STEPS)):
        loaded, evicted, load_ms = mgr.ensure(ctx)
        t = time.perf_counter()
        for _ in range(QUERIES_PER_STEP):
            search_shard(mgr.active[ctx], rng.random(DIM, dtype=np.float32))
        search_ms = (time.perf_counter() - t) * 1000 / QUERIES_PER_STEP
        cur, ws, _ = mem()
        peak = max(peak, ws / MB)
        n_loads += loaded
        n_evictions += len(evicted)
        log.row(step=step, ctx=ctx, loaded=int(loaded), evicted=len(evicted),
                load_ms=round(load_ms, 1), search_ms=round(search_ms, 2),
                active=len(mgr.active), current_mb=round(cur / MB, 1),
                working_set_mb=round(ws / MB, 1))
    print(f"managed finished: {n_loads} loads, {n_evictions} evictions, "
          f"peak working set {peak:.0f} MB, survived the whole stream")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["make", "naive", "managed"])
    ap.add_argument("--budget-mb", type=float, default=None,
                    help="managed mode: working-set budget (default 75%% of memory.max)")
    a = ap.parse_args()
    {"make": run_make, "naive": run_naive,
     "managed": lambda: run_managed(a.budget_mb)}[a.mode]()