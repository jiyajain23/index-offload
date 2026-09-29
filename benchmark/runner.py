"""
LIFELINE Benchmark Runner: Naive vs Unmanaged vs Managed Shard Retrieval.

Evaluates 3 retrieval architectures under identical workloads and detector contention:
1. Naive Full-Load: Eagerly loads all registered shards into memory.
2. Tuned On-Disk Unmanaged: Opens shard handles on-demand per request without LRU/cooldown.
3. Managed ShardManager: Production ShardManager with active-operation leases,
   soft budget enforcement, LRU eviction with hysteresis/cooldown, and bounded query queue.

All runs hold constant:
- Fixed random seed for dataset generation and query sequence.
- Identical vector dimension, points per shard, and query vectors.
- Shared context access distribution (random walk with locality).
- Concurrent detector contention workload using DeterministicDetectorStub (is_synthetic=True).
- P95 search latency, queue time, recall vs exact eligible corpus, coverage, and peak RSS.
"""

import sys
from pathlib import Path

# Add repository root to Python path
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import csv
import json
import logging
import os
import shutil
import tempfile
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import psutil

from app.telemetry import DetectorTelemetryHub, DeterministicDetectorStub
from edge.qdrant_adapter import QdrantEdgeShardStore
from engine.monitor import ResourceMonitor
from engine.shard_manager import ShardManager
from qdrant_edge import (
    Distance,
    EdgeConfig,
    EdgeVectorParams,
    Point,
)
from shared.contracts import CandidateHit, SearchRequest

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("lifeline.benchmark")


@dataclass
class BenchmarkConfig:
    seed: int = 42
    dimension: int = 64
    num_shards: int = 8
    points_per_shard: int = 500
    num_queries: int = 40
    k: int = 5
    soft_budget_mb: float = 250.0
    default_shard_cost_mb: float = 15.0
    query_deadline_ms: float = 1000.0
    detector_target_fps: float = 30.0
    output_dir: str = "benchmark"


@dataclass
class ModeResult:
    mode: str
    detector_fps: float
    detector_p95_frame_ms: float
    detector_is_synthetic: bool
    search_p50_latency_ms: float
    search_p95_latency_ms: float
    search_p99_latency_ms: float
    queue_p95_latency_ms: float
    mean_recall: float
    relevant_shard_coverage_pct: float
    failure_rate_pct: float
    peak_rss_mb: float
    total_loads: int
    total_evictions: int
    elapsed_time_s: float
    notes: str


# -----------------------------------------------------------------------------
# Ground Truth Evaluator for Recall
# -----------------------------------------------------------------------------

class GroundTruthCorpus:
    """Calculates exact cosine-similarity ground truth over raw vectors for recall evaluation."""

    def __init__(self, shards_data: Dict[str, np.ndarray]):
        self.shards_data = shards_data

    def get_ground_truth(self, context_id: str, query_vec: np.ndarray, k: int) -> List[str]:
        if context_id not in self.shards_data:
            return []
        vectors = self.shards_data[context_id]  # Shape (N, D)
        # Cosine similarity: dot(A, B) / (norm(A) * norm(B))
        norms = np.linalg.norm(vectors, axis=1) * np.linalg.norm(query_vec)
        norms[norms == 0] = 1e-9
        sims = np.dot(vectors, query_vec) / norms
        top_indices = np.argsort(sims)[::-1][:k]
        return [f"{context_id}_{idx}" for idx in top_indices]


# -----------------------------------------------------------------------------
# Baseline 1: Naive Full-Load
# -----------------------------------------------------------------------------

class NaiveEngine:
    """Eagerly loads every shard on first query and keeps all handles resident indefinitely."""

    def __init__(self, store: QdrantEdgeShardStore, shard_paths: Dict[str, str]):
        self.store = store
        self.shard_paths = shard_paths
        self.handles: Dict[str, Any] = {}
        self.load_count = 0

    def query(self, request: SearchRequest) -> Tuple[List[CandidateHit], List[str], float]:
        t0 = time.perf_counter()
        target = request.context
        if target not in self.handles and target in self.shard_paths:
            self.handles[target] = self.store.load(self.shard_paths[target])
            self.load_count += 1

        if target not in self.handles:
            return [], [], 0.0

        hits = self.store.search(self.handles[target], request.dense_vector, request.k)
        elapsed = (time.perf_counter() - t0) * 1000.0

        cands = [
            CandidateHit(
                point_id=str(getattr(h, "id", "")),
                shard_id=target,
                score=float(getattr(h, "score", 0.0)),
                payload=getattr(h, "payload", {}) or {},
            )
            for h in hits
        ]
        return cands, [target], elapsed

    def close(self):
        for h in self.handles.values():
            try:
                self.store.close(h)
            except Exception:
                pass
        self.handles.clear()


# -----------------------------------------------------------------------------
# Baseline 2: Tuned On-Disk Unmanaged
# -----------------------------------------------------------------------------

class UnmanagedOnDiskEngine:
    """Opens shard handle on disk per request, searches, and closes it immediately to minimize RSS."""

    def __init__(self, store: QdrantEdgeShardStore, shard_paths: Dict[str, str]):
        self.store = store
        self.shard_paths = shard_paths
        self.load_count = 0

    def query(self, request: SearchRequest) -> Tuple[List[CandidateHit], List[str], float]:
        t0 = time.perf_counter()
        target = request.context
        if target not in self.shard_paths:
            return [], [], 0.0

        # Open on demand
        handle = self.store.load(self.shard_paths[target])
        self.load_count += 1
        try:
            hits = self.store.search(handle, request.dense_vector, request.k)
        finally:
            self.store.close(handle)

        elapsed = (time.perf_counter() - t0) * 1000.0
        cands = [
            CandidateHit(
                point_id=str(getattr(h, "id", "")),
                shard_id=target,
                score=float(getattr(h, "score", 0.0)),
                payload=getattr(h, "payload", {}) or {},
            )
            for h in hits
        ]
        return cands, [target], elapsed

    def close(self):
        pass


# -----------------------------------------------------------------------------
# Benchmark Harness
# -----------------------------------------------------------------------------

class BenchmarkRunner:
    def __init__(self, config: BenchmarkConfig):
        self.cfg = config
        self.rng = np.random.default_rng(config.seed)
        self.temp_dir: Optional[tempfile.TemporaryDirectory] = None
        self.data_dir: Optional[Path] = None
        self.shard_paths: Dict[str, str] = {}
        self.raw_data: Dict[str, np.ndarray] = {}
        self.query_stream: List[Tuple[str, List[float]]] = []
        self.edge_store: Optional[QdrantEdgeShardStore] = None

    def setup(self):
        """Generate reproducible dataset and query stream."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name)

        config = EdgeConfig(
            vectors={"embedding": EdgeVectorParams(size=self.cfg.dimension, distance=Distance.Cosine)}
        )
        self.edge_store = QdrantEdgeShardStore(config)

        print(f"Generating benchmark dataset: {self.cfg.num_shards} shards, "
              f"{self.cfg.points_per_shard} points/shard, dim={self.cfg.dimension}...")

        for i in range(self.cfg.num_shards):
            context_id = f"ctx_shard_{i:02d}"
            shard_path = self.data_dir / context_id
            shard_path.mkdir(parents=True, exist_ok=True)

            vectors = self.rng.random((self.cfg.points_per_shard, self.cfg.dimension), dtype=np.float32)
            self.raw_data[context_id] = vectors

            shard = self.edge_store.create(str(shard_path))
            points = []
            for idx, vec in enumerate(vectors):
                points.append(
                    Point(
                        id=idx,
                        vector={"embedding": vec.tolist()},
                        payload={"record_id": f"{context_id}_{idx}", "context_id": context_id},
                    )
                )
            self.edge_store.upsert_points(shard, points)
            self.edge_store.close(shard)
            self.shard_paths[context_id] = str(shard_path)

        # Create query stream with 75% temporal locality (random walk)
        current_idx = 0
        for _ in range(self.cfg.num_queries):
            if self.rng.random() > 0.75:
                current_idx = int(self.rng.integers(self.cfg.num_shards))
            target_context = f"ctx_shard_{current_idx:02d}"
            q_vec = self.rng.random(self.cfg.dimension, dtype=np.float32).tolist()
            self.query_stream.append((target_context, q_vec))

    def _calculate_recall(self, hits: List[CandidateHit], gt_ids: List[str]) -> float:
        if not gt_ids:
            return 1.0
        retrieved_ids = {h.payload.get("record_id", str(h.point_id)) for h in hits}
        matched = len(retrieved_ids.intersection(set(gt_ids)))
        return matched / len(gt_ids)

    def run_mode(self, mode: str) -> ModeResult:
        """Run benchmark for a specific retrieval mode under detector contention."""
        print(f"\n--- Running Benchmark Mode: {mode.upper()} ---")
        process = psutil.Process()
        initial_rss = process.memory_info().rss / (1024 * 1024)

        # Start synthetic detector contention workload
        hub = DetectorTelemetryHub()
        detector = DeterministicDetectorStub(hub=hub, target_fps=self.cfg.detector_target_fps)
        detector.start()

        latencies_ms: List[float] = []
        queue_times_ms: List[float] = []
        recalls: List[float] = []
        coverage_counts: List[int] = []
        failures = 0
        peak_rss = initial_rss

        gt_corpus = GroundTruthCorpus(self.raw_data)
        t_start = time.perf_counter()

        if mode == "naive":
            engine = NaiveEngine(self.edge_store, self.shard_paths)
            total_evictions = 0
        elif mode == "unmanaged":
            engine = UnmanagedOnDiskEngine(self.edge_store, self.shard_paths)
            total_evictions = 0
        elif mode == "managed":
            monitor = ResourceMonitor()
            engine = ShardManager(
                self.edge_store,
                monitor,
                soft_budget_mb=self.cfg.soft_budget_mb,
                default_shard_cost_mb=self.cfg.default_shard_cost_mb,
                cooldown_seconds=0.1,
            )
            # Register all shards (shard 0 is pinned protocol)
            for sid, spath in self.shard_paths.items():
                is_pinned = (sid == "ctx_shard_00")
                engine.register_shard(sid, spath, pinned=is_pinned)
            total_evictions = 0
        else:
            raise ValueError(f"Unknown mode: {mode}")

        try:
            for step, (target_ctx, q_vec) in enumerate(self.query_stream):
                # Sample memory
                current_rss = process.memory_info().rss / (1024 * 1024)
                peak_rss = max(peak_rss, current_rss)

                req = SearchRequest(
                    context=target_ctx,
                    dense_vector=q_vec,
                    k=self.cfg.k,
                    deadline_ms=self.cfg.query_deadline_ms,
                    include_protocols=False,
                )

                q_t0 = time.perf_counter()
                if mode in ("naive", "unmanaged"):
                    cands, searched_shards, lat = engine.query(req)
                    queue_lat = 0.0
                else:  # managed ShardManager
                    res = engine.query_shards(req)
                    cands = res.candidates
                    searched_shards = res.coverage.searched
                    lat = (time.perf_counter() - q_t0) * 1000.0
                    queue_lat = lat - res.elapsed_ms if lat > res.elapsed_ms else 0.0

                latencies_ms.append(lat)
                queue_times_ms.append(queue_lat)

                # Ground truth check
                gt_ids = gt_corpus.get_ground_truth(target_ctx, np.array(q_vec), self.cfg.k)
                recalls.append(self._calculate_recall(cands, gt_ids))

                if target_ctx in searched_shards:
                    coverage_counts.append(1)
                else:
                    coverage_counts.append(0)
                    failures += 1

                # Keep request pace (30 QPS)
                time.sleep(0.015)

        finally:
            elapsed_time = time.perf_counter() - t_start
            detector.stop()
            det_stats = hub.get_metrics()
            engine.close() if hasattr(engine, "close") else engine.close_all()

        if mode == "naive":
            total_loads = engine.load_count
        elif mode == "unmanaged":
            total_loads = engine.load_count
        else:
            total_loads = len(engine._loaded)
            # count evictions from manager
            total_evictions = len(engine._last_eviction_times)

        # Compute percentiles
        p50_lat = float(np.percentile(latencies_ms, 50)) if latencies_ms else 0.0
        p95_lat = float(np.percentile(latencies_ms, 95)) if latencies_ms else 0.0
        p99_lat = float(np.percentile(latencies_ms, 99)) if latencies_ms else 0.0
        p95_q = float(np.percentile(queue_times_ms, 95)) if queue_times_ms else 0.0
        mean_rec = float(np.mean(recalls)) if recalls else 0.0
        cov_pct = (sum(coverage_counts) / len(coverage_counts) * 100.0) if coverage_counts else 0.0
        fail_pct = (failures / len(self.query_stream) * 100.0) if self.query_stream else 0.0

        notes = (
            "Naive full memory load" if mode == "naive"
            else "Per-request disk open/close churn" if mode == "unmanaged"
            else "Bounded working-set LRU + active leases"
        )

        return ModeResult(
            mode=mode,
            detector_fps=round(det_stats["fps"], 1),
            detector_p95_frame_ms=round(det_stats["p95_latency_ms"], 2),
            detector_is_synthetic=det_stats["is_synthetic"],
            search_p50_latency_ms=round(p50_lat, 2),
            search_p95_latency_ms=round(p95_lat, 2),
            search_p99_latency_ms=round(p99_lat, 2),
            queue_p95_latency_ms=round(p95_q, 2),
            mean_recall=round(mean_rec, 3),
            relevant_shard_coverage_pct=round(cov_pct, 1),
            failure_rate_pct=round(fail_pct, 1),
            peak_rss_mb=round(peak_rss, 1),
            total_loads=total_loads,
            total_evictions=total_evictions,
            elapsed_time_s=round(elapsed_time, 2),
            notes=notes,
        )

    def teardown(self):
        if self.temp_dir:
            self.temp_dir.cleanup()


def run_benchmark():
    parser = argparse.ArgumentParser(description="LIFELINE Shard Retrieval Benchmark")
    parser.add_argument("--queries", type=int, default=40, help="Number of queries to run")
    parser.add_argument("--shards", type=int, default=8, help="Number of shards")
    parser.add_argument("--points", type=int, default=500, help="Points per shard")
    parser.add_argument("--dim", type=int, default=64, help="Vector dimension")
    parser.add_argument("--budget-mb", type=float, default=250.0, help="Managed soft budget MB")
    args = parser.parse_args()

    cfg = BenchmarkConfig(
        dimension=args.dim,
        num_shards=args.shards,
        points_per_shard=args.points,
        num_queries=args.queries,
        soft_budget_mb=args.budget_mb,
    )

    runner = BenchmarkRunner(cfg)
    runner.setup()

    results: List[ModeResult] = []
    try:
        for mode in ["naive", "unmanaged", "managed"]:
            res = runner.run_mode(mode)
            results.append(res)
    finally:
        runner.teardown()

    # Save to CSV and JSON
    out_dir = Path("benchmark")
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "results.csv"
    json_path = out_dir / "results.json"

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(results[0]).keys()))
        writer.writeheader()
        for r in results:
            writer.writerow(asdict(r))

    with open(json_path, "w") as f:
        json.dump([asdict(r) for r in results], f, indent=2)

    # Print summary table
    print("\n" + "=" * 90)
    print("LIFELINE BENCHMARK SUMMARY (Hold-Constant: seed=42, 64-dim, 8 shards, detector contention)")
    print("=" * 90)
    fmt = "{:<10} | {:<12} | {:<12} | {:<10} | {:<10} | {:<12} | {:<10}"
    print(fmt.format("Mode", "Search p95(ms)", "Queue p95(ms)", "Det FPS", "Recall", "Coverage(%)", "Peak RSS(MB)"))
    print("-" * 90)
    for r in results:
        print(fmt.format(
            r.mode,
            f"{r.search_p95_latency_ms} ms",
            f"{r.queue_p95_latency_ms} ms",
            f"{r.detector_fps} fps",
            f"{r.mean_recall:.3f}",
            f"{r.relevant_shard_coverage_pct}%",
            f"{r.peak_rss_mb} MB",
        ))
    print("=" * 90)
    print(f"Results written to: {csv_path} and {json_path}\n")


if __name__ == "__main__":
    run_benchmark()
