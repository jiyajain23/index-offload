import os
import time
import psutil
import numpy as np
from pathlib import Path

from qdrant_edge import (
    EdgeShard,
    EdgeConfig,
    EdgeVectorParams,
    EdgeOptimizersConfig,
    Distance,
    Point,
    UpdateOperation,
)

N = 100_000
DIM = 384
EDGE_DIR = f"./benchmark_data/{N}"

Path(EDGE_DIR).mkdir(parents=True, exist_ok=True)

process = psutil.Process(os.getpid())


def ram_mb():
    return process.memory_info().rss / 1024 / 1024


config = EdgeConfig(
    vectors={
        "embedding": EdgeVectorParams(
            size=DIM,
            distance=Distance.Cosine,
        )
    },
    optimizers=EdgeOptimizersConfig(
        indexing_threshold=1,
    ),
)

print(f"Dataset: {N:,} vectors × {DIM} dimensions")
print(f"RAM before shard: {ram_mb():.1f} MB")

shard = EdgeShard.create(
    EDGE_DIR,
    config,
)

print(f"RAM after shard: {ram_mb():.1f} MB")

# Generate vectors
vectors = np.random.rand(N, DIM).astype(np.float32)

print(f"RAM after vector generation: {ram_mb():.1f} MB")

points = [
    Point(
        id=i,
        vector={"embedding": vectors[i].tolist()},
    )
    for i in range(N)
]

print(f"RAM after Point creation: {ram_mb():.1f} MB")

print("\nInserting...")
start = time.perf_counter()

shard.update(
    UpdateOperation.upsert_points(points)
)

insert_time = time.perf_counter() - start

print(f"Insert time: {insert_time:.2f}s")
print(f"RAM after insert: {ram_mb():.1f} MB")
print("Info:", shard.info())

print("\nOptimizing...")
start = time.perf_counter()

before = ram_mb()

result = shard.optimize()

peak = ram_mb()

optimize_time = time.perf_counter() - start

print(f"Optimize result: {result}")
print(f"Optimize time: {optimize_time:.2f}s")
print(f"RAM before optimize: {before:.1f} MB")
print(f"RAM after optimize: {peak:.1f} MB")

print("\nFinal info:")
print(shard.info())

shard.close()