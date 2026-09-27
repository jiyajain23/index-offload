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

EDGE_DIR = "./indexing_test"

Path(EDGE_DIR).mkdir(parents=True, exist_ok=True)

config = EdgeConfig(
    vectors={
        "embedding": EdgeVectorParams(
            size=4,
            distance=Distance.Cosine,
        )
    },
    optimizers=EdgeOptimizersConfig(
        indexing_threshold=1,
    ),
)

shard = EdgeShard.create(
    EDGE_DIR,
    config,
)

N = 10_000

print(f"Creating {N:,} vectors...")

vectors = np.random.rand(N, 4).astype(np.float32)

points = [
    Point(
        id=i,
        vector={"embedding": vectors[i].tolist()},
    )
    for i in range(N)
]

print("Inserting...")

shard.update(
    UpdateOperation.upsert_points(points)
)

print("\nAFTER INSERT:")
print(shard.info())

print("\nRunning optimize...")

result = shard.optimize()

print("Optimize result:", result)

print("\nAFTER OPTIMIZE:")
print(shard.info())

shard.close()