import numpy as np

from qdrant_edge import (
    EdgeShard,
    EdgeConfig,
    EdgeVectorParams,
    EdgeOptimizersConfig,
    Distance,
    Point,
    UpdateOperation,
)

EDGE_DIR = "./threshold_test"

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

shard = EdgeShard.load(EDGE_DIR, config)

print("BEFORE INSERT:")
print(shard.info())

points = []

for i in range(10):
    vector = np.random.rand(4).astype(np.float32).tolist()

    points.append(
        Point(
            id=i,
            vector={"embedding": vector},
            payload={"test": True},
        )
    )

print("\nInserting 10 points...")

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