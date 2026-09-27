import numpy as np

from qdrant_edge import (
    EdgeShard,
    EdgeConfig,
    EdgeVectorParams,
    Distance,
    Point,
    UpdateOperation,
)

EDGE_DIR = "./edge_data"

config = EdgeConfig(
    vectors={
        "embedding": EdgeVectorParams(
            size=4,
            distance=Distance.Cosine,
        )
    }
)

shard = EdgeShard.load(EDGE_DIR, config)

print("Before insert:")
print(shard.info())

# Create 10 random 4-dimensional vectors
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

result = shard.update(
    UpdateOperation.upsert_points(points)
)

print("Update result:")
print(result)

print("\nAfter insert:")
print(shard.info())

shard.close()