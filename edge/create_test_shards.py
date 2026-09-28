from pathlib import Path
import random

from qdrant_edge import (
    EdgeShard,
    EdgeConfig,
    EdgeVectorParams,
    Distance,
    Point,
    UpdateOperation,
)


config = EdgeConfig(
    vectors={
        "embedding": EdgeVectorParams(
            size=384,
            distance=Distance.Cosine,
        )
    }
)


def create_shard(path, seed):
    Path(path).mkdir(parents=True, exist_ok=True)

    shard = EdgeShard.create(path, config)

    random.seed(seed)

    points = []

    for i in range(10_000):
        vector = [random.random() for _ in range(384)]

        points.append(
            Point(
                id=i,
                vector={"embedding": vector},
                payload={"shard": path},
            )
        )

    print(f"Inserting 10,000 points into {path}...")
    shard.update(UpdateOperation.upsert_points(points))

    print("Optimizing...")
    print("Optimize result:", shard.optimize())

    print("Shard info:", shard.info())

    shard.close()


create_shard("./edge_data/shard1", 42)
create_shard("./edge_data/shard2", 123)