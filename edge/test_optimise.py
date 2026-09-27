# edge/test_optimize.py

from qdrant_edge import (
    EdgeShard,
    EdgeConfig,
    EdgeVectorParams,
    Distance,
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

print("INFO:")
print(shard.info())

print("\nCalling optimize()...")
result = shard.optimize()

print("Result:", result)

print("\nINFO after optimize:")
print(shard.info())

print("\nShard methods:")
print([x for x in dir(shard) if not x.startswith("_")])

shard.close()