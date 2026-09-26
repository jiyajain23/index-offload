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

print("Loading existing EdgeShard...")

shard = EdgeShard.load(
    EDGE_DIR,
    config,
)

print("Loaded successfully!")

info = shard.info()

print("\nShard information:")
print(info)

shard.close()