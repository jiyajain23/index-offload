import os
import time
import psutil

from qdrant_edge import (
    EdgeShard,
    EdgeConfig,
    EdgeVectorParams,
    Distance,
)


process = psutil.Process(os.getpid())


def rss_mb():
    return process.memory_info().rss / (1024 * 1024)


config = EdgeConfig(
    vectors={
        "embedding": EdgeVectorParams(
            size=384,
            distance=Distance.Cosine,
        )
    }
)


print(f"Initial RSS: {rss_mb():.1f} MB")

print("\nLoading shard 1...")
shard1 = EdgeShard.load("./edge_data/shard1", config)
print(f"RSS after shard 1: {rss_mb():.1f} MB")

print("\nLoading shard 2...")
shard2 = EdgeShard.load("./edge_data/shard2", config)
print(f"RSS after shard 2: {rss_mb():.1f} MB")

print("\nKeeping both shards loaded for 5 seconds...")
time.sleep(5)

print("\nClosing shard 2...")
shard2.close()

time.sleep(5)

print(f"RSS after closing shard 2: {rss_mb():.1f} MB")

print("\nClosing shard 1...")
shard1.close()

time.sleep(5)

print(f"Final RSS: {rss_mb():.1f} MB")