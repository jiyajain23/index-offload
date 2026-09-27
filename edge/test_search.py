from qdrant_edge import (
    EdgeShard,
    EdgeConfig,
    EdgeVectorParams,
    Distance,
    Query,         # Added
    QueryRequest,  # Added
)

EDGE_DIR = "./edge_data"

# Configuration matches your "embedding" vector key
config = EdgeConfig(
    vectors={
        "embedding": EdgeVectorParams(
            size=4,
            distance=Distance.Cosine,
        )
    }
)

shard = EdgeShard.load(EDGE_DIR, config)

print("Shard info:")
print(shard.info())

query_vector = [0.1, 0.2, 0.3, 0.4]

print("\nSearching before optimize...")

# FIX: Use shard.query() with QueryRequest and Query.Nearest
results = shard.query(
    QueryRequest(
        query=Query.Nearest(
            query_vector, 
            using="embedding"  # Matches the key defined in your config vectors
        ),
        limit=3,
    )
)

print("\nSearch results:")
for result in results:
    print(result)

shard.close()
