"""Adapter for the qdrant-edge-py 0.8.0 API validated by this repository."""

from __future__ import annotations

from typing import Any


class QdrantEdgeShardStore:
    """Loads, closes, and queries ``EdgeShard`` without leaking its API upward."""

    def __init__(self, config: Any, vector_name: str = "embedding"):
        self.config = config
        self.vector_name = vector_name

    def load(self, path: str) -> Any:
        from qdrant_edge import EdgeShard

        return EdgeShard.load(path, self.config)

    def close(self, shard: Any) -> None:
        shard.close()

    def search(self, shard: Any, query_vector: Any, k: int) -> list[Any]:
        from qdrant_edge import Query, QueryRequest

        vector = query_vector.tolist() if hasattr(query_vector, "tolist") else list(query_vector)
        return list(shard.query(QueryRequest(
            query=Query.Nearest(vector, using=self.vector_name), limit=k
        )))
