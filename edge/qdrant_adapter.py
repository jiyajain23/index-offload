"""Adapter for the qdrant-edge-py 0.8.0 API validated by this repository."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger("lifeline.edge.adapter")


class QdrantEdgeShardStore:
    """Loads, closes, queries, and updates ``EdgeShard`` without leaking SDK implementation upward."""

    def __init__(
        self,
        config: Any,
        vector_name: str = "embedding",
        sparse_vector_name: str = "text_sparse",
    ):
        self.config = config
        self.vector_name = vector_name
        self.sparse_vector_name = sparse_vector_name

    def create(self, path: Union[str, Path]) -> Any:
        from qdrant_edge import EdgeShard
        path_str = str(path)
        Path(path_str).mkdir(parents=True, exist_ok=True)
        return EdgeShard.create(path_str, self.config)

    def load(self, path: Union[str, Path]) -> Any:
        from qdrant_edge import EdgeShard
        return EdgeShard.load(str(path), self.config)

    def close(self, shard: Any) -> None:
        try:
            shard.close()
        except Exception as e:
            logger.warning("Error closing shard: %s", e)

    def search(
        self,
        shard: Any,
        query_vector: Any,
        k: int,
        sparse_indices: Optional[List[int]] = None,
        sparse_values: Optional[List[float]] = None,
        with_payload: bool = True,
    ) -> List[Any]:
        """Execute dense, sparse, or hybrid retrieval."""
        from qdrant_edge import Fusion, Prefetch, Query, QueryRequest, SparseVector

        has_dense = query_vector is not None
        has_sparse = bool(sparse_indices and sparse_values and len(sparse_indices) == len(sparse_values))

        if has_dense and has_sparse:
            # Hybrid search using Reciprocal Rank Fusion (RRF)
            dense_list = query_vector.tolist() if hasattr(query_vector, "tolist") else list(query_vector)
            sv = SparseVector(indices=sparse_indices, values=sparse_values)
            req = QueryRequest(
                prefetches=[
                    Prefetch(query=Query.Nearest(dense_list, using=self.vector_name), limit=max(k * 2, 10)),
                    Prefetch(query=Query.Nearest(sv, using=self.sparse_vector_name), limit=max(k * 2, 10)),
                ],
                query=Fusion.Rrf(60),
                limit=k,
                with_payload=with_payload,
            )
            return list(shard.query(req))

        elif has_sparse:
            sv = SparseVector(indices=sparse_indices, values=sparse_values)
            req = QueryRequest(
                query=Query.Nearest(sv, using=self.sparse_vector_name),
                limit=k,
                with_payload=with_payload,
            )
            return list(shard.query(req))

        elif has_dense:
            dense_list = query_vector.tolist() if hasattr(query_vector, "tolist") else list(query_vector)
            req = QueryRequest(
                query=Query.Nearest(dense_list, using=self.vector_name),
                limit=k,
                with_payload=with_payload,
            )
            return list(shard.query(req))

        return []

    def upsert_points(self, shard: Any, points: List[Any]) -> Any:
        from qdrant_edge import UpdateOperation
        return shard.update(UpdateOperation.upsert_points(points))

    def delete_points(self, shard: Any, point_ids: List[Any]) -> Any:
        from qdrant_edge import UpdateOperation
        return shard.update(UpdateOperation.delete_points(point_ids))

    def snapshot_manifest(self, shard: Any) -> Dict[str, Any]:
        return shard.snapshot_manifest()

    def unpack_snapshot(self, snapshot_path: Union[str, Path], target_path: Union[str, Path]) -> None:
        from qdrant_edge import EdgeShard
        Path(target_path).mkdir(parents=True, exist_ok=True)
        EdgeShard.unpack_snapshot(str(snapshot_path), str(target_path))

    def optimize(self, shard: Any) -> None:
        """Explicit maintenance optimization. NOT called during normal ingestion."""
        shard.optimize()
