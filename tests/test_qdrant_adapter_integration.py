import importlib.util

import pytest


pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("qdrant_edge") is None,
    reason="qdrant-edge-py is not installed in this environment",
)


def test_adapter_loads_and_searches_a_real_edge_shard(tmp_path):
    from qdrant_edge import EdgeConfig, EdgeShard, EdgeVectorParams, Distance, Point, UpdateOperation
    from edge.qdrant_adapter import QdrantEdgeShardStore

    config = EdgeConfig(vectors={"embedding": EdgeVectorParams(size=2, distance=Distance.Cosine)})
    created = EdgeShard.create(str(tmp_path), config)
    created.update(UpdateOperation.upsert_points([
        Point(id=1, vector={"embedding": [1.0, 0.0]}, payload={"label": "first"}),
    ]))
    created.close()
    store = QdrantEdgeShardStore(config)
    shard = store.load(str(tmp_path))
    results = store.search(shard, [1.0, 0.0], 1)
    store.close(shard)
    assert len(results) == 1
