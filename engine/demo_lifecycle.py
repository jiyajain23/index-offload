"""Run a pinned-plus-LRU shard lifecycle against existing Qdrant Edge shards.

Example (inside the constrained Docker/WSL environment):
    python -m engine.demo_lifecycle --protocols edge_data/shard1 \
      --zone-01 edge_data/shard2 --zone-02 edge_data/shard3 --soft-budget-mb 410
"""

from __future__ import annotations

import argparse
import json

from qdrant_edge import Distance, EdgeConfig, EdgeVectorParams

from edge.qdrant_adapter import QdrantEdgeShardStore
from engine.events import EventEmitter
from engine.monitor import ResourceMonitor
from engine.shard_manager import MB, ShardManager


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocols", required=True)
    parser.add_argument("--zone-01", required=True)
    parser.add_argument("--zone-02", required=True)
    parser.add_argument("--soft-budget-mb", type=float, default=None)
    parser.add_argument("--dimension", type=int, default=384)
    parser.add_argument("--estimated-shard-cost-mb", type=float, default=40)
    parser.add_argument("--auto-lru-budget", action="store_true",
                        help="Demo only: calibrate a budget that forces one LRU eviction.")
    parser.add_argument("--auto-lru-margin-mb", type=float, default=8,
                        help="Extra cgroup headroom used only with --auto-lru-budget.")
    args = parser.parse_args()

    config = EdgeConfig(vectors={"embedding": EdgeVectorParams(
        size=args.dimension, distance=Distance.Cosine,
    )})
    events = EventEmitter()
    events.subscribe(lambda event: print(json.dumps({
        "event": event.type, **event.payload,
    }, default=str)))
    manager = ShardManager(
        shard_store=QdrantEdgeShardStore(config),
        resource_monitor=ResourceMonitor(),
        soft_budget_mb=args.soft_budget_mb,
        events=events,
        default_shard_cost_mb=args.estimated_shard_cost_mb,
    )
    cost = args.estimated_shard_cost_mb
    manager.register_shard("emergency_protocols", args.protocols, pinned=True,
                           estimated_cost_mb=cost)
    manager.register_shard("zone_01", args.zone_01, estimated_cost_mb=cost)
    manager.register_shard("zone_02", args.zone_02, estimated_cost_mb=cost)

    try:
        manager.load_pinned()
        if args.auto_lru_budget:
            # Keep room for exactly one estimated context shard after pinned data.
            usage = manager.status()["usage_bytes"]
            manager.set_soft_budget_bytes(
                usage + int((args.estimated_shard_cost_mb + args.auto_lru_margin_mb) * MB)
            )
        manager.ensure_loaded("zone_01")
        manager.ensure_loaded("zone_02")  # Evicts zone_01 when capacity requires it.
        print(json.dumps(manager.search([1.0] * args.dimension, "zone_01", k=5), default=str))
        print(json.dumps(manager.status(), default=str))
    finally:
        manager.close_all()


if __name__ == "__main__":
    main()
