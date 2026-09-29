"""Server-side index preparation and snapshot export for edge devices."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
import time
from typing import Any, Dict, List, Optional
import uuid

from qdrant_edge import (
    Distance,
    EdgeConfig,
    EdgeSparseVectorParams,
    EdgeVectorParams,
    Point,
    SparseVector,
    UpdateOperation,
)

from edge.qdrant_adapter import QdrantEdgeShardStore
from engine.shard_manager import ShardManager
from shared.contracts import SnapshotCandidate
from .store import ServerMemoryStore

logger = logging.getLogger("lifeline.server.prep")


class ServerIndexPreparer:
    """Prepares optimized indexed shards and snapshot archives outside the edge budget."""

    def __init__(
        self,
        store: ServerMemoryStore,
        output_dir: str | Path,
        dimension: int = 384,
    ):
        self.store = store
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.dimension = dimension

    def prepare_snapshot(self, context_id: str, manifest_version: Optional[str] = None) -> SnapshotCandidate:
        """Compile all records for context into an optimized EdgeShard snapshot archive."""
        records = self.store.get_records_for_context(context_id)
        snapshot_id = f"snap_{context_id}_{uuid.uuid4().hex[:8]}"
        m_version = manifest_version or f"v_{int(time.time())}"

        with tempfile.TemporaryDirectory() as td:
            shard_work_dir = Path(td) / "shard"
            shard_work_dir.mkdir(parents=True, exist_ok=True)

            config = EdgeConfig(
                vectors={"embedding": EdgeVectorParams(size=self.dimension, distance=Distance.Cosine)},
                sparse_vectors={"text_sparse": EdgeSparseVectorParams()},
            )
            store_adapter = QdrantEdgeShardStore(config)
            shard = store_adapter.create(str(shard_work_dir))

            # Upsert points
            points = []
            included_inventory = []
            for r in records:
                rec_id = r["record_id"]
                ver = r["version"]
                dense = r.get("dense_vector") or [0.0] * self.dimension
                sparse_idx = r.get("sparse_indices")
                sparse_val = r.get("sparse_values")

                vec_dict: Dict[str, Any] = {"embedding": dense}
                if sparse_idx and sparse_val:
                    vec_dict["text_sparse"] = SparseVector(indices=sparse_idx, values=sparse_val)

                pt_id = ShardManager.to_qdrant_point_id(rec_id)
                points.append(
                    Point(
                        id=pt_id,
                        vector=vec_dict,
                        payload={
                            "record_id": rec_id,
                            "version": ver,
                            "context_id": context_id,
                            "observation": r.get("observation", {}),
                        },
                    )
                )
                included_inventory.append({"record_id": rec_id, "version": ver})

            if points:
                store_adapter.upsert_points(shard, points)

            # Server optimizes the index (outside edge limits)
            store_adapter.optimize(shard)
            store_adapter.close(shard)

            # Package into tar archive
            archive_filename = f"{snapshot_id}.tar"
            archive_path = self.output_dir / archive_filename
            with tarfile.open(archive_path, "w") as tar:
                for item in os.listdir(shard_work_dir):
                    tar.add(shard_work_dir / item, arcname=item)

            # Compute SHA256 checksum
            h = hashlib.sha256()
            with open(archive_path, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    h.update(chunk)
            checksum = h.hexdigest()

            # Create candidate object
            candidate = SnapshotCandidate(
                snapshot_id=snapshot_id,
                target_shard_id=context_id,
                context_id=context_id,
                staging_path=str(archive_path),
                manifest_version=m_version,
                checksum_sha256=checksum,
                record_count=len(points),
                included_records=included_inventory,
            )

            # Record in server catalog
            self.store.record_snapshot(
                snapshot_id=snapshot_id,
                context_id=context_id,
                manifest_version=m_version,
                archive_path=str(archive_path),
                checksum_sha256=checksum,
                record_count=len(points),
                manifest_json=candidate.model_dump_json(),
            )

            logger.info(
                "Prepared server snapshot %s for context %s (%d records, sha=%s)",
                snapshot_id,
                context_id,
                len(points),
                checksum[:12],
            )
            return candidate
