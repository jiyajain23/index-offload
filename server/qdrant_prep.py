"""Index accepted records in a real Qdrant Server and export its shard snapshot.

This path requires a reachable QDRANT_URL. The older Edge-on-server preparer
remains a separately labelled demonstration mode for tests without Qdrant.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import time
from urllib.parse import quote
import uuid

import httpx
from qdrant_client import QdrantClient, models

from engine.shard_manager import ShardManager
from shared.contracts import SnapshotCandidate
from .store import ServerMemoryStore


class QdrantServerIndexPreparer:
    def __init__(self, store: ServerMemoryStore, output_dir: str | Path,
                 qdrant_url: str, api_key: str | None = None, dimension: int = 384,
                 max_snapshot_bytes: int = 1024 * 1024 * 1024):
        self.store = store
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.qdrant_url = qdrant_url.rstrip("/")
        self.api_key = api_key
        self.dimension = dimension
        self.max_snapshot_bytes = max_snapshot_bytes

    def prepare_snapshot(self, context_id: str,
                         manifest_version: str | None = None) -> SnapshotCandidate:
        records = self.store.get_records_for_context(context_id)
        snapshot_id = f"snap_{uuid.uuid4().hex}"
        collection = f"lifeline_{uuid.uuid4().hex}"
        client = QdrantClient(url=self.qdrant_url, api_key=self.api_key, timeout=30)
        archive = self.output_dir / f"{snapshot_id}.snapshot"
        try:
            # A new collection for each export freezes the exact inventory even
            # while the ingestion service accepts further writes in SQLite.
            client.create_collection(
                collection_name=collection,
                shard_number=1,
                vectors_config={"embedding": models.VectorParams(
                    size=self.dimension, distance=models.Distance.COSINE)},
                sparse_vectors_config={"text_sparse": models.SparseVectorParams()},
            )
            points = []
            inventory = []
            for record in records:
                dense = record.get("dense_vector") or [0.0] * self.dimension
                if len(dense) != self.dimension:
                    raise ValueError(f"Vector dimension mismatch for {record['record_id']}")
                vectors = {"embedding": dense}
                indices, values = record.get("sparse_indices"), record.get("sparse_values")
                if indices and values:
                    vectors["text_sparse"] = models.SparseVector(indices=indices, values=values)
                points.append(models.PointStruct(
                    id=ShardManager.to_qdrant_point_id(record["record_id"]),
                    vector=vectors,
                    payload={"record_id": record["record_id"], "version": record["version"],
                             "context_id": context_id, "entity_id": record["entity_id"],
                             "observation": record["observation"]},
                ))
                inventory.append({"record_id": record["record_id"], "version": record["version"]})
            for offset in range(0, len(points), 64):
                client.upsert(collection_name=collection, points=points[offset:offset + 64], wait=True)

            deadline = time.monotonic() + 90
            while True:
                info = client.get_collection(collection)
                if str(info.status).lower().endswith("green") and str(info.optimizer_status).lower() == "ok":
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Qdrant indexing did not finish for {collection}")
                time.sleep(0.5)

            headers = {"api-key": self.api_key} if self.api_key else {}
            url = (f"{self.qdrant_url}/collections/{quote(collection, safe='')}"
                   "/shards/0/snapshot")
            digest = hashlib.sha256()
            total = 0
            try:
                with httpx.stream("GET", url, headers=headers, timeout=60) as response:
                    response.raise_for_status()
                    with archive.open("wb") as output:
                        for chunk in response.iter_bytes(chunk_size=65536):
                            total += len(chunk)
                            if total > self.max_snapshot_bytes:
                                raise ValueError("Qdrant snapshot exceeds download limit")
                            output.write(chunk)
                            digest.update(chunk)
            except Exception:
                archive.unlink(missing_ok=True)
                raise

            candidate = SnapshotCandidate(
                snapshot_id=snapshot_id, target_shard_id=context_id,
                context_id=context_id, staging_path=str(archive),
                manifest_version=manifest_version or snapshot_id,
                checksum_sha256=digest.hexdigest(), archive_format="qdrant",
                record_count=len(points), included_records=inventory,
            )
            self.store.record_snapshot(snapshot_id, context_id, candidate.manifest_version,
                                       str(archive), candidate.checksum_sha256,
                                       len(points), candidate.model_dump_json())
            return candidate
        finally:
            client.close()
