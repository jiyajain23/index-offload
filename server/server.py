"""FastAPI Server application for LIFELINE ingestion, deduplication, and snapshot distribution."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from .index_prep import ServerIndexPreparer
from .qdrant_prep import QdrantServerIndexPreparer
from .store import ServerMemoryStore

app = FastAPI(title="LIFELINE Server API", version="1.0.0")

# Module-level instances initialized lazily or explicitly
DEFAULT_SERVER_DIR = Path("./server_data")
SERVER_STORE: Optional[ServerMemoryStore] = None
SERVER_PREPARER: Optional[ServerIndexPreparer] = None


def init_server(data_dir: Path | str = DEFAULT_SERVER_DIR) -> None:
    global SERVER_STORE, SERVER_PREPARER
    path = Path(data_dir)
    path.mkdir(parents=True, exist_ok=True)
    SERVER_STORE = ServerMemoryStore(path / "server_memory.db")
    qdrant_url = os.environ.get("QDRANT_URL")
    if qdrant_url:
        SERVER_PREPARER = QdrantServerIndexPreparer(
            SERVER_STORE, path / "snapshots", qdrant_url,
            api_key=os.environ.get("QDRANT_API_KEY"))
    else:
        SERVER_PREPARER = ServerIndexPreparer(SERVER_STORE, output_dir=path / "snapshots")


def get_store() -> ServerMemoryStore:
    global SERVER_STORE
    if SERVER_STORE is None:
        init_server()
    return SERVER_STORE  # type: ignore


def get_preparer() -> ServerIndexPreparer:
    global SERVER_PREPARER
    if SERVER_PREPARER is None:
        init_server()
    return SERVER_PREPARER  # type: ignore


@app.get("/health")
def health():
    return {"status": "ok", "service": "lifeline-ingestion-server", "qdrant_configured": bool(os.environ.get("QDRANT_URL"))}


# Request/Response schemas
class IngestBatchRequest(BaseModel):
    items: List[Dict[str, Any]] = Field(..., description="Batch of operations to ingest")


class IngestReceipt(BaseModel):
    operation_id: str
    record_id: str
    status: str
    server_version: int
    received_at: str
    deduplicated: bool


class IngestBatchResponse(BaseModel):
    receipts: List[IngestReceipt]


class SnapshotPrepareRequest(BaseModel):
    context_id: str
    manifest_version: Optional[str] = None


@app.get("/api/v1/health")
def healthcheck():
    return {"status": "ok", "service": "lifeline-server"}


@app.post("/api/v1/ingest", response_model=IngestBatchResponse)
def ingest_batch(req: IngestBatchRequest):
    """Atomically commit incoming batch of operations to server database."""
    if not req.items:
        return IngestBatchResponse(receipts=[])

    store = get_store()
    raw_receipts = store.ingest_batch(req.items)
    return IngestBatchResponse(
        receipts=[IngestReceipt(**r) for r in raw_receipts]
    )


@app.post("/api/v1/snapshots/prepare")
def prepare_snapshot(req: SnapshotPrepareRequest):
    """Trigger server-side index preparation and export for a context."""
    preparer = get_preparer()
    candidate = preparer.prepare_snapshot(req.context_id, req.manifest_version)
    return candidate.model_dump()


@app.get("/api/v1/snapshots/{context_id}")
def get_latest_snapshot_metadata(context_id: str):
    """Get metadata for the latest prepared snapshot for context."""
    store = get_store()
    snap = store.get_latest_snapshot(context_id)
    if not snap:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No snapshot prepared for context {context_id}",
        )
    return json.loads(snap["manifest_json"])


@app.get("/api/v1/snapshots/{context_id}/download")
def download_snapshot(context_id: str):
    """Download the staged snapshot tarball for context."""
    store = get_store()
    snap = store.get_latest_snapshot(context_id)
    if not snap:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No snapshot found for context {context_id}",
        )

    archive_path = Path(snap["archive_path"])
    if not archive_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Snapshot archive file missing on server",
        )

    return FileResponse(
        path=str(archive_path),
        filename=archive_path.name,
        media_type="application/octet-stream",
    )
