"""Main FastAPI entrypoint for LIFELINE Edge Device Backend."""

from __future__ import annotations

from contextlib import asynccontextmanager
import logging
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from qdrant_edge import Distance, EdgeConfig, EdgeSparseVectorParams, EdgeVectorParams

from edge.qdrant_adapter import QdrantEdgeShardStore
from engine.monitor import ResourceMonitor
from engine.network_control import NetworkFaultController
from engine.shard_manager import ShardManager
from memory.service import MemoryService
from memory.store import SQLiteMemoryStore
from shared.contracts import ShardRole
from shared.events import EventBus
from sync.snapshot_manager import SnapshotManager
from sync.upload_worker import UploadWorker

from . import routes
from .telemetry import DetectorTelemetryHub, DeterministicDetectorStub

try:
    from perception.runner import BeaconRunner
    from perception.make_fixture import make_fixture
except Exception as _exc:
    BeaconRunner = None  # type: ignore
    make_fixture = None  # type: ignore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("lifeline.app")


def create_app(
    data_dir: Path | str = "./edge_data",
    server_url: str = "http://localhost:8001",
    soft_budget_mb: Optional[float] = 400.0,
    dimension: int = 384,
    start_detector_stub: bool = False,
) -> FastAPI:
    """Create and configure the complete standalone edge application."""
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)

    # 1. Event Bus
    bus = EventBus(max_history=500)

    # 2. Resource Monitor & Shard Store
    monitor = ResourceMonitor()
    config = EdgeConfig(
        vectors={"embedding": EdgeVectorParams(size=dimension, distance=Distance.Cosine)},
        sparse_vectors={"text_sparse": EdgeSparseVectorParams()},
    )
    shard_store = QdrantEdgeShardStore(config)

    mem_store = SQLiteMemoryStore(data_path / "memory.db")

    # 3. Engine (Person A)
    engine = ShardManager(
        shard_store=shard_store,
        resource_monitor=monitor,
        soft_budget_mb=soft_budget_mb,
        events=bus,
        default_shard_cost_mb=10.0,
        snapshot_is_installed=mem_store.is_snapshot_installed,
    )

    # Initialize dedicated non-evictable local_write shard
    local_write_path = data_path / "local_write_shard"
    if not local_write_path.exists():
        created_shard = shard_store.create(str(local_write_path))
        shard_store.close(created_shard)

    engine.register_shard(
        shard_id="local_write",
        path=str(local_write_path),
        pinned=True,
        role=ShardRole.LOCAL_WRITE,
        estimated_cost_mb=5.0,
    )
    engine.ensure_loaded("local_write")

    demo_mode = os.environ.get("LIFELINE_DEMO", "1") == "1"
    if demo_mode:
        for shard_id, pinned in (("emergency_protocols", True),
                                 ("zone_01", False), ("zone_02", False)):
            shard_path = data_path / shard_id
            if not shard_path.exists():
                shard_store.close(shard_store.create(shard_path))
            engine.register_shard(shard_id, str(shard_path), pinned=pinned,
                                  estimated_cost_mb=5)
        engine.load_pinned()

    # 4. Durable Memory Store & Service (Person B)
    memory_service = MemoryService(mem_store, engine, events=bus)

    # Replay any pending projections from previous run
    replayed = memory_service.replay_pending_projections()
    if replayed:
        logger.info("Replayed %d pending projections on startup", replayed)

    # 5. Network Fault Controller & Sync
    fault_ctl = NetworkFaultController(offline_by_default=False)
    upload_worker = UploadWorker(
        store=mem_store,
        server_url=server_url,
        network_controller=fault_ctl,
        events=bus,
        batch_size=10,
        poll_interval=1.0,
    )

    staging_dir = data_path / "snapshot_staging"
    snapshot_manager = SnapshotManager(
        store=mem_store,
        engine=engine,
        server_url=server_url,
        staging_dir=staging_dir,
        network_controller=fault_ctl,
        events=bus,
    )

    # 6. Detector Telemetry Hub
    telemetry_hub = DetectorTelemetryHub()
    detector_stub = DeterministicDetectorStub(telemetry_hub, events=bus) if start_detector_stub else None
    runner = None
    if demo_mode and BeaconRunner is not None:
        try:
            fixture_path = Path("fixtures/inspection_beacon.avi")
            if not fixture_path.exists():
                fixture_path = data_path / "inspection_beacon.avi"
            if not fixture_path.exists() and make_fixture is not None:
                make_fixture(fixture_path)
            if fixture_path.exists():
                runner = BeaconRunner(fixture_path, memory_service, telemetry_hub, events=bus)
        except Exception as _run_err:
            logger.warning("Could not initialize beacon runner: %s", _run_err)

    # Wire into router module globals
    routes.MEMORY_SERVICE = memory_service
    routes.ENGINE = engine
    routes.UPLOAD_WORKER = upload_worker
    routes.SNAPSHOT_MANAGER = snapshot_manager
    routes.NETWORK_CONTROLLER = fault_ctl
    routes.TELEMETRY_HUB = telemetry_hub
    routes.EVENT_BUS = bus
    routes.PERCEPTION_RUNNER = runner

    # 7. Lifespan context manager
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info("Starting LIFELINE edge services...")
        upload_worker.start()
        if detector_stub:
            detector_stub.start()
        if runner:
            runner.start()
        yield
        logger.info("Stopping LIFELINE edge services...")
        upload_worker.stop()
        if detector_stub:
            detector_stub.stop()
        if runner:
            runner.stop()
        engine.close_all()
        logger.info("LIFELINE shutdown complete.")

    app = FastAPI(title="LIFELINE Edge Backend", version="1.0.0", lifespan=lifespan)

    # Enable CORS for cross-origin frontend dev server requests
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(routes.router, prefix="/api/v1")

    @app.get("/health")
    def health():
        return {"status": "ok", "service": "lifeline-edge-backend"}

    # Discover frontend build directory (checking merged frontend, output, and dashboard)
    root_dir = Path(__file__).resolve().parent.parent
    frontend_candidates = [
        root_dir / "frontend" / ".output" / "public",
        root_dir / "frontend" / "dist",
        root_dir.parent / "frontend" / ".output" / "public",
        root_dir.parent / "frontend" / "dist",
        root_dir / "dashboard" / "dist",
    ]
    frontend_dir = next(
        (p for p in frontend_candidates if p.exists() and ((p / "index.html").exists() or (p / "assets").exists())),
        None
    )

    if frontend_dir:
        logger.info("Serving frontend static assets from %s", frontend_dir)
        assets_dir = frontend_dir / "assets"
        if assets_dir.exists():
            app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

        app.mount("/dashboard", StaticFiles(directory=frontend_dir, html=True), name="dashboard")

        @app.get("/{full_path:path}")
        def serve_frontend_spa(full_path: str):
            requested = frontend_dir / full_path
            if full_path and requested.is_file():
                return FileResponse(requested)
            index_file = frontend_dir / "index.html"
            if index_file.exists():
                return FileResponse(index_file)
            return {"status": "ok", "service": "lifeline-edge-backend", "docs": "/docs"}

    return app


app = create_app(
    data_dir=os.environ.get("LIFELINE_EDGE_DATA", "./edge_data"),
    server_url=os.environ.get("LIFELINE_SERVER_URL", "http://localhost:8001"),
    soft_budget_mb=float(os.environ.get("LIFELINE_SOFT_BUDGET_MB", "400")),
)
