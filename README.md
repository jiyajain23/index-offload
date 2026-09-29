# LIFELINE: Offline-First Edge Memory & Vector Retrieval Platform

**LIFELINE** is an offline-first inspection memory and retrieval management system built for inspection robotics and rugged mobile edge devices operating under strict hardware constraints (RAM, CPU, and intermittent connectivity).

The system enables an edge device to run real-time perception models without dropping frames, record observations durably while disconnected, execute low-latency vector searches over local procedure shards, and safely synchronize with an upstream ingestion server and **Qdrant Cloud** when network connectivity is restored.

---

## 1. System Architecture

LIFELINE is composed of three synchronized tiers:

```
┌────────────────────────────────────────────────────────────────────────┐
│               LIFELINE Modern Operations Dashboard                     │
│    React 19 · TypeScript · TanStack Router · Tailwind CSS · Lucide     │
│   Landing Page (/) · Console (/dashboard) · Live Telemetry · Search   │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ HTTP / JSON API (/api/v1)
┌───────────────────────────────────▼────────────────────────────────────┐
│                    LIFELINE Edge Device Backend                        │
│                 FastAPI · Port 8000 · Soft Budget (400 MB)             │
│                                                                        │
│   ┌─────────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│   │    Engine           │  │  Store           │  │   Detector       │  │
│   │  • ShardManager     │  │  • SQLite Memory │  │  • BeaconRunner  │  │
│   │  • LRU Eviction     │  │  • Revision Log  │  │  • TelemetryHub  │  │
│   │  • Reader Leases    │  │  • Urgent Outbox │  │  • Frame Ingest  │  │
│   │  • local_write Shard│  │  • Privacy Rules │  │  • FPS/p95 Stats │  │
│   └──────────┬──────────┘  └────────┬─────────┘  └────────┬─────────┘  │
│              │                      │                     │            │
│              ▼                      ▼                     ▼            │
│       Qdrant Edge Shards      SQLite Database      Inspection Camera   │
│       (Embedded On-Disk)     (Authoritative DB)     (Video Stream)     │
└─────────────────────────────────────┬──────────────────────────────────┘
                                      │ Outbox Uploads & Snapshot Handoff
┌─────────────────────────────────────▼──────────────────────────────────┐
│                   Upstream Ingestion Server                            │
│                 FastAPI · Port 8001 · ServerStore                      │
│                                                                        │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │              Qdrant Cloud Server Index Preparer                │   │
│   │  • Dense + Sparse Vector Indexing (minilm-l6-v2)               │   │
│   │  • Optimizes collections and exports Shard Snapshots           │   │
│   └───────────────────────────────┬────────────────────────────────┘   │
└───────────────────────────────────┼────────────────────────────────────┘
                                    │ QDRANT_URL + QDRANT_API_KEY
┌───────────────────────────────────▼────────────────────────────────────┐
│                      Qdrant Cloud Cluster                              │
│   Managed Vector Database · Dense & Sparse Vectors · Snapshots Export │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Core Functional Pillars

### 2.1 Edge Engine & Resource Control (`engine/`)
* **Bounded Working-Set Budget**: Enforces a strict soft memory cap (e.g. 400 MB) via `engine/monitor.py` (supporting Linux cgroup v2 with local process RSS fallback).
* **Deterministic LRU Eviction**: Unloads inactive context shards when memory pressure rises, with cooldown and hysteresis guards to eliminate thrashing.
* **Reader Leases (`lease_shard`)**: Guarantees active search queries complete safely without handles being evicted or replaced mid-operation.
* **Dedicated Shard Roles**:
  * `local_write`: Dedicated non-evictable mutable projection shard for local observations.
  * Pinned Procedures: Critical emergency response cards (`emergency_protocols`) remain loaded in memory at all times.
  * On-Demand Contexts: Sharded spatial/zone indexes (`zone_01`, `zone_02`) loaded dynamically.

### 2.2 Authoritative Durable Memory & Sync (`memory/`, `sync/`)
* **SQLite Authority**: Every write, revision, and tombstone is committed durably to SQLite before updating vector search projections.
* **Revision & Conflict Tracking**: Preserves competing concurrent operations under network partitioning. Identifies contested revisions for operator review without silent overwrites.
* **Privacy Enforcement**: Observations marked `LOCAL_ONLY` are strictly preserved on the edge and filtered out of upload batches.
* **Urgent Outbox Worker**: Upload worker prioritizes emergency/hazard records ahead of routine telemetry batches.
* **Atomic Snapshot Activation**: Downloads server-prepared Qdrant snapshots, verifies SHA-256 checksums in staging, hands handles to the Engine, and maintains durable journals for crash rollback.

### 2.3 Perception & Telemetry Boundary (`perception/`, `app/telemetry.py`)
* **Bounded Perception Runner**: Background OpenCV beacon detector analyzing inspection feeds without blocking the search engine.
* **Contention Monitoring**: Thread-safe sliding-window telemetry hub measuring FPS, p50/p95/p99 inference latency, dropped frames, and queue depths.
* **Deterministic Labelling**: Clearly marks synthetic vs live observations to ensure test fixtures are never misrepresented as operational hardware results.

### 2.4 Modern Frontend Operations Console (`frontend/`)
* **Stack**: Built with React 19, TypeScript, TanStack Router & Start, Tailwind CSS, Lucide icons, and Radix UI components.
* **Views**:
  * **Landing Page (`/`)**: Product showcase and architectural specification.
  * **Operations Dashboard (`/dashboard`)**: Live topology status, perception frame inspector, memory search with coverage breakdowns, shard inventory, outbox queue gauges, conflict comparison, and network fault injection drills.

---

## 3. Quickstart & Local Execution

### Prerequisites
* **Python**: 3.10 to 3.14 (Python 3.14 tested)
* **Node.js**: v20+ with `npm`

### 1. Configure Environment (`.env`)
Copy [`.env.example`](.env.example) to `.env`:
```bash
cp .env.example .env
```
Fill in your Qdrant Cloud details:
```env
QDRANT_URL=https://745e9348-4852-4130-aeff-39b7dae841d5.australia-southeast1-0.gcp.cloud.qdrant.io
QDRANT_API_KEY=your_qdrant_api_key_here
LIFELINE_DEMO=1
```

### 2. Install Dependencies
```bash
# Backend dependencies
py -m pip install -r requirements.txt

# Frontend dependencies
cd frontend
npm install
cd ..
```

### 3. Run the Platform

#### Option A: One-Command Launcher (Recommended)
```bash
# Windows
.\dev.bat

# Or Python runner
py run_fullstack.py --dev
```

#### Option B: Dedicated Terminals
**Terminal 1 — Edge Backend:**
```bash
py -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

**Terminal 2 — Ingestion Server:**
```bash
py -m uvicorn server.server:app --host 0.0.0.0 --port 8001
```

**Terminal 3 — Frontend Dev Server:**
```bash
cd frontend
npm run dev
```

---

## 4. Web URLs & Access Points

| Service | Address | Description |
|---|---|---|
| **Operations Dashboard** | [http://localhost:8000/dashboard](http://localhost:8000/dashboard) | Main real-time operational inspection console |
| **Vite Dev Server** | [http://localhost:5173/dashboard](http://localhost:5173/dashboard) | Dev server with Hot Module Replacement |
| **Public Landing Page** | [http://localhost:8000/](http://localhost:8000/) | Product landing page |
| **Edge API Explorer** | [http://localhost:8000/docs](http://localhost:8000/docs) | Swagger UI for Edge device endpoints |
| **Ingestion Server Health** | [http://localhost:8001/health](http://localhost:8001/health) | Sync server & Qdrant Cloud connection state |

---

## 5. API Endpoint Reference

### Edge API (`:8000/api/v1`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/engine/status` | Engine working set memory, budget, active leases, and loaded shards |
| `GET` | `/sync/status` | Outbox counts (queued, sent, acknowledged), urgent queue, local-only counts |
| `POST` | `/sync/trigger` | Triggers immediate outbox batch processing |
| `GET` | `/telemetry/detector` | Detector identity, FPS rate, and p95 latency |
| `GET` | `/perception/status` | Processed frame counter, drop rate, queue depth, latest observation |
| `GET` | `/perception/frame` | Latest processed JPEG frame (with synthetic fallback SVG) |
| `GET` | `/events?limit=30` | Recent event timeline and resident shard inventory |
| `GET` | `/shards` | Direct list of registered shards and residency states |
| `GET` | `/conflicts` | Contested alternative revisions grouped with review metadata |
| `POST` | `/conflicts/resolve` | Resolves competing operations with a superseding record |
| `POST` | `/search/text` | Hybrid search across local active contexts and emergency protocols |
| `POST` | `/records` | Durably commits an observation record to SQLite and projects into search |
| `GET` | `/records/{id}` | Fetches authoritative current record revision |
| `DELETE` | `/records/{id}` | Writes a durable tombstone record |
| `POST` | `/network/offline` | Simulates network link interruption (`{"offline": true}`) |
| `GET` | `/network/status` | Link status and offline fault injection indicator |
| `POST` | `/snapshots/fetch` | Downloads, unpacks, and activates a server vector snapshot |
| `POST` | `/demo/seed` | Seeds multi-zone demonstration records and procedure cards |

### Ingestion Server API (`:8001/api/v1`)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/ingest` | Ingests and deduplicates outbox batches from edge devices |
| `POST` | `/snapshots/prepare` | Indexes records in Qdrant Cloud and exports a shard snapshot |
| `GET` | `/snapshots/latest/{context_id}` | Returns metadata for latest context snapshot |
| `GET` | `/snapshots/download/{snapshot_id}` | Streams prepared snapshot archive to edge device |
| `GET` | `/health` | Ingestion service status and Qdrant Cloud connectivity |

---

## 6. Testing & Quality Assurance

LIFELINE includes an exhaustive test suite covering Qdrant Edge adapter integration, resource monitor accounting, shard leasing, crash recovery, outbox sync, and public API contracts:

```bash
# Run backend test suite
py -m pytest -v

# Run production frontend build
cd frontend
npm run build
```

> **Note on Architecture Validation:** For a full breakdown of the 12 technical experiments proving the system's ability to survive hard Docker `cgroup` memory limits (512MB) and manage embedded vector shards, please read the [Architecture Validation Report](docs/VALIDATION_REPORT.md).

---

## 7. License

Internal inspection robotics software. All rights reserved.
