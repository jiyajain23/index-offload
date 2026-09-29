# LIFELINE

LIFELINE is an exportable React + TypeScript edge-memory and inspection product. The public landing page lives at `/`; the complete operational dashboard lives at `/dashboard`. It is offline-aware, backend-neutral, and contains no authentication, database, hosted service, or fabricated live telemetry.

## Run locally

```bash
npm install
npm run dev
```

Open `http://localhost:5173` (or the URL printed by Vite).

## Production build

```bash
npm run build
```

The deployable output is written to `dist/`.

## API configuration

The frontend reads `VITE_API_BASE_URL` and defaults to `/api/v1`.

```bash
VITE_API_BASE_URL=http://localhost:8000/api/v1 npm run dev
```

Only variables prefixed with `VITE_` are exposed to browser code. The central API boundary and conservative response normalizers live in `src/features/lifeline/api.ts`. Missing values remain unknown and render as an em dash; they are never converted to zero.

### Endpoint map

| Method | Path | Use |
|---|---|---|
| GET | `/engine/status` | FPS, latency, memory, shard counts |
| GET | `/sync/status` | Queue, transfer, acknowledgement, privacy counts |
| GET | `/telemetry/detector` | Detector identity/source and review metadata |
| GET | `/perception/status` | Frame counters, drops, depth, latest observation |
| GET | `/perception/frame` | Latest image; browser adds a cache-busting query |
| GET | `/events?limit=30` | Timeline and optional shard inventory |
| GET | `/network/status` | Link state and `is_offline` |
| GET | `/conflicts` | Read-only competing revision review |
| POST | `/search/text` | `{ "text": string, "context": string, "k": number }` |
| POST | `/network/offline` | `{ "offline": boolean }` demo fault injection |
| POST | `/sync/trigger` | `{}` |
| POST | `/snapshots/fetch` | `{ "context_id": string }` |
| POST | `/demo/seed` | `{}` (reserved for a backend-supplied demo; the static preview does not call it) |

Responses may omit fields. Common snake_case and camelCase aliases are normalized conservatively. The overview polls every five seconds, pauses while the page is hidden, avoids concurrent polls, and cancels browser work on teardown.

## Live and design preview

- **Live** contacts the configured API and never fills missing metrics with sample values.
- **Design preview** is explicitly labelled and uses static fixture values. Its controls simulate locally and disclose that behavior in response feedback.
- The preview image is an embedded synthetic facility frame. The OpenCV red-beacon detector is a label only; this frontend does not claim GPU inference or perform diagnosis.

## Serve `dist` from FastAPI

Build the frontend, then mount the generated client files after API routes. Keep `/api/v1` registered before the catch-all.

```python
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()
DIST = Path(__file__).parent / "dist" / "client"

# Register your /api/v1 routes here first.
app.mount("/assets", StaticFiles(directory=DIST / "assets"), name="assets")

@app.get("/{path:path}")
def frontend(path: str):
    requested = DIST / path
    if path and requested.is_file():
        return FileResponse(requested)
    return FileResponse(DIST / "index.html")
```

Depending on adapter configuration, inspect `dist/` after building and set `DIST` to the directory containing `index.html`.

## Source layout

- `src/features/lifeline/api.ts` — typed HTTP client and conservative normalizers
- `src/features/lifeline/LifelineLanding.tsx` — public product landing page and static code-native preview
- `src/features/lifeline/use-lifeline.ts` — visibility-aware polling and action state
- `src/features/lifeline/fixtures.ts` — clearly separated static preview data
- `src/features/lifeline/LifelineDashboard.tsx` — dashboard sections and interactions
- `src/styles.css` — semantic design tokens and responsive presentation

## Accessibility and efficiency

The interface supports keyboard focus, responsive layouts, reduced-motion preferences, and a low-power display control that disables topology motion. All controls and labels remain HTML; the topology scene is supplemental state visualization.
