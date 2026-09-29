export type UnknownRecord = Record<string, unknown>;
export type CoverageState = "searched" | "queued" | "unavailable_offline" | "skipped_for_budget" | "failed";

export interface StatusSnapshot {
  fps?: number | undefined; p95LatencyMs?: number | undefined; memoryUsedBytes?: number | undefined; memoryBudgetBytes?: number | undefined;
  queuedUploads?: number | undefined; frames?: number | undefined; drops?: number | undefined; depth?: number | undefined; latestObservation?: string | undefined;
  detectorName?: string | undefined; detectorSource?: string | undefined; detectorReviewRequired?: boolean | undefined;
  isOffline?: boolean | undefined; linkState?: string | undefined; localShards?: number | undefined; serverPrepared?: number | undefined;
  queued?: number | undefined; sent?: number | undefined; acknowledged?: number | undefined; projectionBacklog?: number | undefined; urgentQueue?: number | undefined; localOnly?: number | undefined;
  perceptionSource?: string | undefined; isCamera?: boolean | undefined; remoteStream?: boolean | undefined;
}
export interface EventItem { id: string; at?: string | undefined; type?: string | undefined; message?: string | undefined; severity?: string | undefined }
export interface ConflictItem { id: string; context?: string | undefined; status?: string | undefined; revisions: Array<{ id: string; source?: string | undefined; observation?: string | undefined; at?: string | undefined }> }
export interface SearchHit { id: string; score?: number | undefined; sourceShard?: string | undefined; revision?: string | undefined; observation?: string | undefined; contested?: boolean | undefined }
export interface SearchResult { hits: SearchHit[]; coverage: Partial<Record<CoverageState, number>>; partial?: boolean | undefined; shortfall?: number | undefined; elapsedMs?: number | undefined }
export interface ShardItem { id: string; protocol?: string | undefined; mutable?: boolean | undefined; activeContext?: boolean | undefined; availability?: string | undefined; eviction?: string | undefined; location?: "local" | "server_prepared" | undefined }

const RAW_BASE = import.meta.env["VITE_API_BASE_URL"];
let BASE = (RAW_BASE || "/api/v1").replace(/\/$/, "");
if (RAW_BASE && !BASE.endsWith("/api/v1")) {
  BASE = `${BASE}/api/v1`;
}

const rec = (v: unknown): UnknownRecord => v && typeof v === "object" && !Array.isArray(v) ? v as UnknownRecord : {};
const arr = (v: unknown): unknown[] => Array.isArray(v) ? v : [];
const num = (...v: unknown[]) => { const x = v.find((n) => typeof n === "number" && Number.isFinite(n)); return typeof x === "number" ? x : undefined };
const str = (...v: unknown[]) => { const x = v.find((n) => typeof n === "string" && n.trim()); return typeof x === "string" ? x : undefined };
const bool = (...v: unknown[]) => { const x = v.find((n) => typeof n === "boolean"); return typeof x === "boolean" ? x : undefined };

async function request(path: string, init: RequestInit = {}) {
  const response = await fetch(`${BASE}${path}`, { ...init, headers: { Accept: "application/json", ...(init.body ? { "Content-Type": "application/json" } : {}), ...init.headers } });
  if (!response.ok) throw new Error(`${response.status} ${response.statusText || "Request failed"}`);
  if (response.status === 204) return {};
  return response.json() as Promise<unknown>;
}

export const api = {
  get: (path: string, signal?: AbortSignal) => request(path, signal ? { signal } : {}),
  post: (path: string, body: unknown, signal?: AbortSignal) => request(path, { method: "POST", body: JSON.stringify(body), ...(signal ? { signal } : {}) }),
  frameUrl: () => `${BASE}/perception/frame?t=${Date.now()}`,
  setPerceptionSource: (source: "webcam" | "fixture" | "remote") => request("/perception/source", { method: "POST", body: JSON.stringify({ source }) }),
  /** Upload a single JPEG blob captured from the browser's getUserMedia stream */
  uploadBrowserFrame: (blob: Blob): Promise<unknown> => {
    const form = new FormData();
    form.append("file", blob, "frame.jpg");
    return fetch(`${BASE}/perception/frame/upload`, { method: "POST", body: form }).then((r) => {
      if (!r.ok) throw new Error(`${r.status} upload failed`);
      return r.json();
    });
  },
};


export function normalizeStatus(parts: unknown[]): StatusSnapshot {
  const records = parts.map(rec);
  const engine = records[0] ?? {}; const sync = records[1] ?? {}; const detector = records[2] ?? {}; const perception = records[3] ?? {}; const network = records[4] ?? {};
  const memory = rec(engine["memory"]); const queue = rec(sync["queue"]); const metrics = rec(perception["metrics"]);
  return {
    fps: num(engine["fps"], perception["fps"], metrics["fps"]),
    p95LatencyMs: num(engine["p95_latency_ms"], engine["p95LatencyMs"], perception["p95_latency_ms"]),
    memoryUsedBytes: num(engine["memory_used_bytes"], engine["usage_bytes"], memory["used_bytes"]),
    memoryBudgetBytes: num(engine["memory_budget_bytes"], engine["soft_budget_bytes"], memory["budget_bytes"]),
    queuedUploads: num(sync["queued_uploads"], sync["queued"], sync["outbox_queued"], queue["queued"]),
    frames: num(perception["frames"], perception["processed_frames"], metrics["frames"]),
    drops: num(perception["drops"], metrics["drops"]),
    depth: num(perception["depth"], metrics["depth"]),
    latestObservation: str(perception["latest_observation"], perception["latestObservation"]),
    detectorName: str(detector["name"], detector["detector"], detector["model_name"]),
    detectorSource: str(detector["source"], detector["fixture"], detector["provenance"]),
    detectorReviewRequired: bool(detector["review_required"], detector["reviewRequired"]),
    isOffline: bool(network["is_offline"], network["isOffline"]),
    linkState: str(network["link_state"], network["state"]),
    localShards: num(engine["local_shards"], memory["local_shards"]),
    serverPrepared: num(engine["server_prepared"], memory["server_prepared"]),
    queued: num(sync["queued"], sync["outbox_queued"], queue["queued"]),
    sent: num(sync["sent"], sync["outbox_sent"], queue["sent"]),
    acknowledged: num(sync["acknowledged"], sync["acked"], sync["outbox_acknowledged"], queue["acknowledged"]),
    projectionBacklog: num(sync["projection_backlog"], queue["projection_backlog"]),
    urgentQueue: num(sync["urgent_queue"], sync["urgent"], sync["urgent_queued"], queue["urgent"]),
    localOnly: num(sync["local_only"], queue["local_only"]),
    perceptionSource: str(perception["source"], perception["video_source"]),
    isCamera: bool(perception["is_camera"]) ?? (perception["source"] === "webcam" || perception["source"] === "remote"),
    remoteStream: bool(perception["remote_stream"]) ?? (perception["source"] === "remote"),
  };
}

export function normalizeEvents(value: unknown): EventItem[] {
  const r = rec(value);
  return arr(r["events"] ?? value).map((v, i) => {
    const x = rec(v);
    const p = rec(x["payload"]);
    return {
      id: str(x["id"], x["event_id"]) ?? `event-${i}`,
      at: str(x["at"], x["emitted_at"], x["timestamp"], x["created_at"]),
      type: str(x["type"], x["event_type"], x["kind"]),
      message: str(x["message"], x["summary"], p["message"], p["summary"], p["description"], p["error"]),
      severity: str(x["severity"], x["level"], p["severity"])
    };
  });
}

export function normalizeConflicts(value: unknown): ConflictItem[] {
  const r = rec(value);
  return arr(r["conflicts"] ?? value).map((v, i) => {
    const x = rec(v);
    let revisions = arr(x["revisions"]).map((rv, j) => {
      const q = rec(rv);
      return {
        id: str(q["id"], q["revision"]) ?? `revision-${j}`,
        source: str(q["source"], q["shard"]),
        observation: str(q["observation"], q["text"]),
        at: str(q["at"], q["timestamp"])
      };
    });
    if (!revisions.length && (x["operation_id"] || x["competing_operation_id"])) {
      revisions = [
        {
          id: str(x["operation_id"]) ?? `op-${i}`,
          source: "local shard",
          observation: str(x["observation"], x["observation_json"]) ?? "Competing revision",
          at: str(x["created_at"])
        }
      ];
    }
    return {
      id: str(x["id"], x["conflict_id"]) ?? `conflict-${i}`,
      context: str(x["context"], x["context_id"]) ?? (x["record_id"] ? `${x["record_id"]} / v${x["version"] ?? 1}` : undefined),
      status: str(x["status"]) ?? "review",
      revisions
    };
  });
}

export function normalizeSearch(value: unknown): SearchResult {
  const r = rec(value);
  const coverage = rec(r["coverage"]);
  const states: CoverageState[] = ["searched", "queued", "unavailable_offline", "skipped_for_budget", "failed"];
  return {
    hits: arr(r["hits"]).map((v, i) => {
      const x = rec(v);
      let obs: string | undefined = undefined;
      if (typeof x["observation"] === "string" && x["observation"].trim()) {
        obs = x["observation"];
      } else if (typeof x["text"] === "string" && x["text"].trim()) {
        obs = x["text"];
      } else if (x["observation"] && typeof x["observation"] === "object") {
        const o = x["observation"] as Record<string, unknown>;
        obs = (typeof o["description"] === "string" && o["description"]) ||
              (typeof o["summary"] === "string" && o["summary"]) ||
              (typeof o["text"] === "string" && o["text"]) ||
              JSON.stringify(o);
      }
      return {
        id: str(x["id"], x["record_id"]) ?? `hit-${i}`,
        score: num(x["score"]),
        sourceShard: str(x["source_shard"], x["sourceShard"]),
        revision: str(x["revision"], x["version"] !== undefined ? `r${x["version"]}` : undefined),
        observation: obs,
        contested: bool(x["contested"], x["is_contested"])
      };
    }),
    coverage: Object.fromEntries(
      states.flatMap((key) => {
        const val = coverage[key];
        const count = Array.isArray(val) ? val.length : num(val);
        return count === undefined ? [] : [[key, count]];
      })
    ),
    partial: bool(r["partial"]),
    shortfall: num(r["shortfall"]),
    elapsedMs: num(r["elapsed_ms"], r["elapsedMs"])
  };
}

export function normalizeShards(value: unknown): ShardItem[] {
  const r = rec(value);
  return arr(r["shards"]).map((v, i) => {
    const x = rec(v);
    const location = str(x["location"], x["tier"]);
    return {
      id: str(x["id"]) ?? `shard-${i}`,
      protocol: str(x["protocol"]),
      mutable: bool(x["mutable"], x["local_write"]),
      activeContext: bool(x["active_context"], x["activeContext"]),
      availability: str(x["availability"]),
      eviction: str(x["eviction"]),
      location: location === "server_prepared" ? "server_prepared" : location === "local" ? "local" : undefined
    };
  });
}

