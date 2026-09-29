import type { ConflictItem, EventItem, SearchResult, ShardItem, StatusSnapshot } from "./api";
export const previewStatus: StatusSnapshot = { fps: 24, p95LatencyMs: 42, memoryUsedBytes: 1342177280, memoryBudgetBytes: 2147483648, queuedUploads: 7, frames: 18420, drops: 13, depth: 4, latestObservation: "Red beacon candidate at bay 04 — operator review required", detectorName: "OpenCV red-beacon detector", detectorSource: "synthetic fixture", detectorReviewRequired: true, isOffline: true, linkState: "fault injected", localShards: 12, serverPrepared: 4, queued: 7, sent: 31, acknowledged: 28, projectionBacklog: 3, urgentQueue: 1, localOnly: 5 };
export const previewEvents: EventItem[] = [
  { id: "ev-1", at: "10:42:18", type: "inspection", message: "Beacon candidate queued for human review", severity: "warning" },
  { id: "ev-2", at: "10:41:53", type: "sync", message: "Shard bay-04-r17 retained locally", severity: "info" },
  { id: "ev-3", at: "10:40:09", type: "memory", message: "Context forklift-east pinned", severity: "info" },
  { id: "ev-4", at: "10:38:22", type: "network", message: "Client-side demo fault injection enabled", severity: "warning" },
];
export const previewShards: ShardItem[] = [
  { id: "bay-04-r17", protocol: "lifeline/2", mutable: true, activeContext: true, availability: "resident", location: "local" },
  { id: "dock-02-r09", protocol: "lifeline/2", mutable: false, activeContext: false, availability: "resident", eviction: "budget", location: "local" },
  { id: "line-a-r31", protocol: "lifeline/2", mutable: false, activeContext: false, availability: "prepared", location: "server_prepared" },
];
export const previewConflicts: ConflictItem[] = [{ id: "cf-04", context: "bay-04 / beacon-state", status: "review", revisions: [
  { id: "r17", source: "local shard", observation: "Beacon appears illuminated in synthetic frame", at: "10:42:18" },
  { id: "r16", source: "server-prepared", observation: "Beacon state not observed", at: "10:37:44" },
] }];
export const previewSearch: SearchResult = { hits: [
  { id: "h1", score: 0.91, sourceShard: "bay-04-r17", revision: "r17", observation: "Red beacon candidate detected; review pending.", contested: true },
  { id: "h2", score: 0.76, sourceShard: "dock-02-r09", revision: "r09", observation: "Synthetic procedure placeholder: inspect enclosure before reset.", contested: false },
], coverage: { searched: 9, queued: 2, unavailable_offline: 4, skipped_for_budget: 1, failed: 0 }, partial: true, shortfall: 2, elapsedMs: 18 };
export const fixtureFrame = `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" width="960" height="540"><rect width="960" height="540" fill="#091018"/><path d="M0 440L310 260L960 315V540H0Z" fill="#111d26"/><path d="M310 260V40M680 285V62M0 440L960 315" stroke="#27404d" stroke-width="4"/><g stroke="#1c333f"><path d="M0 90H960M0 180H960M0 270H960M0 360H960"/></g><rect x="650" y="170" width="92" height="170" rx="4" fill="#182832" stroke="#53717d"/><circle cx="696" cy="213" r="19" fill="#e64c42"/><circle cx="696" cy="213" r="31" fill="none" stroke="#e64c42" stroke-width="2"/><rect x="72" y="62" width="214" height="42" fill="#10202a" stroke="#2d5968"/><text x="92" y="89" fill="#8ec9d5" font-family="monospace" font-size="16">SYNTHETIC FIXTURE · BAY 04</text><path d="M638 155h116v200H638z" fill="none" stroke="#efaa4a" stroke-width="2" stroke-dasharray="8 6"/></svg>`)}`;
