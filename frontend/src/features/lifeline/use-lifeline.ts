import { useCallback, useEffect, useRef, useState } from "react";
import { api, normalizeConflicts, normalizeEvents, normalizeSearch, normalizeShards, normalizeStatus, type ConflictItem, type EventItem, type SearchResult, type ShardItem, type StatusSnapshot } from "./api";
import { previewConflicts, previewEvents, previewSearch, previewShards, previewStatus } from "./fixtures";

type Mode = "live" | "preview";
type LoadState = "loading" | "ready" | "error";
export function useLifeline(mode: Mode) {
  const [status, setStatus] = useState<StatusSnapshot>({}); const [events, setEvents] = useState<EventItem[]>([]); const [conflicts, setConflicts] = useState<ConflictItem[]>([]); const [shards, setShards] = useState<ShardItem[]>([]);
  const [state, setState] = useState<LoadState>("loading"); const [error, setError] = useState<string>(); const [updatedAt, setUpdatedAt] = useState<Date>(); const active = useRef(false);
  const refresh = useCallback(async () => {
    if (mode === "preview") { setStatus(previewStatus); setEvents(previewEvents); setConflicts(previewConflicts); setShards(previewShards); setState("ready"); setError(undefined); setUpdatedAt(new Date()); return; }
    if (active.current) return; active.current = true;
    const controller = new AbortController();
    try {
      const settled = await Promise.allSettled([
        api.get("/engine/status", controller.signal), api.get("/sync/status", controller.signal), api.get("/telemetry/detector", controller.signal), api.get("/perception/status", controller.signal), api.get("/network/status", controller.signal), api.get("/events?limit=30", controller.signal), api.get("/conflicts", controller.signal),
      ]);
      const core = settled.slice(0, 5).map((x) => x.status === "fulfilled" ? x.value : {});
      setStatus(normalizeStatus(core));
      if (settled[5]?.status === "fulfilled") { setEvents(normalizeEvents(settled[5].value)); setShards(normalizeShards(settled[5].value)); }
      if (settled[6]?.status === "fulfilled") setConflicts(normalizeConflicts(settled[6].value));
      const failures = settled.filter((x) => x.status === "rejected").length;
      setError(failures ? `${failures} of ${settled.length} live requests unavailable` : undefined); setState(failures === settled.length ? "error" : "ready"); setUpdatedAt(new Date());
    } finally { active.current = false; }
    return () => controller.abort();
  }, [mode]);
  useEffect(() => { void refresh(); const id = window.setInterval(() => { if (!document.hidden) void refresh(); }, 5000); const onVisible = () => { if (!document.hidden) void refresh(); }; document.addEventListener("visibilitychange", onVisible); return () => { window.clearInterval(id); document.removeEventListener("visibilitychange", onVisible); }; }, [refresh]);
  return { status, events, conflicts, shards, state, error, updatedAt, refresh, setStatus };
}
export function useActions(mode: Mode, refresh: () => Promise<(() => void) | undefined> | void) {
  const [busy, setBusy] = useState<string>(); const [message, setMessage] = useState<string>(); const [actionError, setActionError] = useState<string>(); const [search, setSearch] = useState<SearchResult>();
  const run = async (key: string, live: () => Promise<unknown>, previewMessage: string) => { setBusy(key); setMessage(undefined); setActionError(undefined); try { if (mode === "preview") { await new Promise((r) => setTimeout(r, 350)); setMessage(`${previewMessage} (simulated locally)`); } else { await live(); setMessage(`${previewMessage}.`); await refresh(); } } catch (e) { setActionError(e instanceof Error ? e.message : "Request failed"); } finally { setBusy(undefined); } };
  const doSearch = async (text: string, context: string, k: number) => { setBusy("search"); setMessage(undefined); setActionError(undefined); try { if (mode === "preview") { await new Promise((r) => setTimeout(r, 300)); setSearch(previewSearch); setMessage("Preview search completed with sample data."); } else { setSearch(normalizeSearch(await api.post("/search/text", { text, context, k }))); setMessage("Memory search completed."); } } catch (e) { setActionError(e instanceof Error ? e.message : "Search failed"); } finally { setBusy(undefined); } };
  return { busy, message, actionError, search, doSearch, run };
}
