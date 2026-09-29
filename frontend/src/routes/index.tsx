import { createFileRoute } from "@tanstack/react-router";
import { LifelineLanding } from "@/features/lifeline/LifelineLanding";
export const Route = createFileRoute("/")({
  head: () => ({ meta: [
    { title: "LIFELINE — Offline-First Edge Memory" },
    { name: "description", content: "LIFELINE keeps inspection context, local shards, retrieval, and queued synchronization visible at the edge." },
    { property: "og:title", content: "LIFELINE — Edge Memory & Inspection" },
    { property: "og:description", content: "Offline-first edge memory and inspection with explicit local context and synchronization state." },
    { property: "og:type", content: "website" },
    { name: "twitter:card", content: "summary_large_image" },
  ]}), component: LifelineLanding,
});
