import { createFileRoute } from "@tanstack/react-router";
import { LifelineDashboard } from "@/features/lifeline/LifelineDashboard";

export const Route = createFileRoute("/dashboard")({
  head: () => ({
    meta: [
      { title: "Operational Dashboard — LIFELINE" },
      { name: "description", content: "Inspect live edge health, memory retrieval, shards, synchronization, and revision conflicts in LIFELINE." },
      { property: "og:title", content: "Operational Dashboard — LIFELINE" },
      { property: "og:description", content: "LIFELINE operational console for edge memory, inspection, synchronization, and conflict review." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: LifelineDashboard,
});
