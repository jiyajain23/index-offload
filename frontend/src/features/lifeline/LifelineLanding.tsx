import { Link } from "@tanstack/react-router";
import {
  Activity,
  ArrowRight,
  Box,
  Database,
  Eye,
  LockKeyhole,
  MemoryStick,
  Network,
  ScanSearch,
  ShieldCheck,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import HalftoneNebula from "@/components/ui/halftone-nebula";

const capabilities = [
  {
    icon: <MemoryStick />,
    label: "Local memory",
    title: "Context stays close to the work.",
    copy: "Pinned and mutable shards preserve the context an edge device needs, even when the network does not.",
  },
  {
    icon: <ScanSearch />,
    label: "Bounded retrieval",
    title: "Search explains its coverage.",
    copy: "Results identify their source shard and revision, while partial coverage and shortfall remain visible.",
  },
  {
    icon: <ShieldCheck />,
    label: "Controlled sync",
    title: "Transfer is explicit, not assumed.",
    copy: "Queued, sent, acknowledged, urgent, and local-only states make synchronization inspectable.",
  },
];

function Brand() {
  return (
    <div className="landing-brand">
      <span className="landing-brand-mark"><Activity /></span>
      <span><strong>LIFELINE</strong><small>EDGE MEMORY / INSPECTION</small></span>
    </div>
  );
}

export function LifelineLanding() {
  return (
    <main className="landing-shell">
      <HalftoneNebula params={{ planetX: 0.74, planetY: 0.2 }} className="landing-hero">
        <header className="landing-nav">
          <Link to="/" aria-label="LIFELINE home"><Brand /></Link>
          <nav aria-label="Landing navigation">
            <a href="#flow">How it works</a>
            <a href="#capabilities">Capabilities</a>
            <a href="#privacy">Privacy</a>
          </nav>
          <Button asChild size="sm" variant="outline" className="nav-cta">
            <Link to="/dashboard">Dashboard <ArrowRight /></Link>
          </Button>
        </header>
        <div className="hero-micro" aria-hidden="true">
          <span>EDGE MEMORY / LOCAL CONTEXT</span>
          <span>VISUAL FIELD / NON-TELEMETRIC</span>
        </div>
        <div className="hero-copy">
          <p className="hero-eyebrow">✦ OFFLINE-FIRST OPERATIONS</p>
          <h1>Memory at<br />the edge.</h1>
          <p>LIFELINE keeps inspection context, local shards, retrieval, and queued synchronization useful when a link is unavailable.</p>
          <Button asChild size="lg" variant="outline" className="hero-cta">
            <Link to="/dashboard">Open dashboard <ArrowRight /></Link>
          </Button>
        </div>
        <p className="hero-disclosure">INTERACTIVE VISUAL / NOT LIVE TELEMETRY</p>
      </HalftoneNebula>

      <section id="flow" className="landing-section flow-section">
        <div className="section-intro">
          <span className="landing-kicker">HOW IT WORKS</span>
          <h2>Context moves deliberately.</h2>
          <p>Local work continues first. Synchronization follows when a link is available and policy permits.</p>
        </div>
        <div className="flow-rail">
          <article><span>01</span><Box /><h3>Observe at the edge</h3><p>Inspection events and context begin near the device.</p></article>
          <article><span>02</span><Database /><h3>Retain in shards</h3><p>Local, pinned, and active context remains explicit.</p></article>
          <article><span>03</span><Network /><h3>Synchronize by state</h3><p>Queued transfer and acknowledgements stay observable.</p></article>
        </div>
      </section>

      <section id="capabilities" className="landing-section capability-section">
        <div className="section-intro">
          <span className="landing-kicker">CAPABILITIES</span>
          <h2>A focused operational surface.</h2>
        </div>
        <div className="capability-grid">
          {capabilities.map((item) => <article key={item.label} className="capability-card"><div>{item.icon}<span>{item.label}</span></div><h3>{item.title}</h3><p>{item.copy}</p></article>)}
        </div>
      </section>

      <section id="privacy" className="landing-section privacy-section">
        <div className="privacy-copy">
          <span className="landing-kicker">PRIVACY / CONTROL</span>
          <h2>Local-first is a visible state.</h2>
          <p>LIFELINE distinguishes local-only context from queued and acknowledged transfers. It does not hide uncertainty or replace operator judgment.</p>
          <Link to="/dashboard">Inspect the operational view <ArrowRight /></Link>
        </div>
        <div className="privacy-ledger">
          <div><LockKeyhole /><span><b>Local-only memory</b><small>Remains on the edge</small></span><em>RETAINED</em></div>
          <div><Network /><span><b>Queued synchronization</b><small>Waits for an available link</small></span><em>EXPLICIT</em></div>
          <div><Eye /><span><b>Inspection indicator</b><small>Requires operator review</small></span><em>REVIEW</em></div>
        </div>
      </section>

      <footer className="landing-footer"><Brand /><span>Offline-first edge memory and inspection.</span><Link to="/dashboard">Open dashboard <ArrowRight /></Link></footer>
    </main>
  );
}
