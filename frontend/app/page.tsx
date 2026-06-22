"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";
import RequestFeed from "../components/RequestFeed";
import { parseSingaporeDate } from "../lib/mapNodes";
import type { RequestNode } from "../lib/mapNodes";
import { DashboardMetric, DashboardResponse, RequestsResponse, RouteTab, TelehitchRequest } from "../lib/types";

const TelehitchMap = dynamic(() => import("../components/TelehitchMap"), { ssr: false, loading: () => <div className="map-loading">Loading map…</div> });
const MAINTENANCE_MESSAGE = "The app is currently going through maintenance. Please try again in several hours.";

type AppSection = "tracker" | "dashboard";

const ROUTE_TABS: Array<{ id: RouteTab; label: string; description: string; disabled?: boolean }> = [
  { id: "within-sg", label: "Within SG", description: "Singapore pickup and dropoff requests" },
  { id: "sg-jb", label: "SG-JB", description: "Cross-border Singapore and Johor Bahru requests", disabled: true },
];

const SECTION_TABS: Array<{ id: AppSection; label: string; description: string; icon: string }> = [
  { id: "tracker", label: "TeleHitch Tracker", description: "Live six-hour map", icon: "↗" },
  { id: "dashboard", label: "Dashboard", description: "Request volume metrics", icon: "▦" },
];

function RouteTabs({ activeTab, onChange }: { activeTab: RouteTab; onChange: (tab: RouteTab) => void }) {
  return (
    <nav className="tabs" aria-label="Route tabs">
      {ROUTE_TABS.map((tab) => (
        <button key={tab.id} className={`${activeTab === tab.id ? "tab active" : "tab"}${tab.disabled ? " disabled" : ""}`} onClick={() => !tab.disabled && onChange(tab.id)} disabled={tab.disabled} aria-disabled={tab.disabled}>
          <span>{tab.label}{tab.disabled ? <em>Under Construction</em> : null}</span>
          <small>{tab.description}</small>
        </button>
      ))}
    </nav>
  );
}

function RequestMetricCard({ metric, title, description, selectedWindow, onWindowChange }: { metric?: DashboardMetric; title: string; description: string; selectedWindow: number; onWindowChange: (windowHours: number) => void }) {
  const maxRolling = Math.max(...(metric?.rolling_points.map((point) => point.total_count) ?? [1]), 1);

  return (
    <article className="metric-panel">
      <div className="panel-heading compact"><div><h2>{title}</h2><p>{description}</p></div></div>
      <div className="metric-body">
        <div className="window-switcher" aria-label={`${title} rolling average window`}>
          {[1, 2, 3, 6].map((windowHours) => (
            <button key={windowHours} className={selectedWindow === windowHours ? "window-option active" : "window-option"} onClick={() => onWindowChange(windowHours)}>
              {windowHours}h
            </button>
          ))}
        </div>
        {metric ? (
          <>
            <strong>{Math.round(metric.average_rolling_total).toLocaleString()}</strong>
            <span>Average unique requests in a rolling {selectedWindow}-hour window</span>
            <div className="mini-chart" aria-label={`${title} rolling request count chart`}>
              {metric.rolling_points.map((point) => (
                <div key={point.bucket_start_gmt8} className="mini-bar" style={{ height: `${Math.max(8, (point.total_count / maxRolling) * 100)}%` }} title={`${point.total_count} requests at ${parseSingaporeDate(point.bucket_start_gmt8).toLocaleTimeString("en-SG", { timeZone: "Asia/Singapore", hour: "2-digit", minute: "2-digit" })}`} />
              ))}
            </div>
          </>
        ) : <p className="loading-copy">Loading dashboard…</p>}
      </div>
    </article>
  );
}

function LiveMetricCard({ metric, title, description }: { metric?: DashboardMetric; title: string; description: string }) {
  return (
    <article className="metric-panel live">
      <div className="panel-heading compact"><div><h2>{title}</h2><p>{description}</p></div></div>
      <div className="metric-body live-count">
        {metric ? <><strong>{metric.live_15m_count.toLocaleString()}</strong><span>Unique requests made in the last 15 minutes</span></> : <p className="loading-copy">Loading dashboard…</p>}
      </div>
    </article>
  );
}

function DashboardView({ activeTab }: { activeTab: RouteTab }) {
  const [data, setData] = useState<DashboardResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [hitcherWindow, setHitcherWindow] = useState(6);
  const [driverWindow, setDriverWindow] = useState(6);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const response = await fetch(`/api/dashboard?tab=${activeTab}`, { cache: "no-store" });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.error || MAINTENANCE_MESSAGE);
        if (!cancelled) { setData(payload as DashboardResponse); setError(null); }
      } catch {
        if (!cancelled) { setError(MAINTENANCE_MESSAGE); setData(null); }
      }
    }
    load();
    const interval = window.setInterval(load, 15000);
    return () => { cancelled = true; window.clearInterval(interval); };
  }, [activeTab]);

  const metrics = data?.metrics;
  const hitcherMetric = metrics?.hitcher_request[hitcherWindow];
  const driverMetric = metrics?.driver_request[driverWindow];
  const hitcherLiveMetric = metrics?.hitcher_request[6];
  const driverLiveMetric = metrics?.driver_request[6];

  if (error) {
    return <section className="dashboard-metrics"><article className="metric-panel full-width"><div className="metric-body"><p className="maintenance-message">{error}</p></div></article></section>;
  }

  return (
    <section className="dashboard-metrics">
      <RequestMetricCard metric={hitcherMetric} title="Hitcher rolling requests" description="Total unique hitcher_requests over time" selectedWindow={hitcherWindow} onWindowChange={setHitcherWindow} />
      <LiveMetricCard metric={hitcherLiveMetric} title="Hitcher live requests" description="Total unique hitcher_requests" />
      <RequestMetricCard metric={driverMetric} title="Driver rolling requests" description="Total unique driver_requests over time" selectedWindow={driverWindow} onWindowChange={setDriverWindow} />
      <LiveMetricCard metric={driverLiveMetric} title="Driver live requests" description="Total unique driver_requests" />
    </section>
  );
}

export default function Home() {
  const [section, setSection] = useState<AppSection>("tracker");
  const [activeTab, setActiveTab] = useState<RouteTab>("within-sg");
  const [requests, setRequests] = useState<TelehitchRequest[]>([]);
  const [status, setStatus] = useState("Loading recent requests…");
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);
  const [selectedNode, setSelectedNode] = useState<RequestNode | null>(null);
  const [activeDriverCount, setActiveDriverCount] = useState<number | null>(null);

  useEffect(() => {
    if (section !== "tracker") return;
    let cancelled = false;
    async function load() {
      try {
        const response = await fetch(`/api/requests/recent?tab=${activeTab}&minutes=360&limit=500`, { cache: "no-store" });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || MAINTENANCE_MESSAGE);
        if (!cancelled) {
          const payload = data as RequestsResponse;
          setRequests(payload.requests);
          setSelectedNode(null);
          setStatus(`${payload.count} mappable requests out of ${payload.total_count} requests over the last 6 hours`);
          setActiveDriverCount(payload.active_driver_count);
          setUpdatedAt(new Date(payload.generated_at));
        }
      } catch {
        if (!cancelled) { setStatus(MAINTENANCE_MESSAGE); setRequests([]); setSelectedNode(null); setActiveDriverCount(null); }
      }
    }
    load();
    const interval = window.setInterval(load, 15000);
    return () => { cancelled = true; window.clearInterval(interval); };
  }, [activeTab, section]);

  const newest = useMemo(() => requests[0]?.message_date_gmt8, [requests]);

  return (
    <main className="app-layout">
      <aside className="side-nav" aria-label="Main navigation">
        <p className="eyebrow">Telehitch Insights</p>
        {SECTION_TABS.map((tab) => (
          <button key={tab.id} className={section === tab.id ? "side-tab active" : "side-tab"} onClick={() => setSection(tab.id)}>
            <span className="side-tab-label"><span className="side-tab-icon" aria-hidden="true">{tab.icon}</span>{tab.label}</span><small>{tab.description}</small>
          </button>
        ))}
      </aside>
      <div className="shell">
        <header className="hero">
          <div>
            <p className="eyebrow">{section === "tracker" ? "TeleHitch Tracker" : "Dashboard"}</p>
            <h1>{section === "tracker" ? "Live ride request map" : "Request volume dashboard"}</h1>
            <p className="hero-copy">{section === "tracker" ? "Minimal six-hour view of pickup and dropoff demand. Darker blinking dots are more recent; larger dots indicate overlapping pickup or dropoff points." : "Within-SG request volume summarized across rolling six-hour windows and the live last-15-minute count."}</p>
          </div>
          {section === "tracker" ? <div className="stat-card"><span>Latest post</span><strong>{newest ? parseSingaporeDate(newest).toLocaleTimeString("en-SG", { timeZone: "Asia/Singapore", hour: "2-digit", minute: "2-digit" }) : "—"}</strong><small>{updatedAt ? `Refreshed ${updatedAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}` : "Auto-refreshes every 15s"}</small></div> : null}
        </header>

        <RouteTabs activeTab={activeTab} onChange={setActiveTab} />

        {section === "tracker" ? (
          <section className="dashboard-grid">
            <div className="map-panel">
              <div className="panel-heading">
                <div className="map-heading-copy"><h2>{ROUTE_TABS.find((tab) => tab.id === activeTab)?.label}</h2><p>{status}</p><div className="legend"><span className="recency-gradient" /> <span>Darker = more recent; lightest ≈ 6 hours ago</span><span className="route-sample" /> Blinking dotted route</div><p className="active-drivers">{activeDriverCount === null ? "Loading active drivers…" : `${activeDriverCount.toLocaleString()} drivers actively searching over the past hour`}</p></div>
              </div>
              <TelehitchMap requests={requests} onSelectNode={setSelectedNode} onClearSelection={() => setSelectedNode(null)} />
            </div>
            <aside className="feed-panel">
              <div className="panel-heading compact"><div><h2>{selectedNode ? `${selectedNode.requests.length} ${selectedNode.kind === "pickup" ? "pick-up" : "drop-off"} request${selectedNode.requests.length === 1 ? "" : "s"}` : "Recent feed"}</h2><p>{selectedNode ? "Click the map background to return to the feed" : "Last 40 posts"}</p></div></div>
              <RequestFeed requests={requests} selectedNode={selectedNode} />
            </aside>
          </section>
        ) : <DashboardView activeTab={activeTab} />}
      </div>
    </main>
  );
}
