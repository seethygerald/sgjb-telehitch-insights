"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";
import RequestFeed from "../components/RequestFeed";
import { parseSingaporeDate } from "../lib/mapNodes";
import type { RequestNode } from "../lib/mapNodes";
import { DashboardResponse, RequestsResponse, RouteTab, TelehitchRequest } from "../lib/types";

const TelehitchMap = dynamic(() => import("../components/TelehitchMap"), { ssr: false, loading: () => <div className="map-loading">Loading map…</div> });
const MAINTENANCE_MESSAGE = "The app is currently going through maintenance. Please try again in several hours.";

type AppSection = "tracker" | "dashboard";

const ROUTE_TABS: Array<{ id: RouteTab; label: string; description: string; disabled?: boolean }> = [
  { id: "within-sg", label: "Within SG", description: "Singapore pickup and dropoff requests" },
  { id: "sg-jb", label: "SG-JB", description: "Cross-border Singapore and Johor Bahru requests", disabled: true },
];

const SECTION_TABS: Array<{ id: AppSection; label: string; description: string }> = [
  { id: "tracker", label: "TeleHitch Tracker", description: "Live six-hour map" },
  { id: "dashboard", label: "Dashboard", description: "Request volume metrics" },
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

function DashboardView({ activeTab }: { activeTab: RouteTab }) {
  const [data, setData] = useState<DashboardResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

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

  const maxRolling = Math.max(...(data?.rolling_6h_points.map((point) => point.total_count) ?? [1]), 1);

  return (
    <section className="dashboard-metrics">
      <article className="metric-panel">
        <div className="panel-heading compact"><div><h2>Rolling 6-hour requests</h2><p>Average total requests over time</p></div></div>
        <div className="metric-body">
          {error ? <p className="maintenance-message">{error}</p> : data ? (
            <>
              <strong>{Math.round(data.average_rolling_6h_total).toLocaleString()}</strong>
              <span>Average requests in a rolling 6-hour window</span>
              <div className="mini-chart" aria-label="Rolling six-hour request count chart">
                {data.rolling_6h_points.map((point) => (
                  <div key={point.bucket_start_gmt8} className="mini-bar" style={{ height: `${Math.max(8, (point.total_count / maxRolling) * 100)}%` }} title={`${point.total_count} requests at ${parseSingaporeDate(point.bucket_start_gmt8).toLocaleTimeString("en-SG", { timeZone: "Asia/Singapore", hour: "2-digit", minute: "2-digit" })}`} />
                ))}
              </div>
            </>
          ) : <p className="loading-copy">Loading dashboard…</p>}
        </div>
      </article>
      <article className="metric-panel live">
        <div className="panel-heading compact"><div><h2>Live 15-minute requests</h2><p>Requests made in the last 15 minutes</p></div></div>
        <div className="metric-body live-count">
          {error ? <p className="maintenance-message">{error}</p> : data ? <><strong>{data.live_15m_count.toLocaleString()}</strong><span>Updated {new Date(data.generated_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}</span></> : <p className="loading-copy">Loading dashboard…</p>}
        </div>
      </article>
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
          setUpdatedAt(new Date(payload.generated_at));
        }
      } catch {
        if (!cancelled) { setStatus(MAINTENANCE_MESSAGE); setRequests([]); setSelectedNode(null); }
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
            <span>{tab.label}</span><small>{tab.description}</small>
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
                <div><h2>{ROUTE_TABS.find((tab) => tab.id === activeTab)?.label}</h2><p>{status}</p></div>
                <div className="legend"><span className="recency-gradient" /> <span>Darker = more recent; lightest ≈ 6 hours ago</span><span className="route-sample" /> Blinking dotted route</div>
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
