"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";
import RequestFeed from "../components/RequestFeed";
import { RequestsResponse, RouteTab, TelehitchRequest } from "../lib/types";

const TelehitchMap = dynamic(() => import("../components/TelehitchMap"), { ssr: false, loading: () => <div className="map-loading">Loading map…</div> });

const TABS: Array<{ id: RouteTab; label: string; description: string }> = [
  { id: "within-sg", label: "Within SG", description: "Singapore pickup and dropoff requests" },
  { id: "sg-jb", label: "SG-JB", description: "Cross-border Singapore and Johor Bahru requests" },
];

export default function Home() {
  const [activeTab, setActiveTab] = useState<RouteTab>("within-sg");
  const [requests, setRequests] = useState<TelehitchRequest[]>([]);
  const [status, setStatus] = useState("Loading recent requests…");
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const response = await fetch(`/api/requests/recent?tab=${activeTab}&minutes=360&limit=500`, { cache: "no-store" });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Failed to load requests");
        if (!cancelled) {
          const payload = data as RequestsResponse;
          setRequests(payload.requests);
          setStatus(`${payload.count} requests from the last 6 hours`);
          setUpdatedAt(new Date(payload.generated_at));
        }
      } catch (error) {
        if (!cancelled) setStatus(error instanceof Error ? error.message : "Unable to load requests");
      }
    }
    load();
    const interval = window.setInterval(load, 15000);
    return () => { cancelled = true; window.clearInterval(interval); };
  }, [activeTab]);

  const newest = useMemo(() => requests[0]?.message_date_gmt8, [requests]);

  return (
    <main className="shell">
      <header className="hero">
        <div>
          <p className="eyebrow">Telehitch Insights</p>
          <h1>Live ride request map</h1>
          <p className="hero-copy">Minimal six-hour view of pickup and dropoff demand. Darker blue dots are newer posts; larger dots indicate overlapping pickup or dropoff points.</p>
        </div>
        <div className="stat-card">
          <span>Latest post</span>
          <strong>{newest ? new Date(newest).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "—"}</strong>
          <small>{updatedAt ? `Refreshed ${updatedAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}` : "Auto-refreshes every 15s"}</small>
        </div>
      </header>

      <nav className="tabs" aria-label="Route tabs">
        {TABS.map((tab) => (
          <button key={tab.id} className={activeTab === tab.id ? "tab active" : "tab"} onClick={() => setActiveTab(tab.id)}>
            <span>{tab.label}</span>
            <small>{tab.description}</small>
          </button>
        ))}
      </nav>

      <section className="dashboard-grid">
        <div className="map-panel">
          <div className="panel-heading">
            <div><h2>{TABS.find((tab) => tab.id === activeTab)?.label}</h2><p>{status}</p></div>
            <div className="legend"><span className="dot dark" /> Newer <span className="dot pale" /> Older</div>
          </div>
          <TelehitchMap requests={requests} />
        </div>
        <aside className="feed-panel">
          <div className="panel-heading compact"><div><h2>Recent feed</h2><p>Last 40 posts</p></div></div>
          <RequestFeed requests={requests} />
        </aside>
      </section>
    </main>
  );
}
