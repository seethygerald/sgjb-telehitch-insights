"use client";

import { KeyboardEvent, useState } from "react";
import { RequestNode, formatSingaporeTime, parseSingaporeDate } from "../lib/mapNodes";
import { TelehitchRequest } from "../lib/types";

function formatRequestType(value: string | null) {
  const label = (value || "request").replace(/_/g, " ").toLowerCase();
  return label.charAt(0).toUpperCase() + label.slice(1);
}

function pickupTimeLabel(request: TelehitchRequest) {
  const raw = request.request_time_text?.trim();
  if (!raw || /^now$/i.test(raw)) return "Now";

  const pickupTime = parseSingaporeDate(raw).getTime();
  const requestTime = parseSingaporeDate(request.message_date_gmt8).getTime();
  if (Number.isFinite(pickupTime) && Number.isFinite(requestTime) && Math.abs(pickupTime - requestTime) < 60 * 1000) {
    return "Now";
  }

  return raw;
}

function locationLine(request: TelehitchRequest) {
  return `${request.pickup_location || "Unknown pickup"} → ${request.dropoff_location || "Unknown dropoff"}`;
}

function paxLabel(request: TelehitchRequest) {
  return request.pax_count ? `${request.pax_count} pax` : "Pax not parsed";
}

function hasResolvedAddress(request: TelehitchRequest) {
  return Boolean(request.pickup_formatted_address || request.dropoff_formatted_address);
}

function RequestCard({ request, expanded, onToggle }: { request: TelehitchRequest; expanded: boolean; onToggle: () => void }) {
  const pickupAddress = request.pickup_formatted_address;
  const dropoffAddress = request.dropoff_formatted_address;
  const expandable = hasResolvedAddress(request);

  function handleKeyDown(event: KeyboardEvent<HTMLElement>) {
    if (!expandable) return;
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onToggle();
    }
  }

  return (
    <article
      className={`feed-card${expanded ? " expanded" : ""}${expandable ? " clickable" : ""}`}
      onClick={expandable ? onToggle : undefined}
      onKeyDown={handleKeyDown}
      role={expandable ? "button" : undefined}
      tabIndex={expandable ? 0 : undefined}
      aria-expanded={expandable ? expanded : undefined}
    >
      <div className="feed-card-topline">
        <span>{formatRequestType(request.request_type)}</span>
        <time>{formatSingaporeTime(request.message_date_gmt8, false)}</time>
      </div>
      <h3>{locationLine(request)}</h3>
      {expanded ? (
        <div className="feed-card-details">
          {pickupAddress ? <p><strong>Pick-up:</strong> {pickupAddress}</p> : null}
          {dropoffAddress ? <p><strong>Drop-off:</strong> {dropoffAddress}</p> : null}
        </div>
      ) : null}
      <p>{paxLabel(request)} · {request.channel || "Unknown channel"} · {pickupTimeLabel(request)}</p>
    </article>
  );
}

export default function RequestFeed({ requests, selectedNode }: { requests: TelehitchRequest[]; selectedNode: RequestNode | null }) {
  const [expandedIds, setExpandedIds] = useState<Set<string>>(() => new Set());
  const visibleRequests = selectedNode?.requests ?? requests;

  function toggleRequest(requestId: string) {
    setExpandedIds((current) => {
      const next = new Set(current);
      if (next.has(requestId)) next.delete(requestId);
      else next.add(requestId);
      return next;
    });
  }

  return (
    <div className="feed-list">
      {visibleRequests.map((request) => {
        const requestKey = `${selectedNode?.id ?? "feed"}-${request.gold_request_id}`;
        return <RequestCard key={requestKey} request={request} expanded={expandedIds.has(requestKey)} onToggle={() => toggleRequest(requestKey)} />;
      })}
    </div>
  );
}
