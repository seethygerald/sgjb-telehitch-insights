"use client";

import { useState } from "react";
import { RequestNode, formatSingaporeTime } from "../lib/mapNodes";
import { TelehitchRequest } from "../lib/types";

function formatRequestType(value: string | null) {
  const label = (value || "request").replace(/_/g, " ").toLowerCase();
  return label.charAt(0).toUpperCase() + label.slice(1);
}

function pickupTimeLabel(request: TelehitchRequest) {
  return request.request_time_text?.trim() || "now";
}

function locationLine(request: TelehitchRequest) {
  return `${request.pickup_location || "Unknown pickup"} → ${request.dropoff_location || "Unknown dropoff"}`;
}

function paxLabel(request: TelehitchRequest) {
  return request.pax_count ? `${request.pax_count} pax` : "Pax not parsed";
}

function RequestCard({ request, expanded, onToggle }: { request: TelehitchRequest; expanded: boolean; onToggle: () => void }) {
  const pickupAddress = request.pickup_formatted_address || request.pickup_location || "Unknown pickup";
  const dropoffAddress = request.dropoff_formatted_address || request.dropoff_location || "Unknown dropoff";
  const pickupPostal = request.pickup_postal_code;
  const dropoffPostal = request.dropoff_postal_code;

  return (
    <article className={`feed-card${expanded ? " expanded" : ""}`}>
      <button className="feed-card-button" type="button" onClick={onToggle} aria-expanded={expanded}>
        <div className="feed-card-topline">
          <span>{formatRequestType(request.request_type)}</span>
          <time>{formatSingaporeTime(request.message_date_gmt8, false)}</time>
        </div>
        <h3>{locationLine(request)}</h3>
        {expanded ? (
          <div className="feed-card-details">
            <p>{pickupAddress}</p>
            {pickupPostal ? <p>{pickupPostal}</p> : null}
            <p>{dropoffAddress}</p>
            {dropoffPostal ? <p>{dropoffPostal}</p> : null}
          </div>
        ) : null}
        <p>{paxLabel(request)} · {request.channel || "Unknown channel"} · {pickupTimeLabel(request)}</p>
      </button>
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
      {visibleRequests.map((request) => (
        <RequestCard
          key={`${selectedNode?.id ?? "feed"}-${request.gold_request_id}`}
          request={request}
          expanded={expandedIds.has(`${selectedNode?.id ?? "feed"}-${request.gold_request_id}`)}
          onToggle={() => toggleRequest(`${selectedNode?.id ?? "feed"}-${request.gold_request_id}`)}
        />
      ))}
    </div>
  );
}
