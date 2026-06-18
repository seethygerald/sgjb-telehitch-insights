"use client";

import { TelehitchRequest } from "../lib/types";

export default function RequestFeed({ requests }: { requests: TelehitchRequest[] }) {
  return (
    <div className="feed-list">
      {requests.slice(0, 40).map((request) => (
        <article className="feed-card" key={request.gold_request_id}>
          <div className="feed-card-topline">
            <span>{request.request_type || "request"}</span>
            <time>{new Date(request.message_date_gmt8).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</time>
          </div>
          <h3>{request.pickup_location || "Unknown pickup"} → {request.dropoff_location || "Unknown dropoff"}</h3>
          <p>{request.pax_count ? `${request.pax_count} pax` : "Pax not parsed"} · {request.channel || "Unknown channel"}</p>
        </article>
      ))}
    </div>
  );
}
