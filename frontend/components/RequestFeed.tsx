"use client";

import { NodeDetails, RequestNode, formatSingaporeTime } from "../lib/mapNodes";
import { TelehitchRequest } from "../lib/types";

export default function RequestFeed({ requests, selectedNode }: { requests: TelehitchRequest[]; selectedNode: RequestNode | null }) {
  const visibleRequests = selectedNode?.requests ?? requests.slice(0, 40);
  return (
    <div className="feed-list">
      {visibleRequests.map((request) => (
        <article className="feed-card" key={`${selectedNode?.id ?? "feed"}-${request.gold_request_id}`}>
          {selectedNode ? (
            <NodeDetails request={request} kind={selectedNode.kind} />
          ) : (
            <>
              <div className="feed-card-topline">
                <span>{request.request_type || "request"}</span>
                <time>{formatSingaporeTime(request.message_date_gmt8, false)}</time>
              </div>
              <h3>{request.pickup_location || "Unknown pickup"} → {request.dropoff_location || "Unknown dropoff"}</h3>
              <p>{request.pax_count ? `${request.pax_count} pax` : "Pax not parsed"} · {request.channel || "Unknown channel"}</p>
            </>
          )}
        </article>
      ))}
    </div>
  );
}
