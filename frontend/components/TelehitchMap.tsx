"use client";

import { useMemo } from "react";
import { divIcon } from "leaflet";
import { MapContainer, Marker, Polyline, TileLayer, Tooltip, useMapEvents } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import { TelehitchRequest } from "../lib/types";
import { NodeDetails, RequestNode, NodeKind } from "../lib/mapNodes";

const TILE_URL = "https://www.onemap.gov.sg/maps/tiles/GreyLite/{z}/{x}/{y}.png";
const TILE_ATTRIBUTION =
  '<img src="https://www.onemap.gov.sg/web-assets/images/logo/om_logo.png" style="height:20px;width:20px;"/>&nbsp;<a href="https://www.onemap.gov.sg/" target="_blank" rel="noopener noreferrer">OneMap</a>&nbsp;&copy;&nbsp;contributors&nbsp;&#124;&nbsp;<a href="https://www.sla.gov.sg/" target="_blank" rel="noopener noreferrer">Singapore Land Authority</a>';

function pointKey(lat: number, lng: number) {
  return `${lat.toFixed(5)},${lng.toFixed(5)}`;
}

function nodeKey(request: TelehitchRequest, kind: NodeKind) {
  return kind === "pickup"
    ? pointKey(request.pickup_latitude, request.pickup_longitude)
    : pointKey(request.dropoff_latitude, request.dropoff_longitude);
}

function buildNodes(requests: TelehitchRequest[]) {
  const nodes = new Map<string, RequestNode>();
  for (const request of requests) {
    for (const kind of ["pickup", "dropoff"] as const) {
      const lat = kind === "pickup" ? request.pickup_latitude : request.dropoff_latitude;
      const lng = kind === "pickup" ? request.pickup_longitude : request.dropoff_longitude;
      const id = `${kind}:${nodeKey(request, kind)}`;
      const existing = nodes.get(id);
      if (existing) {
        existing.requests.push(request);
      } else {
        nodes.set(id, { id, kind, position: [lat, lng], requests: [request] });
      }
    }
  }
  return Array.from(nodes.values());
}

function nodeIcon(count: number, kind: NodeKind) {
  const size = 18 + Math.min(count - 1, 10) * 3;
  return divIcon({
    className: "telehitch-node-wrapper",
    html: `<span class="telehitch-node telehitch-node-${kind}" style="width:${size}px;height:${size}px"><span>${count > 1 ? count : ""}</span></span>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
}

function MapBackgroundClick({ onClearSelection }: { onClearSelection: () => void }) {
  useMapEvents({ click: onClearSelection });
  return null;
}

export default function TelehitchMap({ requests, onSelectNode, onClearSelection }: { requests: TelehitchRequest[]; onSelectNode: (node: RequestNode) => void; onClearSelection: () => void }) {
  const nodes = useMemo(() => buildNodes(requests), [requests]);

  return (
    <MapContainer center={[1.3521, 103.8198]} zoom={11} minZoom={11} maxZoom={19} className="map-canvas" scrollWheelZoom>
      <MapBackgroundClick onClearSelection={onClearSelection} />
      <TileLayer attribution={TILE_ATTRIBUTION} url={TILE_URL} detectRetina maxZoom={19} minZoom={11} />
      {requests.map((request) => (
        <Polyline
          key={`route-${request.gold_request_id}`}
          positions={[[request.pickup_latitude, request.pickup_longitude], [request.dropoff_latitude, request.dropoff_longitude]]}
          pathOptions={{ color: "#0f56b3", weight: 3, opacity: 0.7, className: "telehitch-route" }}
        />
      ))}
      {nodes.map((node) => (
        <Marker
          key={node.id}
          position={node.position}
          icon={nodeIcon(node.requests.length, node.kind)}
          eventHandlers={{ click: (event) => { event.originalEvent.stopPropagation(); onSelectNode(node); } }}
        >
          <Tooltip direction="top" offset={[0, -8]} opacity={1} sticky>
            {node.requests.length > 1 ? <strong>{node.requests.length} requests (click for more info)</strong> : <NodeDetails request={node.requests[0]} kind={node.kind} />}
          </Tooltip>
        </Marker>
      ))}
    </MapContainer>
  );
}
