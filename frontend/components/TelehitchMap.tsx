"use client";

import { useMemo } from "react";
import { divIcon, DomEvent, LeafletMouseEvent } from "leaflet";
import { MapContainer, Marker, Polyline, TileLayer, Tooltip, useMapEvents } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import { TelehitchRequest } from "../lib/types";
import { NodeDetails, RequestNode, NodeKind, parseSingaporeDate } from "../lib/mapNodes";

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

function recencyScoreForTimestamp(timestamp: string) {
  const parsed = parseSingaporeDate(timestamp).getTime();
  if (!Number.isFinite(parsed)) return 0;
  const ageHours = Math.max(0, Math.min(6, (Date.now() - parsed) / (60 * 60 * 1000)));
  return 1 - ageHours / 6;
}

function recencyColor(score: number) {
  const lightness = Math.round(72 - score * 36);
  const saturation = Math.round(76 + score * 14);
  return `hsl(213 ${saturation}% ${lightness}%)`;
}

function nodeRecencyScore(node: RequestNode) {
  const newestTimestamp = Math.max(...node.requests.map((request) => parseSingaporeDate(request.message_date_gmt8).getTime()).filter(Number.isFinite));
  if (!Number.isFinite(newestTimestamp)) return 0;
  return recencyScoreForTimestamp(new Date(newestTimestamp).toISOString());
}

function routeRecencyScore(request: TelehitchRequest) {
  return recencyScoreForTimestamp(request.message_date_gmt8);
}

function nodeIcon(node: RequestNode) {
  const count = node.requests.length;
  const size = Math.round(18 + Math.sqrt(Math.max(count - 1, 0)) * 9 + Math.min(count - 1, 25));
  const recency = nodeRecencyScore(node);
  const color = recencyColor(recency);
  return divIcon({
    className: "telehitch-node-wrapper",
    html: `<span class="telehitch-node telehitch-node-${node.kind}" style="width:${size}px;height:${size}px;--node-color:${color};--node-shadow:rgba(15,86,179,${0.18 + recency * 0.42})"><span>${count > 1 ? count : ""}</span></span>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
}

function MapBackgroundClick({ onClearSelection }: { onClearSelection: () => void }) {
  useMapEvents({
    click: (event: LeafletMouseEvent) => {
      const target = event.originalEvent.target;
      if (target instanceof HTMLElement && target.closest(".telehitch-node-wrapper, .leaflet-interactive")) return;
      onClearSelection();
    },
  });
  return null;
}

export default function TelehitchMap({ requests, onSelectNode, onClearSelection }: { requests: TelehitchRequest[]; onSelectNode: (node: RequestNode) => void; onClearSelection: () => void }) {
  const nodes = useMemo(() => buildNodes(requests), [requests]);

  return (
    <MapContainer center={[1.3521, 103.8198]} zoom={11} minZoom={11} maxZoom={19} className="map-canvas" scrollWheelZoom>
      <MapBackgroundClick onClearSelection={onClearSelection} />
      <TileLayer attribution={TILE_ATTRIBUTION} url={TILE_URL} detectRetina maxZoom={19} minZoom={11} />
      {requests.map((request) => {
        const recency = routeRecencyScore(request);
        return (
          <Polyline
            key={`route-${request.gold_request_id}`}
            positions={[[request.pickup_latitude, request.pickup_longitude], [request.dropoff_latitude, request.dropoff_longitude]]}
            pathOptions={{ color: recencyColor(recency), weight: 3, dashArray: "2 12", lineCap: "round", className: "telehitch-route" }}
          />
        );
      })}
      {nodes.map((node) => (
        <Marker
          key={node.id}
          position={node.position}
          icon={nodeIcon(node)}
          eventHandlers={{ click: (event) => { DomEvent.stop(event); onSelectNode(node); } }}
        >
          <Tooltip direction="top" offset={[0, -8]} opacity={1} sticky>
            {node.requests.length > 1 ? <strong>{node.requests.length} requests (click for more info)</strong> : <NodeDetails request={node.requests[0]} kind={node.kind} />}
          </Tooltip>
        </Marker>
      ))}
    </MapContainer>
  );
}
