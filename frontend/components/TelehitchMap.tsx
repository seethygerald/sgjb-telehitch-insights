"use client";

import { Fragment } from "react";
import { CircleMarker, MapContainer, Polyline, Popup, TileLayer } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import { TelehitchRequest } from "../lib/types";

const TILE_URL = process.env.NEXT_PUBLIC_ONEMAP_TILE_URL || "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";
const TILE_ATTRIBUTION = process.env.NEXT_PUBLIC_ONEMAP_TILE_URL ? "© OneMap, Singapore Land Authority" : "© OpenStreetMap contributors";

function ageRatio(date: string) {
  const ageMs = Date.now() - new Date(date).getTime();
  return Math.min(Math.max(ageMs / (6 * 60 * 60 * 1000), 0), 1);
}

function pointKey(lat: number, lng: number) {
  return `${lat.toFixed(5)},${lng.toFixed(5)}`;
}

export default function TelehitchMap({ requests }: { requests: TelehitchRequest[] }) {
  const pickupCounts = new Map<string, number>();
  const dropoffCounts = new Map<string, number>();
  for (const request of requests) {
    pickupCounts.set(pointKey(request.pickup_latitude, request.pickup_longitude), (pickupCounts.get(pointKey(request.pickup_latitude, request.pickup_longitude)) ?? 0) + 1);
    dropoffCounts.set(pointKey(request.dropoff_latitude, request.dropoff_longitude), (dropoffCounts.get(pointKey(request.dropoff_latitude, request.dropoff_longitude)) ?? 0) + 1);
  }

  return (
    <MapContainer center={[1.3521, 103.8198]} zoom={11} minZoom={9} className="map-canvas" scrollWheelZoom>
      <TileLayer attribution={TILE_ATTRIBUTION} url={TILE_URL} />
      {requests.map((request) => {
        const freshness = 1 - ageRatio(request.message_date_gmt8);
        const color = `rgba(15, ${Math.round(72 + freshness * 70)}, ${Math.round(160 + freshness * 70)}, ${0.22 + freshness * 0.78})`;
        const pickupCount = pickupCounts.get(pointKey(request.pickup_latitude, request.pickup_longitude)) ?? 1;
        const dropoffCount = dropoffCounts.get(pointKey(request.dropoff_latitude, request.dropoff_longitude)) ?? 1;
        const route = [[request.pickup_latitude, request.pickup_longitude], [request.dropoff_latitude, request.dropoff_longitude]] as [number, number][];

        return (
          <Fragment key={request.gold_request_id}>
            <Polyline positions={route} pathOptions={{ color, weight: 2, opacity: 0.35 }} />
            <CircleMarker center={route[0]} radius={5 + Math.min(pickupCount, 12)} pathOptions={{ color, fillColor: color, fillOpacity: 0.82, weight: 1 }}>
              <Popup><PopupBody request={request} kind="Pickup" /></Popup>
            </CircleMarker>
            <CircleMarker center={route[1]} radius={5 + Math.min(dropoffCount, 12)} pathOptions={{ color, fillColor: color, fillOpacity: 0.52, weight: 1, dashArray: "3" }}>
              <Popup><PopupBody request={request} kind="Dropoff" /></Popup>
            </CircleMarker>
          </Fragment>
        );
      })}
    </MapContainer>
  );
}

function PopupBody({ request, kind }: { request: TelehitchRequest; kind: "Pickup" | "Dropoff" }) {
  const location = kind === "Pickup" ? request.pickup_location : request.dropoff_location;
  const address = kind === "Pickup" ? request.pickup_formatted_address : request.dropoff_formatted_address;
  const postal = kind === "Pickup" ? request.pickup_postal_code : request.dropoff_postal_code;
  return (
    <div className="popup-card">
      <strong>{kind}: {location || "Unknown"}</strong>
      <span>{address || "No formatted address"}</span>
      {postal ? <span>Postal: {postal}</span> : null}
      <span>{new Date(request.message_date_gmt8).toLocaleString()}</span>
      <span>{request.pax_count ? `${request.pax_count} pax` : "Pax unknown"} · {request.channel || "Unknown channel"}</span>
      {request.message ? <p>{request.message}</p> : null}
    </div>
  );
}
