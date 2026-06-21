import { TelehitchRequest } from "./types";

export type NodeKind = "pickup" | "dropoff";
export type RequestNode = {
  id: string;
  kind: NodeKind;
  position: [number, number];
  requests: TelehitchRequest[];
};

export function parseSingaporeDate(value: string) {
  const hasTimezone = /(?:Z|[+-]\d{2}:?\d{2})$/.test(value);
  const normalized = hasTimezone ? value : `${value.replace(" ", "T")}+08:00`;
  return new Date(normalized);
}

export function formatSingaporeTime(value: string, includePeriod = true) {
  return parseSingaporeDate(value).toLocaleTimeString("en-SG", {
    timeZone: "Asia/Singapore",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    hour12: includePeriod,
  });
}

export function detailRows(request: TelehitchRequest, kind: NodeKind) {
  const isPickup = kind === "pickup";
  const hasFormattedAddress = Boolean(isPickup ? request.pickup_formatted_address : request.dropoff_formatted_address);
  const address = isPickup
    ? request.pickup_formatted_address || request.pickup_location || "Unknown pickup"
    : request.dropoff_formatted_address || request.dropoff_location || "Unknown drop-off";
  const postal = isPickup ? request.pickup_postal_code : request.dropoff_postal_code;
  const baseRows = [
    { label: isPickup ? "Pick-up address" : "Drop-off address", value: address },
    { label: isPickup && hasFormattedAddress ? "Postal Code" : "Postal code", value: postal || "—" },
  ];

  if (isPickup) {
    return [
      ...baseRows,
      { label: "Request made at", value: formatSingaporeTime(request.message_date_gmt8, true) },
      { label: "Pick-up time", value: request.request_time_text || "—" },
      { label: "Number of pax", value: request.pax_count?.toString() || "—" },
    ];
  }

  const requestMadeRow = { label: "Request made at", value: formatSingaporeTime(request.message_date_gmt8, false) };
  const paxRow = { label: "Number of pax", value: request.pax_count?.toString() || "—" };
  return hasFormattedAddress ? [...baseRows, paxRow, requestMadeRow] : [...baseRows, requestMadeRow, paxRow];
}

export function NodeDetails({ request, kind }: { request: TelehitchRequest; kind: NodeKind }) {
  return (
    <div className="node-detail-card">
      {detailRows(request, kind).map((row) => (
        <div key={row.label}><strong>{row.label}:</strong> {row.value}</div>
      ))}
    </div>
  );
}
