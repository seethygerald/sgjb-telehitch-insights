import { DatabricksStatementResponse, RouteTab, TelehitchRequest } from "./types";

const SELECT_COLUMNS = [
  "gold_request_id", "silver_request_id", "channel", "topic_id", "message_id", "message_date_gmt8", "scraped_at_gmt8",
  "sender_id", "sender_handle", "request_type", "request_time_text", "pax_count", "pickup_location", "dropoff_location",
  "normalized_pickup_location", "normalized_dropoff_location", "pickup_postal_code", "pickup_latitude", "pickup_longitude",
  "pickup_formatted_address", "pickup_resolution_source", "dropoff_postal_code", "dropoff_latitude", "dropoff_longitude",
  "dropoff_formatted_address", "dropoff_resolution_source", "message",
] as const;

const REQUIRED_ENV = ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_SQL_WAREHOUSE_ID"] as const;

function getDatabricksConfig() {
  const missing = REQUIRED_ENV.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required Databricks environment variables: ${missing.join(", ")}`);
  }

  return {
    host: process.env.DATABRICKS_HOST!.replace(/\/$/, ""),
    token: process.env.DATABRICKS_TOKEN!,
    warehouseId: process.env.DATABRICKS_SQL_WAREHOUSE_ID!,
    catalog: process.env.DATABRICKS_CATALOG ?? "workspace",
    schema: process.env.DATABRICKS_SCHEMA ?? "gold",
    table: process.env.DATABRICKS_TABLE ?? "gold_telehitch_requests",
  };
}

function tableName() {
  const { catalog, schema, table } = getDatabricksConfig();
  return `\`${catalog}\`.\`${schema}\`.\`${table}\``;
}

function tabFilter(tab: RouteTab) {
  if (tab === "within-sg") {
    return `AND upper(coalesce(pickup_resolution_source, '')) NOT LIKE '%MALAYSIA%'
      AND upper(coalesce(dropoff_resolution_source, '')) NOT LIKE '%MALAYSIA%'
      AND coalesce(pickup_postal_code, '') RLIKE '^[0-9]{6}$'
      AND coalesce(dropoff_postal_code, '') RLIKE '^[0-9]{6}$'`;
  }

  return `AND (
      upper(coalesce(normalized_pickup_location, pickup_location, '')) RLIKE '(JB|JOHOR|MALAYSIA|MY)'
      OR upper(coalesce(normalized_dropoff_location, dropoff_location, '')) RLIKE '(JB|JOHOR|MALAYSIA|MY)'
      OR coalesce(pickup_postal_code, '') NOT RLIKE '^[0-9]{6}$'
      OR coalesce(dropoff_postal_code, '') NOT RLIKE '^[0-9]{6}$'
    )`;
}

function buildRecentSql(minutes: number, tab: RouteTab, limit: number) {
  return `SELECT ${SELECT_COLUMNS.join(",\n       ")}
FROM ${tableName()}
WHERE message_date_gmt8 >= from_utc_timestamp(current_timestamp(), 'Asia/Singapore') - interval ${minutes} minutes
  AND lower(coalesce(request_type, '')) = 'hitcher'
  AND pickup_latitude IS NOT NULL
  AND pickup_longitude IS NOT NULL
  AND dropoff_latitude IS NOT NULL
  AND dropoff_longitude IS NOT NULL
  ${tabFilter(tab)}
ORDER BY message_date_gmt8 DESC
LIMIT ${limit}`;
}

function buildTrackedCountSql(minutes: number, tab: RouteTab) {
  return `SELECT count(*) AS tracked_count
FROM ${tableName()}
WHERE message_date_gmt8 >= from_utc_timestamp(current_timestamp(), 'Asia/Singapore') - interval ${minutes} minutes
  AND lower(coalesce(request_type, '')) = 'hitcher'
  ${tabFilter(tab)}`;
}

async function executeStatement(statement: string): Promise<DatabricksStatementResponse> {
  const { host, token, warehouseId } = getDatabricksConfig();
  const response = await fetch(`${host}/api/2.0/sql/statements`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ statement, warehouse_id: warehouseId, wait_timeout: "30s", disposition: "INLINE", format: "JSON_ARRAY" }),
    cache: "no-store",
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Databricks SQL request failed (${response.status}): ${body}`);
  }

  const initial = (await response.json()) as DatabricksStatementResponse;
  if (initial.status.state === "SUCCEEDED" || initial.status.state === "FAILED") return initial;

  const deadline = Date.now() + 25000;
  let current = initial;
  while (["PENDING", "RUNNING"].includes(current.status.state) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 1000));
    const poll = await fetch(`${host}/api/2.0/sql/statements/${current.statement_id}`, {
      headers: { Authorization: `Bearer ${token}` },
      cache: "no-store",
    });
    if (!poll.ok) throw new Error(`Databricks SQL poll failed (${poll.status}): ${await poll.text()}`);
    current = (await poll.json()) as DatabricksStatementResponse;
  }
  return current;
}

function parseNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function parseString(value: unknown): string | null {
  if (value === null || value === undefined || value === "") return null;
  return String(value);
}

function parseSingaporeTimestamp(value: string) {
  const hasTimezone = /(?:Z|[+-]\d{2}:?\d{2})$/.test(value);
  return new Date(hasTimezone ? value : `${value.replace(" ", "T")}+08:00`).getTime();
}

function isWithinRecentWindow(request: TelehitchRequest, minutes: number) {
  const messageTime = parseSingaporeTimestamp(request.message_date_gmt8);
  if (!Number.isFinite(messageTime)) return false;
  return messageTime >= Date.now() - minutes * 60 * 1000;
}

function rowsToRequests(response: DatabricksStatementResponse): TelehitchRequest[] {
  if (response.status.state !== "SUCCEEDED") {
    throw new Error(response.status.error?.message ?? `Databricks statement ended with ${response.status.state}`);
  }

  const rows = response.result?.data_array ?? [];
  return rows.map((row, rowIndex) => {
    const record = Object.fromEntries(SELECT_COLUMNS.map((name, index) => [name, row[index] ?? null])) as Record<string, unknown>;
    return {
      gold_request_id: parseString(record.gold_request_id) ?? `row-${rowIndex}`,
      silver_request_id: parseString(record.silver_request_id),
      channel: parseString(record.channel),
      topic_id: parseString(record.topic_id),
      message_id: parseString(record.message_id),
      message_date_gmt8: parseString(record.message_date_gmt8) ?? new Date().toISOString(),
      scraped_at_gmt8: parseString(record.scraped_at_gmt8),
      sender_id: parseString(record.sender_id),
      sender_handle: parseString(record.sender_handle),
      request_type: parseString(record.request_type),
      request_time_text: parseString(record.request_time_text),
      pax_count: parseNumber(record.pax_count),
      pickup_location: parseString(record.pickup_location),
      dropoff_location: parseString(record.dropoff_location),
      normalized_pickup_location: parseString(record.normalized_pickup_location),
      normalized_dropoff_location: parseString(record.normalized_dropoff_location),
      pickup_postal_code: parseString(record.pickup_postal_code),
      pickup_latitude: parseNumber(record.pickup_latitude) ?? 0,
      pickup_longitude: parseNumber(record.pickup_longitude) ?? 0,
      pickup_formatted_address: parseString(record.pickup_formatted_address),
      pickup_resolution_source: parseString(record.pickup_resolution_source),
      dropoff_postal_code: parseString(record.dropoff_postal_code),
      dropoff_latitude: parseNumber(record.dropoff_latitude) ?? 0,
      dropoff_longitude: parseNumber(record.dropoff_longitude) ?? 0,
      dropoff_formatted_address: parseString(record.dropoff_formatted_address),
      dropoff_resolution_source: parseString(record.dropoff_resolution_source),
      message: parseString(record.message),
    };
  });
}

export async function fetchRecentRequests(minutes: number, tab: RouteTab, limit: number) {
  const statement = buildRecentSql(minutes, tab, limit);
  const response = await executeStatement(statement);
  return rowsToRequests(response).filter((request) => isWithinRecentWindow(request, minutes));
}

export async function fetchTrackedRequestCount(minutes: number, tab: RouteTab) {
  const statement = buildTrackedCountSql(minutes, tab);
  const response = await executeStatement(statement);
  if (response.status.state !== "SUCCEEDED") {
    throw new Error(response.status.error?.message ?? `Databricks statement ended with ${response.status.state}`);
  }

  return parseNumber(response.result?.data_array?.[0]?.[0]) ?? 0;
}
