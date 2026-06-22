import { DashboardMetric, DatabricksStatementResponse, RequestType, RouteTab, TelehitchRequest } from "./types";

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
  AND request_type = 'hitcher_request'
  AND pickup_latitude IS NOT NULL
  AND pickup_longitude IS NOT NULL
  AND dropoff_latitude IS NOT NULL
  AND dropoff_longitude IS NOT NULL
  ${tabFilter(tab)}
ORDER BY message_date_gmt8 DESC
LIMIT ${limit}`;
}


function buildDashboardSql() {
  return `WITH window_options AS (
  SELECT explode(array(1, 2, 3, 6)) AS window_hours
), request_types AS (
  SELECT explode(array('hitcher_request', 'driver_request')) AS request_type
), buckets AS (
  SELECT explode(sequence(
    date_trunc('MINUTE', from_utc_timestamp(current_timestamp(), 'Asia/Singapore') - interval 24 hours),
    date_trunc('MINUTE', from_utc_timestamp(current_timestamp(), 'Asia/Singapore')),
    interval 15 minutes
  )) AS bucket_start
), base AS (
  SELECT gold_request_id, request_type, message_date_gmt8
  FROM ${tableName()}
  WHERE message_date_gmt8 >= from_utc_timestamp(current_timestamp(), 'Asia/Singapore') - interval 30 hours
    AND request_type IN ('hitcher_request', 'driver_request')
), rolling AS (
  SELECT
    rt.request_type,
    wo.window_hours,
    b.bucket_start,
    count(DISTINCT base.gold_request_id) AS total_count
  FROM request_types rt
  CROSS JOIN window_options wo
  CROSS JOIN buckets b
  LEFT JOIN base
    ON base.request_type = rt.request_type
   AND base.message_date_gmt8 > b.bucket_start - make_interval(0, 0, 0, 0, wo.window_hours, 0, 0)
   AND base.message_date_gmt8 <= b.bucket_start
  GROUP BY rt.request_type, wo.window_hours, b.bucket_start
), live AS (
  SELECT
    rt.request_type,
    count(DISTINCT base.gold_request_id) AS total_count
  FROM request_types rt
  LEFT JOIN base
    ON base.request_type = rt.request_type
   AND base.message_date_gmt8 >= from_utc_timestamp(current_timestamp(), 'Asia/Singapore') - interval 15 minutes
  GROUP BY rt.request_type
), daily AS (
  SELECT
    rt.request_type,
    count(DISTINCT base.gold_request_id) AS total_count
  FROM request_types rt
  LEFT JOIN base
    ON base.request_type = rt.request_type
   AND base.message_date_gmt8 >= date_trunc('DAY', from_utc_timestamp(current_timestamp(), 'Asia/Singapore'))
  GROUP BY rt.request_type
)
SELECT 'rolling' AS metric, request_type, window_hours, CAST(bucket_start AS STRING) AS bucket_start_gmt8, CAST(total_count AS DOUBLE) AS metric_value
FROM rolling
UNION ALL
SELECT 'live_15m' AS metric, request_type, NULL AS window_hours, NULL AS bucket_start_gmt8, CAST(total_count AS DOUBLE) AS metric_value
FROM live
UNION ALL
SELECT 'daily_total' AS metric, request_type, NULL AS window_hours, NULL AS bucket_start_gmt8, CAST(total_count AS DOUBLE) AS metric_value
FROM daily
ORDER BY metric, request_type, window_hours, bucket_start_gmt8`;
}

function buildUniqueRequestCountSql(minutes: number, requestType: RequestType) {
  return `SELECT count(DISTINCT gold_request_id) AS total_count
FROM ${tableName()}
WHERE message_date_gmt8 >= from_utc_timestamp(current_timestamp(), 'Asia/Singapore') - interval ${minutes} minutes
  AND request_type = '${requestType}'`;
}

function buildTotalCountSql(minutes: number) {
  return buildUniqueRequestCountSql(minutes, "hitcher_request");
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
  const normalizedFraction = value.replace(/\.(\d{3})\d+/, ".$1");
  const normalized = (hasTimezone ? normalizedFraction : `${normalizedFraction}+08:00`).replace(" ", "T");
  return new Date(normalized).getTime();
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

export async function fetchTotalRequestCount(minutes: number) {
  const statement = buildTotalCountSql(minutes);
  const response = await executeStatement(statement);
  if (response.status.state !== "SUCCEEDED") {
    throw new Error(response.status.error?.message ?? `Databricks statement ended with ${response.status.state}`);
  }

  return parseNumber(response.result?.data_array?.[0]?.[0]) ?? 0;
}


export async function fetchDashboardMetrics() {
  const response = await executeStatement(buildDashboardSql());
  if (response.status.state !== "SUCCEEDED") {
    throw new Error(response.status.error?.message ?? `Databricks statement ended with ${response.status.state}`);
  }

  const metrics: Record<RequestType, Record<number, DashboardMetric>> = {
    hitcher_request: {},
    driver_request: {},
  };

  for (const requestType of ["hitcher_request", "driver_request"] as const) {
    for (const windowHours of [1, 2, 3, 6]) {
      metrics[requestType][windowHours] = {
        request_type: requestType,
        window_hours: windowHours,
        average_rolling_total: 0,
        current_rolling_total: 0,
        live_15m_count: 0,
        daily_total_count: 0,
        rolling_points: [],
      };
    }
  }

  const liveCounts: Record<RequestType, number> = { hitcher_request: 0, driver_request: 0 };
  const dailyCounts: Record<RequestType, number> = { hitcher_request: 0, driver_request: 0 };
  for (const row of response.result?.data_array ?? []) {
    const metric = parseString(row[0]);
    const requestType = parseString(row[1]) as RequestType | null;
    const windowHours = parseNumber(row[2]);
    const bucket = parseString(row[3]);
    const value = parseNumber(row[4]) ?? 0;
    if (requestType !== "hitcher_request" && requestType !== "driver_request") continue;

    if (metric === "live_15m") {
      liveCounts[requestType] = value;
    } else if (metric === "daily_total") {
      dailyCounts[requestType] = value;
    } else if (metric === "rolling" && bucket && windowHours && metrics[requestType][windowHours]) {
      metrics[requestType][windowHours].rolling_points.push({ bucket_start_gmt8: bucket, total_count: value });
    }
  }

  for (const requestType of ["hitcher_request", "driver_request"] as const) {
    for (const metric of Object.values(metrics[requestType])) {
      metric.live_15m_count = liveCounts[requestType];
      metric.daily_total_count = dailyCounts[requestType];
      metric.current_rolling_total = metric.rolling_points.at(-1)?.total_count ?? 0;
      metric.average_rolling_total = metric.rolling_points.length > 0
        ? metric.rolling_points.reduce((sum, point) => sum + point.total_count, 0) / metric.rolling_points.length
        : 0;
    }
  }

  return { metrics };
}


export async function fetchUniqueRequestCount(minutes: number, requestType: RequestType) {
  const response = await executeStatement(buildUniqueRequestCountSql(minutes, requestType));
  if (response.status.state !== "SUCCEEDED") {
    throw new Error(response.status.error?.message ?? `Databricks statement ended with ${response.status.state}`);
  }

  return parseNumber(response.result?.data_array?.[0]?.[0]) ?? 0;
}
