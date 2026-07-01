import { NextRequest, NextResponse } from "next/server";
import { isWithinServiceWindow, serviceWindowMessage } from "../../../../lib/availability";
import { fetchLatestRequestTime, fetchRecentRequests, fetchTotalRequestCount, fetchUniqueRequestCount } from "../../../../lib/databricks";
import { RouteTab } from "../../../../lib/types";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const revalidate = 0;

function parsePositiveInteger(value: string | null, fallback: number, max: number) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, max);
}

function parseTab(value: string | null): RouteTab {
  return value === "sg-jb" ? "sg-jb" : "within-sg";
}

export async function GET(request: NextRequest) {
  const params = request.nextUrl.searchParams;
  const minutes = parsePositiveInteger(params.get("minutes"), 360, 720);
  const limit = parsePositiveInteger(params.get("limit"), 5000, 5000);
  const tab = parseTab(params.get("tab"));

  if (!isWithinServiceWindow()) {
    return NextResponse.json({ error: serviceWindowMessage() }, { status: 503, headers: { "Cache-Control": "no-store, max-age=0" } });
  }

  try {
    const [requests, totalCount, activeDriverCount, latestPostAt] = await Promise.all([
      fetchRecentRequests(minutes, tab, limit),
      fetchTotalRequestCount(minutes),
      fetchUniqueRequestCount(60, "driver_request"),
      fetchLatestRequestTime(minutes),
    ]);
    return NextResponse.json({
      generated_at: new Date().toISOString(),
      minutes,
      tab,
      count: requests.length,
      total_count: totalCount,
      active_driver_count: activeDriverCount,
      latest_post_at: latestPostAt,
      requests,
    }, { headers: { "Cache-Control": "no-store, max-age=0" } });
  } catch (error) {
    console.error("Recent requests Databricks request failed", error);
    return NextResponse.json({ error: serviceWindowMessage() }, { status: 503, headers: { "Cache-Control": "no-store, max-age=0" } });
  }
}
