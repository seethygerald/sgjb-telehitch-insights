import { NextRequest, NextResponse } from "next/server";
import { fetchRecentRequests, fetchGlobalTrackedRequestCount } from "../../../../lib/databricks";
import { RouteTab } from "../../../../lib/types";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

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
  const limit = parsePositiveInteger(params.get("limit"), 500, 1000);
  const tab = parseTab(params.get("tab"));

  try {
    const [requests, trackedCount] = await Promise.all([
      fetchRecentRequests(minutes, tab, limit),
      fetchGlobalTrackedRequestCount(minutes),
    ]);
    return NextResponse.json({
      generated_at: new Date().toISOString(),
      minutes,
      tab,
      count: requests.length,
      tracked_count: trackedCount,
      requests,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
