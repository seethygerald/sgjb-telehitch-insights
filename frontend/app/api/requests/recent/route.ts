import { NextRequest, NextResponse } from "next/server";
import { fetchRecentRequests, fetchTotalRequestCount } from "../../../../lib/databricks";
import { RouteTab } from "../../../../lib/types";

const MAINTENANCE_MESSAGE = "The app is currently going through maintenance. Please try again in several hours.";

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
    const [requests, totalCount] = await Promise.all([
      fetchRecentRequests(minutes, tab, limit),
      fetchTotalRequestCount(minutes),
    ]);
    return NextResponse.json({
      generated_at: new Date().toISOString(),
      minutes,
      tab,
      count: requests.length,
      total_count: totalCount,
      requests,
    });
  } catch (error) {
    console.error("Recent requests Databricks request failed", error);
    return NextResponse.json({ error: MAINTENANCE_MESSAGE }, { status: 503 });
  }
}
