import { NextRequest, NextResponse } from "next/server";
import { fetchDashboardMetrics } from "../../../lib/databricks";
import { RouteTab } from "../../../lib/types";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const MAINTENANCE_MESSAGE = "The app is currently going through maintenance. Please try again in several hours.";

function parseTab(value: string | null): RouteTab {
  return value === "sg-jb" ? "sg-jb" : "within-sg";
}

export async function GET(request: NextRequest) {
  const tab = parseTab(request.nextUrl.searchParams.get("tab"));

  try {
    const metrics = await fetchDashboardMetrics();
    return NextResponse.json({
      generated_at: new Date().toISOString(),
      tab,
      ...metrics,
    }, { headers: { "Cache-Control": "no-store, max-age=0" } });
  } catch (error) {
    console.error("Dashboard Databricks request failed", error);
    return NextResponse.json({ error: MAINTENANCE_MESSAGE }, { status: 503, headers: { "Cache-Control": "no-store, max-age=0" } });
  }
}
