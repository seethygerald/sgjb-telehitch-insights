import { NextRequest, NextResponse } from "next/server";
import { isWithinServiceWindow, serviceWindowMessage } from "../../../lib/availability";
import { fetchDashboardMetrics } from "../../../lib/databricks";
import { RouteTab } from "../../../lib/types";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function parseTab(value: string | null): RouteTab {
  return value === "sg-jb" ? "sg-jb" : "within-sg";
}

export async function GET(request: NextRequest) {
  const tab = parseTab(request.nextUrl.searchParams.get("tab"));

  if (!isWithinServiceWindow()) {
    return NextResponse.json({ error: serviceWindowMessage() }, { status: 503, headers: { "Cache-Control": "no-store, max-age=0" } });
  }

  try {
    const metrics = await fetchDashboardMetrics();
    return NextResponse.json({
      generated_at: new Date().toISOString(),
      tab,
      ...metrics,
    }, { headers: { "Cache-Control": "no-store, max-age=0" } });
  } catch (error) {
    console.error("Dashboard Databricks request failed", error);
    return NextResponse.json({ error: serviceWindowMessage() }, { status: 503, headers: { "Cache-Control": "no-store, max-age=0" } });
  }
}
