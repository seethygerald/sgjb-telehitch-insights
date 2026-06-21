# Telehitch Insights Frontend

A Vercel-ready Next.js app that visualizes `workspace.gold.gold_telehitch_requests` as a minimal live map.

## Architecture

Browser requests never talk to Databricks directly. The app uses this flow:

1. Next.js client polls `/api/requests/recent?tab=within-sg&minutes=360` every 15 seconds.
2. The Vercel API route runs on the server and calls the Databricks SQL Statement Execution API.
3. Databricks returns recent rows from `workspace.gold.gold_telehitch_requests` with complete pickup/dropoff coordinates for the active tab, plus a total request count for the same time window.
4. The browser renders pickup/dropoff dots and route lines on a Leaflet map.

## Local development

```bash
cd frontend
cp .env.example .env.local
npm install
npm run dev
```

Open <http://localhost:3000>.

## Required Vercel environment variables

Set these as encrypted Vercel project environment variables:

- `DATABRICKS_HOST` — Databricks workspace URL, for example `https://adb-....azuredatabricks.net`.
- `DATABRICKS_TOKEN` — Databricks PAT or OAuth token with permission to query the SQL warehouse.
- `DATABRICKS_SQL_WAREHOUSE_ID` — SQL warehouse ID used by the Statement Execution API.
- `DATABRICKS_CATALOG` — defaults to `workspace`.
- `DATABRICKS_SCHEMA` — defaults to `gold`.
- `DATABRICKS_TABLE` — defaults to `gold_telehitch_requests`.

Map tiles:

- The map uses OneMap Night XYZ tiles at `https://www.onemap.gov.sg/maps/tiles/Night/{z}/{x}/{y}.png`.

## Deploying to free Vercel

1. Import this repository into Vercel.
2. Set the project root directory to `frontend`.
3. Keep the build command as `npm run build` and output settings as the Next.js defaults.
4. Add the environment variables above for Production, Preview, and Development as needed.
5. Deploy.

## UI behavior

- Two tabs: `Within SG` and `SG-JB`.
- Both tabs show mappable rows from the last 6 hours for the active tab and display that count alongside the total request count for the same six-hour window.
- Newer requests render in darker blue; older requests fade toward pale blue.
- Orange pickup and dropoff nodes blink on the map.
- Node radius increases when multiple requests share the same rounded pickup or dropoff coordinate.
- Pickup and dropoff points are connected by blinking route lines animated from pickup toward dropoff.
