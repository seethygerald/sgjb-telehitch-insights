# Location overrides runbook

## What updates what?

`workspace.silver.location_overrides` does not update Gold directly. The flow is:

1. Populate or edit `workspace.silver.location_overrides`.
2. Run `databricks/geocode_silver_locations.py`.
   - At the start of the notebook, overrides are merged into `workspace.silver.location_geocodes`.
   - Override rows are marked with `resolution_source = 'override'`.
3. Run the dbt Gold model.
   - Gold reads `workspace.silver.location_geocodes`.
   - When more than one geocode exists for the same normalized location, Gold ranks `override` first.

For historical rows already present in Gold, use a Gold full refresh or temporarily widen `gold_incremental_lookback_hours`; otherwise only rows in the current incremental lookback are rebuilt.

## One-time setup

1. Make sure the Databricks OneMap secret scope has `email`, `password`, and optionally an existing `access-token`.
2. Import `databricks/populate_location_overrides.py` into Databricks as a notebook, or run it as a Databricks Python task.
3. Use these widget defaults unless your table names differ:
   - `catalog = workspace`
   - `silver_schema = silver`
   - `overrides_table = location_overrides`
   - `secret_scope = telehitch-onemap`
   - `dry_run = true` for the first run

## First safe run

1. Run `populate_location_overrides.py` with `dry_run = true`.
2. Review the displayed override rows.
3. If the rows look correct, run it again with `dry_run = false`.
4. Check the table:

```sql
SELECT *
FROM workspace.silver.location_overrides
ORDER BY normalized_location;
```

## Refresh geocodes and Gold

1. Run `databricks/geocode_silver_locations.py`.
2. Verify overrides reached the geocode cache:

```sql
SELECT normalized_location, postal_code, formatted_address, resolution_source
FROM workspace.silver.location_geocodes
WHERE resolution_source = 'override'
ORDER BY normalized_location;
```

3. Run dbt Gold. For recent rows, an incremental run is enough. For historical rows, run Gold with full refresh or temporarily widen `gold_incremental_lookback_hours`.
4. Verify Gold uses overrides:

```sql
SELECT
    normalized_pickup_location,
    pickup_postal_code,
    pickup_formatted_address,
    pickup_resolution_source,
    normalized_dropoff_location,
    dropoff_postal_code,
    dropoff_formatted_address,
    dropoff_resolution_source
FROM workspace.gold.gold_telehitch_requests
WHERE pickup_resolution_source = 'override'
   OR dropoff_resolution_source = 'override'
ORDER BY message_date_gmt8 DESC
LIMIT 100;
```
