# Databricks notebook source
"""Populate manual location overrides for broad Singapore town names.

This one-off Databricks notebook/script resolves broad town names such as
"sengkang" and "queenstown" to their MRT station OneMap result and upserts
those rows into `workspace.silver.location_overrides` by default.

Refresh order:
1. Run this script once to populate/update `location_overrides`.
2. Run `databricks/geocode_silver_locations.py`; it applies overrides into
   `location_geocodes` before making OneMap API calls.
3. Run dbt Gold. Use a full refresh, or widen `gold_incremental_lookback_hours`,
   if older already-materialized Gold rows must pick up new overrides.
"""

# COMMAND ----------

import base64
import binascii
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

SAFE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")
ONEMAP_SEARCH_URL = "https://www.onemap.gov.sg/api/common/elastic/search"
ONEMAP_TOKEN_URL = "https://www.onemap.gov.sg/api/auth/post/getToken"
REQUEST_TIMEOUT_SECONDS = 30
TOKEN_REFRESH_SAFETY_SECONDS = 300

OVERRIDE_SCHEMA = StructType([
    StructField("normalized_location", StringType(), nullable=False),
    StructField("postal_code", StringType(), nullable=False),
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
    StructField("formatted_address", StringType(), nullable=True),
    StructField("notes", StringType(), nullable=True),
    StructField("updated_at", TimestampType(), nullable=True),
])

# Add project-specific aliases here as you see more broad locations in Telegram.
TOWN_MRT_QUERIES = {
    "admiralty": "Admiralty MRT Station",
    "aljunied": "Aljunied MRT Station",
    "ang mo kio": "Ang Mo Kio MRT Station",
    "bedok": "Bedok MRT Station",
    "bishan": "Bishan MRT Station",
    "boon lay": "Boon Lay MRT Station",
    "bukit batok": "Bukit Batok MRT Station",
    "bukit panjang": "Bukit Panjang MRT Station",
    "buona vista": "Buona Vista MRT Station",
    "canberra": "Canberra MRT Station",
    "changi airport": "Changi Airport MRT Station",
    "chinatown": "Chinatown MRT Station",
    "choa chu kang": "Choa Chu Kang MRT Station",
    "city hall": "City Hall MRT Station",
    "clementi": "Clementi MRT Station",
    "commonwealth": "Commonwealth MRT Station",
    "dhoby ghaut": "Dhoby Ghaut MRT Station",
    "dover": "Dover MRT Station",
    "eunos": "Eunos MRT Station",
    "expo": "Expo MRT Station",
    "farrer park": "Farrer Park MRT Station",
    "holland village": "Holland Village MRT Station",
    "hougang": "Hougang MRT Station",
    "jurong east": "Jurong East MRT Station",
    "kallang": "Kallang MRT Station",
    "kovan": "Kovan MRT Station",
    "lakeside": "Lakeside MRT Station",
    "lavender": "Lavender MRT Station",
    "little india": "Little India MRT Station",
    "marsiling": "Marsiling MRT Station",
    "newton": "Newton MRT Station",
    "novena": "Novena MRT Station",
    "orchard": "Orchard MRT Station",
    "outram park": "Outram Park MRT Station",
    "pasir ris": "Pasir Ris MRT Station",
    "paya lebar": "Paya Lebar MRT Station",
    "pioneer": "Pioneer MRT Station",
    "potong pasir": "Potong Pasir MRT Station",
    "punggol": "Punggol MRT Station",
    "queenstown": "Queenstown MRT Station",
    "redhill": "Redhill MRT Station",
    "sembawang": "Sembawang MRT Station",
    "sengkang": "Sengkang MRT Station",
    "serangoon": "Serangoon MRT Station",
    "simei": "Simei MRT Station",
    "tampines": "Tampines MRT Station",
    "tanjong pagar": "Tanjong Pagar MRT Station",
    "toa payoh": "Toa Payoh MRT Station",
    "woodlands": "Woodlands MRT Station",
    "yew tee": "Yew Tee MRT Station",
    "yio chu kang": "Yio Chu Kang MRT Station",
    "yishun": "Yishun MRT Station",
}

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "workspace", "Catalog")
    dbutils.widgets.text("silver_schema", "silver", "Silver schema")
    dbutils.widgets.text("overrides_table", "location_overrides", "Overrides table")
    dbutils.widgets.text("secret_scope", "telehitch-onemap", "OneMap secret scope")
    dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry run")
except NameError:
    pass


def widget_value(name, default):
    try:
        return dbutils.widgets.get(name)
    except NameError:
        return default


def validated_identifier(value, label):
    if not SAFE_IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{label} must be a safe Unity Catalog identifier")
    return value


def quoted_relation(catalog, schema, table):
    return ".".join(
        f"`{validated_identifier(value, label)}`"
        for value, label in ((catalog, "catalog"), (schema, "schema"), (table, "table"))
    )


CATALOG = validated_identifier(widget_value("catalog", "workspace"), "catalog")
SILVER_SCHEMA = validated_identifier(widget_value("silver_schema", "silver"), "silver_schema")
OVERRIDES_TABLE = quoted_relation(CATALOG, SILVER_SCHEMA, widget_value("overrides_table", "location_overrides"))
SECRET_SCOPE = widget_value("secret_scope", "telehitch-onemap")
DRY_RUN = widget_value("dry_run", "false").lower() == "true"

print(f"Overrides target: {OVERRIDES_TABLE}")
print(f"Dry run: {DRY_RUN}")

# COMMAND ----------


def normalize_location(value):
    normalized = re.sub(r"\s+", " ", value or "").strip().lower()
    return normalized or None


def get_optional_secret(key):
    try:
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)
    except Exception as exc:
        _ = exc
        return None


class OneMapHTTPError(RuntimeError):
    def __init__(self, status_code, response_text):
        super().__init__(f"OneMap returned HTTP {status_code}: {response_text}")
        self.status_code = status_code


def http_json(url, method="GET", params=None, payload=None, headers=None):
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    request_headers = {"Accept": "application/json", "User-Agent": "telehitch-location-overrides/1.0"}
    request_headers.update(headers or {})
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise OneMapHTTPError(exc.code, exc.read().decode("utf-8", errors="replace")[:500]) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Could not reach OneMap: {exc.reason}") from exc


def jwt_expiry_epoch(access_token):
    try:
        payload_segment = access_token.split(".")[1]
        padding = "=" * (-len(payload_segment) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_segment + padding).decode("utf-8"))
        return int(payload["exp"])
    except (IndexError, KeyError, TypeError, ValueError, UnicodeDecodeError, binascii.Error):
        return None


def token_expires_soon(access_token):
    expiry_epoch = jwt_expiry_epoch(access_token)
    if expiry_epoch is None:
        return False
    return expiry_epoch <= int(datetime.now(timezone.utc).timestamp()) + TOKEN_REFRESH_SAFETY_SECONDS


def persist_access_token(access_token):
    WorkspaceClient().secrets.put_secret(scope=SECRET_SCOPE, key="access-token", string_value=access_token)


def refresh_onemap_token():
    email = get_optional_secret("email")
    password = get_optional_secret("password")
    if not email or not password:
        raise RuntimeError(f"Secret scope {SECRET_SCOPE!r} must contain both 'email' and 'password'")
    response = http_json(ONEMAP_TOKEN_URL, method="POST", payload={"email": email, "password": password})
    access_token = response.get("access_token")
    if not access_token:
        raise RuntimeError("OneMap authentication returned no access_token")
    persist_access_token(access_token)
    return access_token


def get_onemap_token():
    access_token = get_optional_secret("access-token")
    if access_token and not token_expires_soon(access_token):
        return access_token
    return refresh_onemap_token()


def search_onemap(search_value, access_token):
    return http_json(
        ONEMAP_SEARCH_URL,
        params={"searchVal": search_value, "returnGeom": "Y", "getAddrDetails": "Y", "pageNum": 1},
        headers={"Authorization": f"Bearer {access_token}"},
    )


def search_onemap_with_refresh(search_value, access_token):
    try:
        return search_onemap(search_value, access_token), access_token
    except OneMapHTTPError as exc:
        if exc.status_code != 401:
            raise
    refreshed_token = refresh_onemap_token()
    return search_onemap(search_value, refreshed_token), refreshed_token


def choose_mrt_result(payload):
    valid_results = [
        result for result in (payload.get("results") or [])
        if str(result.get("POSTAL") or "").strip().isdigit()
        and str(result.get("LATITUDE") or "").strip()
        and str(result.get("LONGITUDE") or "").strip()
    ]
    if not valid_results:
        return None
    mrt_results = [
        result for result in valid_results
        if "MRT" in str(result.get("SEARCHVAL") or result.get("ADDRESS") or "").upper()
    ]
    return (mrt_results or valid_results)[0]


def ensure_overrides_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {OVERRIDES_TABLE} (
            normalized_location STRING NOT NULL,
            postal_code STRING NOT NULL,
            latitude DOUBLE,
            longitude DOUBLE,
            formatted_address STRING,
            notes STRING,
            updated_at TIMESTAMP
        )
        USING DELTA
    """)


access_token = get_onemap_token()
updated_at = datetime.now(timezone.utc)
rows = []
failures = []

for normalized_location, search_value in TOWN_MRT_QUERIES.items():
    payload, access_token = search_onemap_with_refresh(search_value, access_token)
    result = choose_mrt_result(payload)
    if not result:
        failures.append((normalized_location, search_value, len(payload.get("results") or [])))
        continue
    rows.append(Row(
        normalized_location=normalize_location(normalized_location),
        postal_code=str(result.get("POSTAL") or "").strip(),
        latitude=float(result.get("LATITUDE")),
        longitude=float(result.get("LONGITUDE")),
        formatted_address=str(result.get("ADDRESS") or result.get("SEARCHVAL") or "").strip(),
        notes=f"Town-level override to OneMap MRT station search: {search_value}",
        updated_at=updated_at,
    ))
    time.sleep(0.1)

print(f"Resolved overrides: {len(rows)}")
if failures:
    print("Unresolved override searches:")
    for normalized_location, search_value, result_count in failures:
        print(f"- {normalized_location}: {search_value} ({result_count} OneMap results)")

if rows:
    overrides_df = spark.createDataFrame(rows, schema=OVERRIDE_SCHEMA)
    overrides_df.createOrReplaceTempView("town_mrt_location_overrides")
    display(overrides_df.orderBy("normalized_location"))

if DRY_RUN:
    print("Dry run enabled; not writing overrides.")
elif not rows:
    print("No overrides resolved; not writing overrides.")
else:
    ensure_overrides_table()
    spark.sql(f"""
        MERGE INTO {OVERRIDES_TABLE} AS target
        USING town_mrt_location_overrides AS source
          ON target.normalized_location = source.normalized_location
        WHEN MATCHED THEN UPDATE SET
            target.postal_code = source.postal_code,
            target.latitude = source.latitude,
            target.longitude = source.longitude,
            target.formatted_address = source.formatted_address,
            target.notes = source.notes,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            normalized_location, postal_code, latitude, longitude,
            formatted_address, notes, updated_at
        )
        VALUES (
            source.normalized_location, source.postal_code, source.latitude,
            source.longitude, source.formatted_address, source.notes, source.updated_at
        )
    """)
    print(f"Upserted {len(rows)} overrides into {OVERRIDES_TABLE}")
