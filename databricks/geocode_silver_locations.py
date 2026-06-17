# Databricks notebook source
"""Incrementally resolve distinct Silver pickup/drop-off locations with OneMap.

Import this file as a Databricks Python notebook. The job:

1. Creates the reusable Delta geocode cache and manual-override tables.
2. Applies reviewed overrides before making API calls.
3. Selects only distinct locations that are new or had a transient error.
4. Makes an optionally bounded number of OneMap requests on the notebook driver.
5. Idempotently replaces generated results for each processed normalized
   location while preserving manual overrides.

The notebook never updates the Airflow-managed Bronze table or the dbt-managed
Silver requests table.
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
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


ONEMAP_TOKEN_URL = "https://www.onemap.gov.sg/api/auth/post/getToken"
ONEMAP_SEARCH_URL = "https://www.onemap.gov.sg/api/common/elastic/search"
SINGAPORE_POSTAL_CODE_PATTERN = re.compile(r"^\d{6}$")
POSTAL_CODE_IN_TEXT_PATTERN = re.compile(r"(?<!\d)(\d{6})(?!\d)")
LOCATION_SEARCH_SUFFIX_PATTERN = re.compile(
    r"\s+(?:"
    r"side\s+entrance|main\s+entrance|drop\s*off\s+point|"
    r"pick\s*up\s+point|pickup\s+point|taxi\s+stand|"
    r"hotel|entrance|lobby"
    r")$"
)
SAFE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")
TOKEN_REFRESH_SAFETY_SECONDS = 300

NON_SPECIFIC_LOCATIONS = {
    "any",
    "any where",
    "anywhere",
    "east",
    "jb",
    "johor",
    "johor bahru",
    "n/a",
    "na",
    "nil",
    "north",
    "not sure",
    "sg",
    "singapore",
    "south",
    "tbc",
    "west",
}

GEOCODE_RESULT_SCHEMA = StructType(
    [
        StructField("normalized_location", StringType(), nullable=False),
        StructField("original_location", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("formatted_address", StringType(), nullable=True),
        StructField("search_value", StringType(), nullable=True),
        StructField("resolution_status", StringType(), nullable=False),
        StructField("resolution_source", StringType(), nullable=False),
        StructField("result_count", IntegerType(), nullable=False),
        StructField("error_message", StringType(), nullable=True),
        StructField("attempted_at", TimestampType(), nullable=False),
        StructField("resolved_at", TimestampType(), nullable=True),
    ]
)


# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text(
    "requests_table",
    "silver_telehitch_requests",
    "Silver requests table",
)
dbutils.widgets.text(
    "geocodes_table",
    "location_geocodes",
    "Geocode cache table",
)
dbutils.widgets.text(
    "overrides_table",
    "location_overrides",
    "Manual overrides table",
)
dbutils.widgets.text(
    "secret_scope",
    "telehitch-onemap",
    "OneMap secret scope",
)
dbutils.widgets.text(
    "max_locations_per_run",
    "0",
    "Maximum locations (0 = unlimited)",
)
dbutils.widgets.text(
    "request_timeout_seconds",
    "30",
    "HTTP timeout seconds",
)
dbutils.widgets.text(
    "sleep_between_requests_seconds",
    "0.25",
    "Delay between API calls",
)


# COMMAND ----------

def validated_identifier(value, label):
    """Validate a Unity Catalog identifier before interpolating it into SQL."""
    if not SAFE_IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"{label} must contain only letters, digits, underscores, or hyphens "
            "and cannot start with a digit"
        )
    return value


def quoted_relation(catalog, schema, table):
    """Return a safely quoted three-part Unity Catalog relation name."""
    return ".".join(
        f"`{validated_identifier(value, label)}`"
        for value, label in (
            (catalog, "catalog"),
            (schema, "schema"),
            (table, "table"),
        )
    )


def positive_int(value, label):
    result = int(value)
    if result <= 0:
        raise ValueError(f"{label} must be greater than zero")
    return result


def non_negative_int(value, label):
    result = int(value)
    if result < 0:
        raise ValueError(f"{label} must be zero or greater")
    return result


def non_negative_float(value, label):
    result = float(value)
    if result < 0:
        raise ValueError(f"{label} must be zero or greater")
    return result


CATALOG = validated_identifier(dbutils.widgets.get("catalog"), "catalog")
SILVER_SCHEMA = validated_identifier(
    dbutils.widgets.get("silver_schema"),
    "silver_schema",
)
REQUESTS_TABLE = quoted_relation(
    CATALOG,
    SILVER_SCHEMA,
    dbutils.widgets.get("requests_table"),
)
GEOCODES_TABLE = quoted_relation(
    CATALOG,
    SILVER_SCHEMA,
    dbutils.widgets.get("geocodes_table"),
)
OVERRIDES_TABLE = quoted_relation(
    CATALOG,
    SILVER_SCHEMA,
    dbutils.widgets.get("overrides_table"),
)
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
MAX_LOCATIONS_PER_RUN = non_negative_int(
    dbutils.widgets.get("max_locations_per_run"),
    "max_locations_per_run",
)
REQUEST_TIMEOUT_SECONDS = positive_int(
    dbutils.widgets.get("request_timeout_seconds"),
    "request_timeout_seconds",
)
SLEEP_BETWEEN_REQUESTS_SECONDS = non_negative_float(
    dbutils.widgets.get("sleep_between_requests_seconds"),
    "sleep_between_requests_seconds",
)

print(f"Requests source: {REQUESTS_TABLE}")
print(f"Geocode cache: {GEOCODES_TABLE}")
print(f"Overrides: {OVERRIDES_TABLE}")
print(
    "Maximum locations this run: "
    f"{MAX_LOCATIONS_PER_RUN or 'unlimited'}"
)


# COMMAND ----------

def ensure_tables():
    """Create the cache and override tables without modifying source requests."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GEOCODES_TABLE} (
            normalized_location STRING NOT NULL,
            original_location STRING,
            postal_code STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            formatted_address STRING,
            search_value STRING,
            resolution_status STRING NOT NULL,
            resolution_source STRING NOT NULL,
            result_count INT NOT NULL,
            error_message STRING,
            attempted_at TIMESTAMP NOT NULL,
            resolved_at TIMESTAMP
        )
        USING DELTA
        """
    )

    spark.sql(
        f"""
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
        """
    )


def apply_overrides():
    """Make reviewed overrides authoritative over API-derived results."""
    spark.sql(
        f"""
        MERGE INTO {GEOCODES_TABLE} AS target
        USING (
            SELECT
                lower(trim(regexp_replace(normalized_location, '\\\\s+', ' ')))
                    AS normalized_location,
                postal_code,
                latitude,
                longitude,
                formatted_address
            FROM {OVERRIDES_TABLE}
        ) AS source
          ON target.normalized_location = source.normalized_location

        WHEN MATCHED THEN UPDATE SET
            target.postal_code = source.postal_code,
            target.latitude = source.latitude,
            target.longitude = source.longitude,
            target.formatted_address = source.formatted_address,
            target.search_value = source.normalized_location,
            target.resolution_status = 'resolved',
            target.resolution_source = 'override',
            target.result_count = 1,
            target.error_message = NULL,
            target.attempted_at = current_timestamp(),
            target.resolved_at = current_timestamp()

        WHEN NOT MATCHED THEN INSERT (
            normalized_location,
            original_location,
            postal_code,
            latitude,
            longitude,
            formatted_address,
            search_value,
            resolution_status,
            resolution_source,
            result_count,
            error_message,
            attempted_at,
            resolved_at
        )
        VALUES (
            source.normalized_location,
            source.normalized_location,
            source.postal_code,
            source.latitude,
            source.longitude,
            source.formatted_address,
            source.normalized_location,
            'resolved',
            'override',
            1,
            NULL,
            current_timestamp(),
            current_timestamp()
        )
        """
    )


ensure_tables()
apply_overrides()


# COMMAND ----------

def normalize_location(value):
    """Normalize location text identically to the SQL selection and Gold join."""
    if value is None:
        return None
    normalized = re.sub(r"\s+", " ", value).strip().lower()
    return normalized or None


def postal_codes_in_location(value):
    """Return unique standalone six-digit codes in their original order."""
    return list(dict.fromkeys(POSTAL_CODE_IN_TEXT_PATTERN.findall(value or "")))


def get_optional_secret(key):
    """Return a Databricks secret or None when the key does not exist."""
    try:
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)
    except Exception as exc:
        # Databricks does not expose a stable public exception type for a
        # missing secret key. Do not print the exception because some secret
        # backends can include sensitive metadata.
        _ = exc
        return None


class OneMapHTTPError(RuntimeError):
    """Expose the OneMap HTTP status without leaking authorization headers."""

    def __init__(self, status_code, response_text):
        super().__init__(
            f"OneMap returned HTTP {status_code}: {response_text}"
        )
        self.status_code = status_code


def http_json(url, method="GET", params=None, payload=None, headers=None):
    """Make a bounded JSON HTTP request using only the Python standard library."""
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"

    request_headers = {
        "Accept": "application/json",
        "User-Agent": "telehitch-geocoder/1.0",
    }
    request_headers.update(headers or {})

    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        url,
        data=body,
        headers=request_headers,
        method=method,
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=REQUEST_TIMEOUT_SECONDS,
        ) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        response_text = exc.read().decode("utf-8", errors="replace")[:500]
        raise OneMapHTTPError(exc.code, response_text) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Could not reach OneMap: {exc.reason}") from exc


def jwt_expiry_epoch(access_token):
    """Return a JWT expiry epoch, or None when the token is opaque/malformed."""
    try:
        payload_segment = access_token.split(".")[1]
        padding = "=" * (-len(payload_segment) % 4)
        payload = json.loads(
            base64.urlsafe_b64decode(payload_segment + padding).decode("utf-8")
        )
        return int(payload["exp"])
    except (
        IndexError,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        binascii.Error,
    ):
        return None


def token_expires_soon(access_token, now_epoch=None):
    """Refresh JWTs five minutes before expiry; let opaque tokens use 401 retry."""
    expiry_epoch = jwt_expiry_epoch(access_token)
    if expiry_epoch is None:
        return False

    if now_epoch is None:
        now_epoch = int(datetime.now(timezone.utc).timestamp())

    return expiry_epoch <= now_epoch + TOKEN_REFRESH_SAFETY_SECONDS


def persist_access_token(access_token):
    """Store the replacement token for reuse by later notebook job runs."""
    WorkspaceClient().secrets.put_secret(
        scope=SECRET_SCOPE,
        key="access-token",
        string_value=access_token,
    )


def refresh_onemap_token():
    """Authenticate with OneMap, cache the replacement token, and return it."""
    email = get_optional_secret("email")
    password = get_optional_secret("password")
    if not email or not password:
        raise RuntimeError(
            f"Secret scope {SECRET_SCOPE!r} must contain both 'email' and "
            "'password' so expired OneMap tokens can be refreshed"
        )

    response = http_json(
        ONEMAP_TOKEN_URL,
        method="POST",
        payload={"email": email, "password": password},
    )
    access_token = response.get("access_token")
    if not access_token:
        raise RuntimeError("OneMap authentication returned no access_token")

    persist_access_token(access_token)
    return access_token


def get_onemap_token():
    """Reuse a valid cached token or refresh it before its JWT expiry."""
    access_token = get_optional_secret("access-token")
    if access_token and not token_expires_soon(access_token):
        return access_token
    return refresh_onemap_token()


def search_onemap(location, access_token):
    """Search OneMap for one normalized Singapore location."""
    return http_json(
        ONEMAP_SEARCH_URL,
        params={
            "searchVal": location,
            "returnGeom": "Y",
            "getAddrDetails": "Y",
            "pageNum": 1,
        },
        headers={"Authorization": f"Bearer {access_token}"},
    )


def search_onemap_with_refresh(location, access_token):
    """Refresh and retry once when OneMap rejects a token with HTTP 401."""
    try:
        return search_onemap(location, access_token), access_token
    except OneMapHTTPError as exc:
        if exc.status_code != 401:
            raise

    refreshed_token = refresh_onemap_token()
    return search_onemap(location, refreshed_token), refreshed_token


def choose_result(payload):
    """Return a result only when all valid matches share one postal code."""
    results = payload.get("results") or []
    valid_results = [
        result
        for result in results
        if SINGAPORE_POSTAL_CODE_PATTERN.fullmatch(
            str(result.get("POSTAL") or "").strip()
        )
    ]

    if not valid_results:
        return None, len(results), "no_match"

    unique_postal_codes = {
        str(result["POSTAL"]).strip()
        for result in valid_results
    }
    if len(unique_postal_codes) != 1:
        return None, len(results), "ambiguous"

    return valid_results[0], len(results), "resolved"


def choose_postal_code_result(payload, postal_code):
    """Return the first exact OneMap result for one explicit postal code."""
    results = payload.get("results") or []
    for result in results:
        if str(result.get("POSTAL") or "").strip() == postal_code:
            return result, len(results), "resolved"
    return None, len(results), "no_match"


def onemap_search_values(normalized_location):
    """Return conservative OneMap search fallbacks for noisy place names."""
    values = [normalized_location]
    candidate = normalized_location

    while True:
        stripped = LOCATION_SEARCH_SUFFIX_PATTERN.sub("", candidate).strip()
        if stripped == candidate:
            break
        if stripped and stripped not in values:
            values.append(stripped)
        candidate = stripped

    return values


# COMMAND ----------

location_limit_clause = (
    f"LIMIT {MAX_LOCATIONS_PER_RUN}"
    if MAX_LOCATIONS_PER_RUN > 0
    else ""
)

locations_to_process = spark.sql(
    f"""
    WITH all_locations AS (
        SELECT pickup_location AS original_location
        FROM {REQUESTS_TABLE}
        WHERE pickup_location IS NOT NULL

        UNION ALL

        SELECT dropoff_location AS original_location
        FROM {REQUESTS_TABLE}
        WHERE dropoff_location IS NOT NULL
    ),

    normalized AS (
        SELECT
            lower(trim(regexp_replace(original_location, '\\\\s+', ' ')))
                AS normalized_location,
            min(original_location) AS original_location
        FROM all_locations
        GROUP BY
            lower(trim(regexp_replace(original_location, '\\\\s+', ' ')))
    )

    SELECT DISTINCT
        locations.normalized_location,
        locations.original_location
    FROM normalized AS locations
    LEFT JOIN {GEOCODES_TABLE} AS geocodes
      ON geocodes.normalized_location = locations.normalized_location
    WHERE locations.normalized_location IS NOT NULL
      AND locations.normalized_location <> ''
      AND (
          geocodes.normalized_location IS NULL
          OR geocodes.resolution_status = 'error'
          OR (
              geocodes.postal_code IS NULL
              AND locations.normalized_location RLIKE
                  '(^|[^0-9])[0-9]{{6}}([^0-9]|$)'
          )
          OR (
              geocodes.postal_code LIKE '%,%'
              AND locations.normalized_location RLIKE
                  '(^|[^0-9])[0-9]{{6}}([^0-9].*)?[0-9]{{6}}([^0-9]|$)'
          )
      )
    ORDER BY locations.normalized_location
    {location_limit_clause}
    """
).collect()

print(f"Unresolved locations selected: {len(locations_to_process)}")

def no_result_row(
    normalized_location,
    original_location,
    status,
    source,
    attempted_at,
    result_count=0,
    error_message=None,
):
    return Row(
        normalized_location=normalized_location,
        original_location=original_location,
        postal_code=None,
        latitude=None,
        longitude=None,
        formatted_address=None,
        search_value=None,
        resolution_status=status,
        resolution_source=source,
        result_count=result_count,
        error_message=error_message,
        attempted_at=attempted_at,
        resolved_at=None,
    )


output_rows = []
onemap_token = None

if locations_to_process:
    attempted_at = datetime.now(timezone.utc).replace(tzinfo=None)

    for location_row in locations_to_process:
        normalized_location = normalize_location(
            location_row["normalized_location"]
        )
        original_location = location_row["original_location"]
        postal_codes = postal_codes_in_location(original_location)

        if postal_codes:
            for postal_code in postal_codes:
                try:
                    if onemap_token is None:
                        onemap_token = get_onemap_token()
                    payload, onemap_token = search_onemap_with_refresh(
                        postal_code,
                        onemap_token,
                    )
                    result, result_count, status = choose_postal_code_result(
                        payload,
                        postal_code,
                    )

                    if result is None:
                        output_rows.append(
                            Row(
                                normalized_location=normalized_location,
                                original_location=original_location,
                                postal_code=postal_code,
                                latitude=None,
                                longitude=None,
                                formatted_address=None,
                                search_value=None,
                                resolution_status="resolved",
                                resolution_source="rule",
                                result_count=result_count,
                                error_message=(
                                    "OneMap returned no exact result"
                                    f" for postal code {postal_code}"
                                    if status == "no_match"
                                    else None
                                ),
                                attempted_at=attempted_at,
                                resolved_at=attempted_at,
                            )
                        )
                    else:
                        output_rows.append(
                            Row(
                                normalized_location=normalized_location,
                                original_location=original_location,
                                postal_code=postal_code,
                                latitude=float(result["LATITUDE"]),
                                longitude=float(result["LONGITUDE"]),
                                formatted_address=result.get("ADDRESS"),
                                search_value=result.get("SEARCHVAL"),
                                resolution_status="resolved",
                                resolution_source="onemap",
                                result_count=result_count,
                                error_message=None,
                                attempted_at=attempted_at,
                                resolved_at=attempted_at,
                            )
                        )
                except Exception as exc:
                    output_rows.append(
                        Row(
                            normalized_location=normalized_location,
                            original_location=original_location,
                            postal_code=postal_code,
                            latitude=None,
                            longitude=None,
                            formatted_address=None,
                            search_value=None,
                            resolution_status="resolved",
                            resolution_source="rule",
                            result_count=0,
                            error_message=str(exc)[:1000],
                            attempted_at=attempted_at,
                            resolved_at=attempted_at,
                        )
                    )

                time.sleep(SLEEP_BETWEEN_REQUESTS_SECONDS)
            continue

        if normalized_location in NON_SPECIFIC_LOCATIONS:
            output_rows.append(
                no_result_row(
                    normalized_location,
                    original_location,
                    status="no_match",
                    source="rule",
                    attempted_at=attempted_at,
                    error_message="Non-specific or non-Singapore location",
                )
            )
            continue

        try:
            if onemap_token is None:
                onemap_token = get_onemap_token()
            result = None
            result_count = 0
            status = "no_match"
            for search_value in onemap_search_values(normalized_location):
                payload, onemap_token = search_onemap_with_refresh(
                    search_value,
                    onemap_token,
                )
                result, result_count, status = choose_result(payload)
                if result is not None:
                    break
                if status == "ambiguous":
                    break

            if result is None:
                output_rows.append(
                    no_result_row(
                        normalized_location,
                        original_location,
                        status=status,
                        source="onemap",
                        attempted_at=attempted_at,
                        result_count=result_count,
                    )
                )
            else:
                output_rows.append(
                    Row(
                        normalized_location=normalized_location,
                        original_location=original_location,
                        postal_code=str(result["POSTAL"]).strip(),
                        latitude=float(result["LATITUDE"]),
                        longitude=float(result["LONGITUDE"]),
                        formatted_address=result.get("ADDRESS"),
                        search_value=result.get("SEARCHVAL"),
                        resolution_status="resolved",
                        resolution_source="onemap",
                        result_count=result_count,
                        error_message=None,
                        attempted_at=attempted_at,
                        resolved_at=attempted_at,
                    )
                )
        except Exception as exc:
            output_rows.append(
                no_result_row(
                    normalized_location,
                    original_location,
                    status="error",
                    source="onemap",
                    attempted_at=attempted_at,
                    error_message=str(exc)[:1000],
                )
            )

        time.sleep(SLEEP_BETWEEN_REQUESTS_SECONDS)

print(f"Geocoding results produced: {len(output_rows)}")


# COMMAND ----------

if output_rows:
    results_df = spark.createDataFrame(
        output_rows,
        schema=GEOCODE_RESULT_SCHEMA,
    )
    results_df.createOrReplaceTempView("new_location_geocodes")

    spark.sql(
        f"""
        DELETE FROM {GEOCODES_TABLE}
        WHERE resolution_source <> 'override'
          AND normalized_location IN (
              SELECT DISTINCT normalized_location
              FROM new_location_geocodes
          )
        """
    )

    spark.sql(
        f"""
        INSERT INTO {GEOCODES_TABLE} (
            normalized_location,
            original_location,
            postal_code,
            latitude,
            longitude,
            formatted_address,
            search_value,
            resolution_status,
            resolution_source,
            result_count,
            error_message,
            attempted_at,
            resolved_at
        )
        SELECT
            normalized_location,
            original_location,
            postal_code,
            latitude,
            longitude,
            formatted_address,
            search_value,
            resolution_status,
            resolution_source,
            result_count,
            error_message,
            attempted_at,
            resolved_at
        FROM new_location_geocodes
        """
    )
    print(f"Wrote {len(output_rows)} location results into {GEOCODES_TABLE}")
else:
    print("No unresolved locations required an API call.")


# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            resolution_status,
            resolution_source,
            count(*) AS location_count
        FROM {GEOCODES_TABLE}
        GROUP BY resolution_status, resolution_source
        ORDER BY resolution_status, resolution_source
        """
    )
)
