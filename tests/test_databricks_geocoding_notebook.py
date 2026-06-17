from pathlib import Path


ROOT = Path(__file__).parents[1]
NOTEBOOK = ROOT / "databricks" / "geocode_silver_locations.py"


def notebook_text():
    return NOTEBOOK.read_text()


def test_geocoding_notebook_is_importable_by_databricks():
    notebook = notebook_text()

    assert notebook.startswith("# Databricks notebook source")
    assert "# COMMAND ----------" in notebook
    assert "dbutils.widgets.text" in notebook


def test_geocoding_notebook_reads_silver_without_modifying_it():
    notebook = notebook_text()

    assert '"silver_telehitch_requests"' in notebook
    assert "SELECT pickup_location AS original_location" in notebook
    assert "SELECT dropoff_location AS original_location" in notebook
    assert "MERGE INTO {REQUESTS_TABLE}" not in notebook
    assert "UPDATE {REQUESTS_TABLE}" not in notebook
    assert "DELETE FROM {REQUESTS_TABLE}" not in notebook


def test_geocoding_notebook_is_unlimited_by_default_and_incremental():
    notebook = notebook_text()

    assert '"max_locations_per_run",' in notebook
    assert '"0",' in notebook
    assert "location_limit_clause = (" in notebook
    assert "if MAX_LOCATIONS_PER_RUN > 0" in notebook
    assert "f\"LIMIT {MAX_LOCATIONS_PER_RUN}\"" in notebook
    assert "f\"{MAX_LOCATIONS_PER_RUN or 'unlimited'}\"" in notebook
    assert "geocodes.normalized_location IS NULL" in notebook
    assert "geocodes.resolution_status = 'error'" in notebook
    assert "DELETE FROM {GEOCODES_TABLE}" in notebook
    assert "INSERT INTO {GEOCODES_TABLE}" in notebook
    assert "USING new_location_geocodes AS source" not in notebook


def test_selection_and_processing_share_one_databricks_cell():
    notebook = notebook_text()
    cells = notebook.split("# COMMAND ----------")
    processing_cell = next(
        cell for cell in cells if "locations_to_process = spark.sql" in cell
    )

    assert "if locations_to_process:" in processing_cell
    assert processing_cell.index("locations_to_process = spark.sql") < (
        processing_cell.index("if locations_to_process:")
    )


def test_geocoding_notebook_uses_secrets_and_never_embeds_credentials():
    notebook = notebook_text()

    assert "dbutils.secrets.get" in notebook
    assert "WorkspaceClient().secrets.put_secret" in notebook
    assert '"access-token"' in notebook
    assert '"email"' in notebook
    assert '"password"' in notebook
    assert "ONEMAP_TOKEN_URL" in notebook
    assert "print(access_token)" not in notebook
    assert "print(onemap_token)" not in notebook


def test_geocoding_notebook_refreshes_tokens_before_expiry():
    notebook = notebook_text()

    assert "TOKEN_REFRESH_SAFETY_SECONDS = 300" in notebook
    assert "def jwt_expiry_epoch(access_token):" in notebook
    assert "base64.urlsafe_b64decode" in notebook
    assert 'payload["exp"]' in notebook
    assert "def token_expires_soon(access_token, now_epoch=None):" in notebook
    assert "expiry_epoch <= now_epoch + TOKEN_REFRESH_SAFETY_SECONDS" in notebook
    assert "if access_token and not token_expires_soon(access_token):" in notebook
    assert "return refresh_onemap_token()" in notebook


def test_geocoding_notebook_refreshes_and_retries_once_after_401():
    notebook = notebook_text()

    assert "class OneMapHTTPError(RuntimeError):" in notebook
    assert "self.status_code = status_code" in notebook
    assert "def search_onemap_with_refresh(location, access_token):" in notebook
    assert "if exc.status_code != 401:" in notebook
    assert "refreshed_token = refresh_onemap_token()" in notebook
    assert (
        "return search_onemap(location, refreshed_token), refreshed_token"
        in notebook
    )
    assert "payload, onemap_token = search_onemap_with_refresh(" in notebook


def test_geocoding_notebook_rejects_ambiguous_postal_codes():
    notebook = notebook_text()

    assert 'SINGAPORE_POSTAL_CODE_PATTERN = re.compile(r"^\\d{6}$")' in notebook
    assert 'return None, len(results), "ambiguous"' in notebook
    assert 'return valid_results[0], len(results), "resolved"' in notebook


def test_geocoding_notebook_expands_postal_codes_into_separate_rows():
    notebook = notebook_text()

    assert (
        'POSTAL_CODE_IN_TEXT_PATTERN = re.compile(r"(?<!\\d)(\\d{6})(?!\\d)")'
        in notebook
    )
    assert "def postal_codes_in_location(value):" in notebook
    assert "list(dict.fromkeys(" in notebook
    assert "def choose_postal_code_result(payload, postal_code):" in notebook
    assert "for postal_code in postal_codes:" in notebook
    assert "search_onemap_with_refresh(\n                        postal_code" in notebook
    assert "postal_code=postal_code" in notebook
    assert 'postal_code=",".join(postal_codes)' not in notebook
    assert "result_count=len(postal_codes)" not in notebook
    assert "geocodes.postal_code IS NULL" in notebook
    assert "geocodes.postal_code LIKE '%,%'" in notebook

    postal_loop_index = notebook.index("for postal_code in postal_codes:")
    token_index = notebook.index(
        "onemap_token = get_onemap_token()",
        postal_loop_index,
    )
    assert postal_loop_index < token_index


def test_manual_overrides_are_applied_before_api_selection():
    notebook = notebook_text()

    ensure_index = notebook.index("ensure_tables()")
    override_index = notebook.index("apply_overrides()", ensure_index)
    selection_index = notebook.index("locations_to_process = spark.sql")

    assert ensure_index < override_index < selection_index
    assert "resolution_source <> 'override'" in notebook
