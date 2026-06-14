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


def test_geocoding_notebook_is_bounded_and_incremental():
    notebook = notebook_text()

    assert '"max_locations_per_run", "100"' in notebook
    assert "LIMIT {MAX_LOCATIONS_PER_RUN}" in notebook
    assert "geocodes.normalized_location IS NULL" in notebook
    assert "geocodes.resolution_status = 'error'" in notebook
    assert "MERGE INTO {GEOCODES_TABLE}" in notebook
    assert (
        "target.normalized_location = source.normalized_location"
        in notebook
    )


def test_geocoding_notebook_uses_secrets_and_never_embeds_credentials():
    notebook = notebook_text()

    assert "dbutils.secrets.get" in notebook
    assert '"access-token"' in notebook
    assert '"email"' in notebook
    assert '"password"' in notebook
    assert "ONEMAP_TOKEN_URL" in notebook
    assert "print(access_token)" not in notebook
    assert "print(onemap_token)" not in notebook


def test_geocoding_notebook_rejects_ambiguous_postal_codes():
    notebook = notebook_text()

    assert 'SINGAPORE_POSTAL_CODE_PATTERN = re.compile(r"^\\d{6}$")' in notebook
    assert 'return None, len(results), "ambiguous"' in notebook
    assert 'return valid_results[0], len(results), "resolved"' in notebook


def test_manual_overrides_are_applied_before_api_selection():
    notebook = notebook_text()

    ensure_index = notebook.index("ensure_tables()")
    override_index = notebook.index("apply_overrides()", ensure_index)
    selection_index = notebook.index("locations_to_process = spark.sql")

    assert ensure_index < override_index < selection_index
    assert "target.resolution_source <> 'override'" in notebook
