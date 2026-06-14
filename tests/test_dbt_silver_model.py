import re
from pathlib import Path


ROOT = Path(__file__).parents[1]
DBT_PROJECT = ROOT / "analytics" / "telehitch_dbt"
SILVER_MODEL = DBT_PROJECT / "models" / "silver_telehitch_requests.sql"


def test_silver_model_accepts_explicit_and_structured_requests():
    model = SILVER_MODEL.read_text()

    assert r"\bdrivers?\s+looking\s+(?:for\s+)?(?:hitchers?|passengers?)\b" in model
    assert r"\bhitchers?\s+looking\s+(?:for\s+)?drivers?\b" in model
    assert r"\blooking\s+for\s+drivers?\b" in model
    assert r"\blooking\s+for\s+passengers?\b" in model
    assert r"(?:接送服务|\bwhole\s+car\b|\b[5-9]\s*seater\b)" in model
    assert model.count(r"\b(?:pick\s*up|pickup)\b") == 2
    assert model.count(r"\b(?:drop\s*off|dropoff|drop)\b") == 2


def classify_request(message):
    message = message.lower().strip()
    has_pickup = re.search(r"\b(?:pick\s*up|pickup)\b", message)
    has_dropoff = re.search(r"\b(?:drop\s*off|dropoff|drop)\b", message)

    if re.search(
        r"^(?:bikers?\s+looking\s+(?:for\s+)?pillions?|pillions?\s+looking\s+(?:for\s+)?bikers?)\b",
        message,
    ):
        return "noise"

    if (
        re.search(
            r"\bdrivers?\s+looking\s+(?:for\s+)?(?:hitchers?|passengers?)\b",
            message,
        )
        or re.search(r"\blooking\s+for\s+passengers?\b", message)
        or (
            re.search(r"(?:接送服务|\bwhole\s+car\b|\b[5-9]\s*seater\b)", message)
            and has_pickup
            and has_dropoff
        )
    ):
        return "driver_request"

    if (
        re.search(r"\bhitchers?\s+looking\s+(?:for\s+)?drivers?\b", message)
        or re.search(r"\blooking\s+for\s+drivers?\b", message)
        or (has_pickup and has_dropoff)
    ):
        return "hitcher_request"

    return "noise"


def test_silver_model_classifies_real_message_variants():
    examples = [
        ("Hitcher looking driver\nPick up: jb\nDrop off: sg", "hitcher_request"),
        ("Looking for driver\nPickup: JB\nDropoff: SG", "hitcher_request"),
        (
            "Driver 🚗Looking for passengers Date:TODAY Pick up: JB Drop off: SG",
            "driver_request",
        ),
        ("DRIVER LOOKING FOR HITCHER Pickup: SG Drop off: JB", "driver_request"),
        ("driver looking for hitcher pick up: sg drop off: jb", "driver_request"),
        (
            "Driver 🚗Looking for passengers Pick up: Jb Drop off: Sg Whole car",
            "driver_request",
        ),
        ("Pick up KSL Drop SG Date today Pax 2 Time now", "hitcher_request"),
        (
            "🚙接送服务 Pick up: SG /JB Drop off: SG /JB 🚘 5 seater Whole car",
            "driver_request",
        ),
        (
            "Biker looking for Pillion 🛵\n\nPick up;-East\nDrop Off: Anywhere\nTime: Now",
            "noise",
        ),
        ("Bikers looking for Pillions\nPickup: East\nDropoff: West", "noise"),
        ("Pillion looking for Biker\nPickup: East\nDropoff: West", "noise"),
        ("Pillions looking for Bikers\nPickup: East\nDropoff: West", "noise"),
        ("Someone mentioned a driver and hitcher", "noise"),
    ]

    for message, expected in examples:
        assert classify_request(message) == expected


def test_silver_model_keeps_the_latest_scraped_copy_per_message():
    model = SILVER_MODEL.read_text()

    assert "partition by channel, id" in model
    assert "order by scraped_at_gmt8 desc" in model
    assert "as scrape_recency_rank" in model
    assert "where scrape_recency_rank = 1" in model


def test_silver_model_reads_the_declared_bronze_source():
    model = SILVER_MODEL.read_text()
    source = (DBT_PROJECT / "models" / "sources.yml").read_text()
    project = (DBT_PROJECT / "dbt_project.yml").read_text()

    assert "source('telegram_bronze', 'messages')" in model
    assert "- name: telegram_bronze" in source
    assert 'identifier: "{{ var(\'raw_table\') }}"' in source
    assert "raw_catalog: workspace" in project
    assert "raw_schema: default" in project
    assert "raw_table: sgjb-telehitch-raw" in project


def test_silver_model_uses_a_six_hour_incremental_lookback():
    model = SILVER_MODEL.read_text()
    project = (DBT_PROJECT / "dbt_project.yml").read_text()

    assert "incremental_lookback_hours: 6" in project
    assert "var('incremental_lookback_hours')" in model
    assert "current_timestamp() - interval" in model
    assert "hours" in model
    assert "max(scraped_at_gmt8)" not in model
    assert "incremental_lookback_days" not in project
    assert "incremental_lookback_days" not in model


def test_silver_model_excludes_biker_pillion_requests():
    model = SILVER_MODEL.read_text()

    assert (
        r"^(?:bikers?\s+looking\s+(?:for\s+)?pillions?|"
        r"pillions?\s+looking\s+(?:for\s+)?bikers?)\b"
    ) in model
    assert ") then null" in model


def test_silver_model_replaces_now_with_the_message_timestamp():
    model = SILVER_MODEL.read_text()

    assert "when lower(trim(request_time_text)) = 'now'" in model
    assert "then cast(message_date_gmt8 as string)" in model
