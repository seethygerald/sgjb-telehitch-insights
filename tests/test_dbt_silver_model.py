import re
from pathlib import Path


ROOT = Path(__file__).parents[1]
DBT_PROJECT = ROOT / "analytics" / "telehitch_dbt"
SILVER_MODEL = DBT_PROJECT / "models" / "silver_telehitch_requests.sql"
GOLD_MODEL = DBT_PROJECT / "models" / "gold_telehitch_requests.sql"


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


def test_declares_notebook_managed_geocode_cache_source():
    source = (DBT_PROJECT / "models" / "sources.yml").read_text()

    assert "- name: telehitch_silver" in source
    assert "database: workspace" in source
    assert "schema: silver" in source
    assert "- name: location_geocodes" in source
    assert "identifier: location_geocodes" in source
    assert "- name: normalized_location" in source
    normalized_location_block = source.split("- name: normalized_location", 1)[1]
    normalized_location_block = normalized_location_block.split(
        "- name: resolution_status",
        1,
    )[0]
    assert "- unique" not in normalized_location_block
    assert "values: [resolved, no_match, ambiguous, error]" in source
    assert "values: [onemap, rule, override]" in source
    assert "- name: result_count" in source


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


def test_silver_model_normalizes_compact_cross_border_routes_conservatively():
    model = SILVER_MODEL.read_text()

    assert "as pickup_location_raw" in model
    assert "as dropoff_location_raw" in model
    assert r"^jb(?:\s|[/→👉🇲🇾🇸🇬-])*sg\s*$" in model
    assert r"^sg(?:\s|[/→👉🇲🇾🇸🇬-])*jb\s*$" in model
    assert "then 'jb'" in model
    assert "then 'sg'" in model
    assert "else pickup_location_raw" in model
    assert "else dropoff_location_raw" in model

    route_pattern = re.compile(r"^(jb|sg)(?:\s|[/→👉🇲🇾🇸🇬-])*(sg|jb)\s*$", re.I)

    def normalize_location(location):
        match = route_pattern.fullmatch(location)
        return match.group(1).lower() if match and match.group(1) != match.group(2) else location

    assert normalize_location("JB🇲🇾SG") == "jb"
    assert normalize_location("SG🇸🇬JB") == "sg"
    assert normalize_location("JB SG") == "jb"
    assert normalize_location("SG→JB") == "sg"
    assert normalize_location("Jurong East") == "Jurong East"
    assert normalize_location("Queenstown MRT") == "Queenstown MRT"
    assert normalize_location("Woodlands") == "Woodlands"
    assert normalize_location("Singapore General Hospital") == "Singapore General Hospital"


def test_silver_model_uses_lower_integer_midpoint_for_labelled_pax_ranges():
    model = SILVER_MODEL.read_text()

    assert "as pax_range_min" in model
    assert "as pax_range_max" in model
    assert "pax_range_min <= pax_range_max" in model
    assert "floor((pax_range_min + pax_range_max) / 2.0)" in model

    assert (1 + 6) // 2 == 3
    assert (2 + 4) // 2 == 3
    assert (1 + 5) // 2 == 3
    assert (2 + 3) // 2 == 2


def test_silver_model_covers_the_real_cross_border_message_and_reverse_route():
    message = """🇲🇾👉🇸🇬 Driver 🇸🇬👉🇲🇾
🚗Looking for passengers 👪

Date:TODAY
Time: NOW
Pick up: JB🇲🇾SG
Drop off: SG🇸🇬JB
Pax: 1-6
Pm pm pm thank you"""
    reverse = message.replace("Pick up: JB🇲🇾SG", "Pick up: SG🇸🇬JB").replace(
        "Drop off: SG🇸🇬JB", "Drop off: JB🇲🇾SG"
    )

    pickup_pattern = re.compile(r"(?im)^\s*pick\s*up\s*:\s*([^\r\n]+)")
    dropoff_pattern = re.compile(r"(?im)^\s*drop\s*off\s*:\s*([^\r\n]+)")
    pax_pattern = re.compile(r"(?i)\bpax\s*:?\s*(\d{1,2})\s*[-–]\s*(\d{1,2})\b")
    route_pattern = re.compile(r"^(jb|sg)(?:\s|[/→👉🇲🇾🇸🇬-])*(sg|jb)\s*$", re.I)

    def parse(sample):
        pickup = route_pattern.fullmatch(pickup_pattern.search(sample).group(1)).group(1)
        dropoff = route_pattern.fullmatch(dropoff_pattern.search(sample).group(1)).group(1)
        pax_min, pax_max = map(int, pax_pattern.search(sample).groups())
        return pickup.lower(), dropoff.lower(), (pax_min + pax_max) // 2

    assert parse(message) == ("jb", "sg", 3)
    assert parse(reverse) == ("sg", "jb", 3)


def test_gold_model_is_an_incremental_dbt_table_in_gold_schema():
    model = GOLD_MODEL.read_text()
    macro = (DBT_PROJECT / "macros" / "generate_schema_name.sql").read_text()
    schema = (DBT_PROJECT / "models" / "schema.yml").read_text()
    project = (DBT_PROJECT / "dbt_project.yml").read_text()

    assert "materialized='incremental'" in model
    assert "incremental_strategy='merge'" in model
    assert "unique_key='gold_request_id'" in model
    assert "on_schema_change='fail'" in model
    assert "schema='gold'" in model
    assert "alias='gold_telehitch_requests'" in model
    assert "{{ custom_schema_name | trim }}" in macro
    assert "gold_incremental_lookback_hours: 6" in project
    assert "- name: gold_telehitch_requests" in schema
    assert "- name: gold_request_id" in schema
    assert "- name: silver_request_id" in schema
    assert "- unique" in schema


def test_gold_model_attaches_pickup_and_dropoff_geocode_combinations():
    model = GOLD_MODEL.read_text()

    assert "source('telehitch_silver', 'location_geocodes')" in model
    assert "left join pickup_geocodes" in model
    assert "left join dropoff_geocodes" in model
    assert "pickup_geocodes.geocode_rank = 1" in model
    assert "dropoff_geocodes.geocode_rank = 1" in model
    assert "pickup_postal_code" in model
    assert "pickup_formatted_address" in model
    assert "dropoff_postal_code" in model
    assert "dropoff_formatted_address" in model


def test_gold_model_deduplicates_same_user_route_posts_within_two_hours():
    model = GOLD_MODEL.read_text()

    assert "cast(sender_id as string)" in model
    assert "lower(trim(sender_handle))" in model
    assert "concat('unknown:', channel, '#', cast(message_id as string))" in model
    assert "as telegram_user_key" in model
    assert "coalesce(pickup_postal_code, normalized_pickup_location)" in model
    assert "coalesce(dropoff_postal_code, normalized_dropoff_location)" in model
    assert "left join with_dedup_key as prior_post" in model
    assert "prior_post.telegram_user_key = candidate.telegram_user_key" in model
    assert "prior_post.pickup_dedup_location_key = candidate.pickup_dedup_location_key" in model
    assert "prior_post.dropoff_dedup_location_key = candidate.dropoff_dedup_location_key" in model
    assert "interval 2 hours" in model
    assert "where prior_post.message_id is null" in model
    assert "where is_canonical_request" not in model


def test_gold_model_replaces_affected_silver_request_rows_incrementally():
    model = GOLD_MODEL.read_text()

    assert "as silver_request_id" in model
    assert "delete from {{ this }} where silver_request_id in" in model
    assert "ref('silver_telehitch_requests')" in model
    assert "gold_incremental_lookback_hours" in model
    assert "affected_silver_requests as" in model
    assert "scraped_at_gmt8 >= current_timestamp()" in model
    assert "where silver_request_id in" in model
    assert "select silver_request_id" in model
    assert "from affected_silver_requests" in model
