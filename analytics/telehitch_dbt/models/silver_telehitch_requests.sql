{{
    config(
        unique_key=['channel', 'message_id'],
        on_schema_change='fail'
    )
}}

with raw_messages as (
    select
        channel,
        topic_id,
        id as message_id,
        message_date_gmt8,
        message,
        sender_id,
        sender_handle,
        scraped_at_gmt8,
        lower(trim(message)) as message_lower,
        row_number() over (
            partition by channel, id
            order by scraped_at_gmt8 desc
        ) as scrape_recency_rank
    from {{ source('telegram_bronze', 'messages') }}
    where message is not null

    {% if is_incremental() %}
      and scraped_at_gmt8 >=
          current_timestamp() - interval {{ var('incremental_lookback_hours') }} hours
    {% endif %}
),

requests_only as (
    select
        *,
        case
            when regexp_like(
                message_lower,
                r'\bdrivers?\s+looking\s+(?:for\s+)?(?:hitchers?|passengers?)\b'
            ) or regexp_like(
                message_lower,
                r'\blooking\s+for\s+passengers?\b'
            ) or (
                regexp_like(
                    message_lower,
                    r'(?:接送服务|\bwhole\s+car\b|\b[5-9]\s*seater\b)'
                )
                and regexp_like(
                    message_lower,
                    r'\b(?:pick\s*up|pickup)\b'
                )
                and regexp_like(
                    message_lower,
                    r'\b(?:drop\s*off|dropoff|drop)\b'
                )
            ) then 'driver_request'
            when regexp_like(
                message_lower,
                r'\bhitchers?\s+looking\s+(?:for\s+)?drivers?\b'
            ) or regexp_like(
                message_lower,
                r'\blooking\s+for\s+drivers?\b'
            ) or (
                regexp_like(
                    message_lower,
                    r'\b(?:pick\s*up|pickup)\b'
                )
                and regexp_like(
                    message_lower,
                    r'\b(?:drop\s*off|dropoff|drop)\b'
                )
            ) then 'hitcher_request'
        end as request_type
    from raw_messages
    where scrape_recency_rank = 1
),

extracted as (
    select
        channel,
        topic_id,
        message_id,
        message_date_gmt8,
        message,
        sender_id,
        sender_handle,
        scraped_at_gmt8,
        request_type,

        nullif(trim(regexp_extract(
            message,
            r'(?im)^\s*(?:pick\s*up(?:\s+(?:point|location))?|pickup(?:\s+(?:point|location))?|from)\s*:\s*([^\r\n]+)',
            1
        )), '') as pickup_location,

        nullif(trim(regexp_extract(
            message,
            r'(?im)^\s*(?:drop\s*off(?:\s+(?:point|location))?|dropoff(?:\s+(?:point|location))?|destination|to)\s*:\s*([^\r\n]+)',
            1
        )), '') as dropoff_location,

        nullif(trim(regexp_extract(
            message,
            r'(?im)^\s*(?:time|when|pickup\s+time)\s*:\s*([^\r\n]+)',
            1
        )), '') as request_time_text,

        try_cast(nullif(regexp_extract(
            message,
            r'(?i)\bpax\s*:?\s*(\d{1,2})\b',
            1
        ), '') as int) as pax_after_label,

        try_cast(nullif(regexp_extract(
            message,
            r'(?i)\b(\d{1,2})\s*(?:pax|passengers?)\b',
            1
        ), '') as int) as pax_before_label
    from requests_only
    where request_type is not null
),

cleaned as (
    select
        channel,
        topic_id,
        message_id,
        message_date_gmt8,
        message,
        sender_id,
        sender_handle,
        scraped_at_gmt8,
        request_type,
        pickup_location,
        dropoff_location,
        request_time_text,
        case
            when regexp_like(lower(message), r'\bwhole\s+car\b') then null
            when regexp_like(lower(message), r'\bpax\s*:?\s*(?:-|nil|n/?a|tbc)(?:\s|$)') then null
            when regexp_like(lower(message), r'\b\d{1,2}\s*[-–]\s*\d{1,2}\s*(?:pax|passengers?)\b') then null
            when regexp_like(lower(message), r'\bpax\s*:?\s*\d{1,2}\s*[-–]\s*\d{1,2}\b') then null
            when coalesce(pax_after_label, pax_before_label) between 1 and 10
                then coalesce(pax_after_label, pax_before_label)
            when pax_after_label is null and pax_before_label is null then 1
            else null
        end as pax_count,
        case
            when pickup_location is null then 'missing_pickup'
            when dropoff_location is null then 'missing_dropoff'
            when request_time_text is null then 'missing_time'
            else 'parsed'
        end as parse_status
    from extracted
)

select * from cleaned
