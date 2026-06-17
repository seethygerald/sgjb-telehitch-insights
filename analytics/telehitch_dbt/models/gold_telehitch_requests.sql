{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='gold_request_id',
        schema='gold',
        alias='gold_telehitch_requests',
        file_format='delta',
        on_schema_change='fail',
        tblproperties={
            'delta.feature.timestampNtz': 'supported'
        },
        pre_hook=[
            "{% if is_incremental() %}"
            ~ "delete from {{ this }} where silver_request_id in ("
            ~ "select sha2(concat_ws('||', channel, cast(message_id as string)), 256) "
            ~ "from {{ ref('silver_telehitch_requests') }} "
            ~ "where scraped_at_gmt8 >= current_timestamp() - interval "
            ~ "{{ var('gold_incremental_lookback_hours') }} hours"
            ~ ")"
            ~ "{% endif %}"
        ]
    )
}}

with silver_requests as (
    select
        sha2(concat_ws('||', channel, cast(message_id as string)), 256)
            as silver_request_id,
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
        lower(trim(regexp_replace(pickup_location, '\\s+', ' ')))
            as normalized_pickup_location,
        lower(trim(regexp_replace(dropoff_location, '\\s+', ' ')))
            as normalized_dropoff_location,
        request_time_text,
        pax_count,
        parse_status
    from {{ ref('silver_telehitch_requests') }}
    where parse_status = 'parsed'
),

affected_silver_requests as (
    select distinct
        sha2(concat_ws('||', channel, cast(message_id as string)), 256)
            as silver_request_id
    from {{ ref('silver_telehitch_requests') }}
    where parse_status = 'parsed'

    {% if is_incremental() %}
      and scraped_at_gmt8 >= current_timestamp()
          - interval {{ var('gold_incremental_lookback_hours') }} hours
    {% endif %}
),

pickup_geocodes as (
    select
        normalized_location,
        postal_code,
        latitude,
        longitude,
        formatted_address,
        search_value,
        resolution_status,
        resolution_source,
        result_count,
        error_message,
        row_number() over (
            partition by
                normalized_location,
                coalesce(postal_code, ''),
                coalesce(formatted_address, ''),
                coalesce(cast(latitude as string), ''),
                coalesce(cast(longitude as string), '')
            order by
                case resolution_source
                    when 'override' then 1
                    when 'onemap' then 2
                    when 'rule' then 3
                    else 4
                end,
                resolved_at desc,
                attempted_at desc
        ) as geocode_rank
    from {{ source('telehitch_silver', 'location_geocodes') }}
    where resolution_status = 'resolved'
),

dropoff_geocodes as (
    select
        normalized_location,
        postal_code,
        latitude,
        longitude,
        formatted_address,
        search_value,
        resolution_status,
        resolution_source,
        result_count,
        error_message,
        row_number() over (
            partition by
                normalized_location,
                coalesce(postal_code, ''),
                coalesce(formatted_address, ''),
                coalesce(cast(latitude as string), ''),
                coalesce(cast(longitude as string), '')
            order by
                case resolution_source
                    when 'override' then 1
                    when 'onemap' then 2
                    when 'rule' then 3
                    else 4
                end,
                resolved_at desc,
                attempted_at desc
        ) as geocode_rank
    from {{ source('telehitch_silver', 'location_geocodes') }}
    where resolution_status = 'resolved'
),

request_geocode_combinations as (
    select
        requests.*,
        pickup_geocodes.postal_code as pickup_postal_code,
        pickup_geocodes.latitude as pickup_latitude,
        pickup_geocodes.longitude as pickup_longitude,
        pickup_geocodes.formatted_address as pickup_formatted_address,
        pickup_geocodes.search_value as pickup_search_value,
        pickup_geocodes.resolution_source as pickup_resolution_source,
        pickup_geocodes.result_count as pickup_geocode_result_count,
        pickup_geocodes.error_message as pickup_geocode_error_message,
        dropoff_geocodes.postal_code as dropoff_postal_code,
        dropoff_geocodes.latitude as dropoff_latitude,
        dropoff_geocodes.longitude as dropoff_longitude,
        dropoff_geocodes.formatted_address as dropoff_formatted_address,
        dropoff_geocodes.search_value as dropoff_search_value,
        dropoff_geocodes.resolution_source as dropoff_resolution_source,
        dropoff_geocodes.result_count as dropoff_geocode_result_count,
        dropoff_geocodes.error_message as dropoff_geocode_error_message
    from silver_requests as requests
    left join pickup_geocodes
      on pickup_geocodes.normalized_location = requests.normalized_pickup_location
     and pickup_geocodes.geocode_rank = 1
    left join dropoff_geocodes
      on dropoff_geocodes.normalized_location = requests.normalized_dropoff_location
     and dropoff_geocodes.geocode_rank = 1
),

with_dedup_key as (
    select
        *,
        coalesce(
            cast(sender_id as string),
            lower(trim(sender_handle)),
            concat('unknown:', channel, '#', cast(message_id as string))
        ) as telegram_user_key,
        coalesce(pickup_postal_code, normalized_pickup_location)
            as pickup_dedup_location_key,
        coalesce(dropoff_postal_code, normalized_dropoff_location)
            as dropoff_dedup_location_key
    from request_geocode_combinations
),

deduplicated as (
    select
        candidate.*
    from with_dedup_key as candidate
    left join with_dedup_key as prior_post
      on prior_post.telegram_user_key = candidate.telegram_user_key
     and prior_post.pickup_dedup_location_key = candidate.pickup_dedup_location_key
     and prior_post.dropoff_dedup_location_key = candidate.dropoff_dedup_location_key
     and (
         cast(prior_post.message_date_gmt8 as timestamp)
             < cast(candidate.message_date_gmt8 as timestamp)
         or (
             cast(prior_post.message_date_gmt8 as timestamp)
                 = cast(candidate.message_date_gmt8 as timestamp)
             and concat(prior_post.channel, '#', cast(prior_post.message_id as string))
                 < concat(candidate.channel, '#', cast(candidate.message_id as string))
         )
     )
     and cast(prior_post.message_date_gmt8 as timestamp)
         >= cast(candidate.message_date_gmt8 as timestamp) - interval 2 hours
    where prior_post.message_id is null
)

select
    sha2(concat_ws(
        '||',
        silver_request_id,
        coalesce(pickup_dedup_location_key, ''),
        coalesce(dropoff_dedup_location_key, ''),
        coalesce(pickup_formatted_address, ''),
        coalesce(dropoff_formatted_address, '')
    ), 256) as gold_request_id,
    silver_request_id,
    true as is_canonical_request,
    telegram_user_key,
    channel,
    topic_id,
    message_id,
    message_date_gmt8,
    sender_id,
    sender_handle,
    request_type,
    pickup_location,
    dropoff_location,
    normalized_pickup_location,
    normalized_dropoff_location,
    pickup_postal_code,
    pickup_formatted_address,
    pickup_latitude,
    pickup_longitude,
    pickup_search_value,
    pickup_resolution_source,
    pickup_geocode_result_count,
    pickup_geocode_error_message,
    dropoff_postal_code,
    dropoff_formatted_address,
    dropoff_latitude,
    dropoff_longitude,
    dropoff_search_value,
    dropoff_resolution_source,
    dropoff_geocode_result_count,
    dropoff_geocode_error_message,
    request_time_text,
    pax_count,
    scraped_at_gmt8,
    message
from deduplicated

{% if is_incremental() %}
where silver_request_id in (
    select silver_request_id
    from affected_silver_requests
)
{% endif %}
