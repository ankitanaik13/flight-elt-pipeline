{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('staging', 'flight_raw') }}
),

transformed as (
    select
        -- identifiers
        flight_id,
        fl_date::date                                                   as fl_date,
        reporting_airline,
        tail_number,
        flight_number,

        -- origin
        origin_airport_id,
        origin,
        origin_city_name,
        origin_state,

        -- destination
        dest_airport_id,
        dest,
        dest_city_name,
        dest_state,

        -- scheduled times
        crs_dep_time,
        crs_arr_time,

        -- actual times
        dep_time,
        arr_time,

        -- delay minutes (NULL → 0 for on-time flights)
        coalesce(dep_delay, 0)          as dep_delay,
        coalesce(dep_delay_minutes, 0)  as dep_delay_minutes,
        coalesce(arr_delay, 0)          as arr_delay,
        coalesce(arr_delay_minutes, 0)  as arr_delay_minutes,

        -- taxi & air
        taxi_out,
        wheels_off,
        wheels_on,
        taxi_in,
        crs_elapsed_time,
        actual_elapsed_time,
        air_time,

        -- distance
        distance,

        -- cancellation
        cancelled,
        cancellation_code,
        diverted,

        -- delay components (NULL → 0)
        coalesce(carrier_delay, 0)       as carrier_delay,
        coalesce(weather_delay, 0)       as weather_delay,
        coalesce(nas_delay, 0)           as nas_delay,
        coalesce(security_delay, 0)      as security_delay,
        coalesce(late_aircraft_delay, 0) as late_aircraft_delay,

        -- ── derived columns ──────────────────────────────────────

        -- departure hour (0–23) from 4-digit HHMM scheduled time
        left(lpad(crs_dep_time::text, 4, '0'), 2)::integer  as dep_hour,
        left(lpad(crs_arr_time::text, 4, '0'), 2)::integer  as arr_hour,

        -- flight status flags
        (cancelled = 1)                                      as is_cancelled,
        (diverted = 1)                                       as is_diverted,
        (dep_delay_minutes > 15)                             as is_delayed_departure,
        (arr_delay_minutes > 15)                             as is_delayed_arrival,

        -- total delay across all cause buckets
        coalesce(carrier_delay, 0)
            + coalesce(weather_delay, 0)
            + coalesce(nas_delay, 0)
            + coalesce(security_delay, 0)
            + coalesce(late_aircraft_delay, 0)               as total_delay_minutes,

        -- primary delay cause (largest non-null component wins)
        case
            when coalesce(carrier_delay, 0) >= coalesce(weather_delay, 0)
             and coalesce(carrier_delay, 0) >= coalesce(nas_delay, 0)
             and coalesce(carrier_delay, 0) >= coalesce(security_delay, 0)
             and coalesce(carrier_delay, 0) >= coalesce(late_aircraft_delay, 0)
             and coalesce(carrier_delay, 0) > 0
                then 'Carrier'
            when coalesce(weather_delay, 0) >= coalesce(nas_delay, 0)
             and coalesce(weather_delay, 0) >= coalesce(security_delay, 0)
             and coalesce(weather_delay, 0) >= coalesce(late_aircraft_delay, 0)
             and coalesce(weather_delay, 0) > 0
                then 'Weather'
            when coalesce(nas_delay, 0) >= coalesce(security_delay, 0)
             and coalesce(nas_delay, 0) >= coalesce(late_aircraft_delay, 0)
             and coalesce(nas_delay, 0) > 0
                then 'NAS'
            when coalesce(security_delay, 0) >= coalesce(late_aircraft_delay, 0)
             and coalesce(security_delay, 0) > 0
                then 'Security'
            when coalesce(late_aircraft_delay, 0) > 0
                then 'Late Aircraft'
            else 'None'
        end                                                  as primary_delay_cause,

        -- route shorthand
        origin || '-' || dest                                as route,

        -- departure time-of-day bucket
        case
            when left(lpad(crs_dep_time::text, 4, '0'), 2)::integer between 5  and 11 then 'Morning'
            when left(lpad(crs_dep_time::text, 4, '0'), 2)::integer between 12 and 16 then 'Afternoon'
            when left(lpad(crs_dep_time::text, 4, '0'), 2)::integer between 17 and 20 then 'Evening'
            else 'Night'
        end                                                  as dep_time_period,

        -- day-of-week name
        case extract(dow from fl_date::date)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end                                                  as day_of_week_name,

        -- weekend flag
        extract(dow from fl_date::date) in (0, 6)           as is_weekend,

        -- metadata
        ingested_at,
        source_file

    from source
)

select * from transformed
