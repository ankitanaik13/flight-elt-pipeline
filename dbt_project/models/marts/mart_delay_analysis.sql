{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with base as (
    select
        delay_label,
        primary_delay_cause,
        dep_time_period,
        day_of_week_name,
        is_weekend,
        distance_bucket,
        date_trunc('month', fl_date)::date  as flight_month,

        count(*)                                                         as flight_count,

        round(avg(arr_delay_minutes)::NUMERIC, 2)                                 as avg_arr_delay_minutes,
        round(avg(nullif(carrier_delay, 0))::NUMERIC, 2)                         as avg_carrier_delay,
        round(avg(nullif(weather_delay, 0))::NUMERIC, 2)                         as avg_weather_delay,
        round(avg(nullif(nas_delay, 0))::NUMERIC, 2)                             as avg_nas_delay,
        round(avg(nullif(late_aircraft_delay, 0))::NUMERIC, 2)                   as avg_late_aircraft_delay,
        round(avg(nullif(security_delay, 0))::NUMERIC, 2)                        as avg_security_delay,

        -- most common offenders in each segment
        mode() within group (order by carrier_name)                     as worst_carrier,
        mode() within group (order by route)                            as worst_route

    from {{ ref('int_flights_enriched') }}
    group by 1, 2, 3, 4, 5, 6, 7
),

with_pct as (
    select
        *,
        round(
            flight_count * 100.0 / sum(flight_count) over (), 4
        )  as pct_of_total
    from base
)

select * from with_pct
order by flight_count desc
