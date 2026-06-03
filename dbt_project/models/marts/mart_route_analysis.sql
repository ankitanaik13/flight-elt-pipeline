{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with base as (
    select
        route,
        origin,
        dest,
        origin_airport_name,
        dest_airport_name,
        origin_city,
        dest_city,
        distance_bucket,
        carrier_name,

        count(*)                                                         as total_flights,

        round(
            sum(case when not is_cancelled and not is_delayed_arrival then 1 else 0 end)
            * 100.0 / nullif(count(*), 0), 2
        )                                                                as on_time_rate,

        round(
            sum(case when is_cancelled then 1 else 0 end)
            * 100.0 / nullif(count(*), 0), 2
        )                                                                as cancellation_rate,

        round(avg(arr_delay_minutes) filter (where arr_delay_minutes > 0)::NUMERIC, 2)  as avg_arr_delay_minutes,
        round(avg(distance)::NUMERIC, 2)                                          as avg_distance,

        mode() within group (order by primary_delay_cause)              as most_common_delay_cause,
        mode() within group (order by dep_time_period)                  as most_common_delay_period

    from {{ ref('int_flights_enriched') }}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    having count(*) >= 10
),

ranked as (
    select
        *,
        rank() over (order by on_time_rate desc)  as route_reliability_rank
    from base
)

select * from ranked
order by total_flights desc
