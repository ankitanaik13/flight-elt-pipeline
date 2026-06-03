{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with base as (
    select
        carrier_name,
        reporting_airline,
        date_trunc('month', fl_date)::date  as flight_month,

        -- counts
        count(*)                                                         as total_flights,
        sum(case when is_cancelled then 1 else 0 end)                   as total_cancelled,
        sum(case when is_diverted  then 1 else 0 end)                   as total_diverted,

        -- on-time rates (standard FAA: ≤ 15 min late = on time)
        round(
            sum(case when not is_cancelled and not is_delayed_departure then 1 else 0 end)
            * 100.0 / nullif(count(*), 0), 2
        )                                                                as on_time_departure_rate,

        round(
            sum(case when not is_cancelled and not is_delayed_arrival then 1 else 0 end)
            * 100.0 / nullif(count(*), 0), 2
        )                                                                as on_time_arrival_rate,

        -- average delays (delayed flights only, to avoid diluting with on-time)
        round(avg(dep_delay_minutes) filter (where dep_delay_minutes > 15)::NUMERIC, 2)  as avg_dep_delay_minutes,
        round(avg(arr_delay_minutes) filter (where arr_delay_minutes > 15)::NUMERIC, 2)  as avg_arr_delay_minutes,

        -- delay component averages (all flights, NULLs excluded by AVG)
        round(avg(nullif(carrier_delay, 0))::NUMERIC, 2)      as avg_carrier_delay,
        round(avg(nullif(weather_delay, 0))::NUMERIC, 2)      as avg_weather_delay,
        round(avg(nullif(nas_delay, 0))::NUMERIC, 2)          as avg_nas_delay,
        round(avg(nullif(late_aircraft_delay, 0))::NUMERIC, 2) as avg_late_aircraft_delay,

        -- carrier-caused share of total delay minutes
        round(
            sum(carrier_delay) * 100.0
            / nullif(sum(total_delay_minutes), 0), 2
        )                                                                as pct_carrier_caused

    from {{ ref('int_flights_enriched') }}
    group by 1, 2, 3
),

ranked as (
    select
        *,
        round(
            total_cancelled * 100.0 / nullif(total_flights, 0), 2
        )                                                                as cancellation_rate,

        rank() over (order by total_flights desc)                        as total_flights_rank,

        -- reliability: higher on-time arrival, lower cancellation = better
        round(on_time_arrival_rate - (total_cancelled * 100.0 / nullif(total_flights, 0)), 2)
                                                                         as reliability_score
    from base
)

select * from ranked
order by reliability_score desc nulls last
