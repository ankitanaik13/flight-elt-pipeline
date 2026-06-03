{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with departures as (
    select
        origin                                                           as iata_code,
        origin_airport_name                                              as airport_name,
        origin_city                                                      as city,
        origin_state_name                                                as state,

        count(*)                                                         as total_departures,
        sum(case when is_cancelled then 1 else 0 end)                   as total_cancelled_departures,

        round(
            avg(dep_delay_minutes) filter (where dep_delay_minutes > 15), 2
        )                                                                as avg_departure_delay,

        round(
            avg(arr_delay_minutes) filter (where arr_delay_minutes > 15), 2
        )                                                                as avg_arrival_delay,

        -- % of departures with weather or NAS delay recorded
        round(
            sum(case when weather_delay > 0 then 1 else 0 end)
            * 100.0 / nullif(count(*), 0), 2
        )                                                                as pct_weather_delayed,

        round(
            sum(case when nas_delay > 0 then 1 else 0 end)
            * 100.0 / nullif(count(*), 0), 2
        )                                                                as pct_nas_delayed,

        -- modal values (most frequent)
        mode() within group (order by dep_hour)                         as busiest_hour,
        mode() within group (order by day_of_week_name)                 as busiest_day_of_week,
        mode() within group (order by dest)                             as top_destination,

        count(distinct route)                                           as unique_routes,
        count(distinct reporting_airline)                               as unique_carriers

    from {{ ref('int_flights_enriched') }}
    group by 1, 2, 3, 4
),

arrivals as (
    select
        dest          as iata_code,
        count(*)      as total_arrivals
    from {{ ref('int_flights_enriched') }}
    group by 1
),

combined as (
    select
        d.*,
        coalesce(a.total_arrivals, 0)                                   as total_arrivals,

        round(
            d.total_cancelled_departures * 100.0
            / nullif(d.total_departures, 0), 2
        )                                                                as departure_cancellation_rate,

        rank() over (order by d.total_departures desc)                  as airport_rank
    from departures d
    left join arrivals a using (iata_code)
)

select * from combined
order by total_departures desc
