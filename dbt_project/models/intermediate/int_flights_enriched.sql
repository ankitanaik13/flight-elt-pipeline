{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

with flights as (
    select * from {{ ref('stg_flights') }}
),

airlines as (
    select * from {{ ref('stg_airlines') }}
),

airports as (
    select * from {{ ref('stg_airports') }}
),

cancellation_codes as (
    select * from {{ ref('cancellation_codes') }}
),

delay_categories as (
    select * from {{ ref('delay_categories') }}
),

enriched as (
    select
        -- ── Flight identifiers ──────────────────────────────────
        f.flight_id,
        f.fl_date,
        f.reporting_airline,
        f.tail_number,
        f.flight_number,
        f.route,

        -- ── Carrier ─────────────────────────────────────────────
        al.carrier_name,

        -- ── Origin airport ──────────────────────────────────────
        f.origin,
        oa.airport_name   as origin_airport_name,
        oa.city           as origin_city,
        oa.us_state       as origin_state_name,
        oa.latitude       as origin_lat,
        oa.longitude      as origin_lon,

        -- ── Destination airport ──────────────────────────────────
        f.dest,
        da.airport_name   as dest_airport_name,
        da.city           as dest_city,
        da.us_state       as dest_state_name,
        da.latitude       as dest_lat,
        da.longitude      as dest_lon,

        -- ── Timing ───────────────────────────────────────────────
        f.crs_dep_time,
        f.dep_time,
        f.dep_hour,
        f.dep_time_period,
        f.crs_arr_time,
        f.arr_time,
        f.arr_hour,
        f.day_of_week_name,
        f.is_weekend,

        -- ── Delays ───────────────────────────────────────────────
        f.dep_delay,
        f.dep_delay_minutes,
        f.arr_delay,
        f.arr_delay_minutes,
        f.carrier_delay,
        f.weather_delay,
        f.nas_delay,
        f.security_delay,
        f.late_aircraft_delay,
        f.total_delay_minutes,
        f.primary_delay_cause,
        dc.label          as delay_label,

        -- ── Status flags ─────────────────────────────────────────
        f.is_cancelled,
        f.is_diverted,
        f.is_delayed_departure,
        f.is_delayed_arrival,

        -- ── Cancellation enrichment ───────────────────────────────
        f.cancellation_code,
        cc.reason         as cancellation_reason,

        -- ── Flight characteristics ────────────────────────────────
        f.distance,
        f.air_time,
        f.actual_elapsed_time,
        f.crs_elapsed_time,
        f.taxi_out,
        f.taxi_in,

        -- ── Derived categorisations ───────────────────────────────
        case
            when f.distance < 500  then 'Short Haul'
            when f.distance < 1500 then 'Medium Haul'
            else 'Long Haul'
        end                         as distance_bucket,

        (oa.us_state = da.us_state
         and oa.us_state is not null)  as is_same_state_flight,

        -- ── Metadata ─────────────────────────────────────────────
        f.ingested_at,
        f.source_file

    from flights f
    left join airlines al
        on f.reporting_airline = al.iata_code
    left join airports oa
        on f.origin = oa.iata_code
    left join airports da
        on f.dest = da.iata_code
    left join cancellation_codes cc
        on f.cancellation_code = cc.code
    left join delay_categories dc
        on f.arr_delay_minutes between dc.min_minutes and dc.max_minutes
)

select * from enriched
