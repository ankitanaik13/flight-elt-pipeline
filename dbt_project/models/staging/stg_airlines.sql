{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('staging', 'airline_raw') }}
),

transformed as (
    select
        nullif(trim(iata_code), '')  as iata_code,
        nullif(trim(icao_code), '')  as icao_code,
        trim(airline_name)           as carrier_name,
        trim(country)                as country,
        trim(active)                 as active,
        ingested_at
    from source
)

select * from transformed
where iata_code is not null
