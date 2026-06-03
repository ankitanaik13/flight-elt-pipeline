{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('staging', 'airport_raw') }}
),

transformed as (
    select
        nullif(trim(iata_code), '')                           as iata_code,
        nullif(trim(icao_code), '')                           as icao_code,
        trim(airport_name)                                    as airport_name,
        trim(city)                                            as city,
        trim(country)                                         as country,
        round(latitude::numeric, 4)                           as latitude,
        round(longitude::numeric, 4)                          as longitude,
        altitude,
        trim(timezone)                                        as timezone,
        trim(dst)                                             as dst,

        -- US state extracted from "City, ST" pattern in the city field
        case
            when trim(country) = 'United States'
                then nullif(trim(split_part(trim(city), ', ', 2)), '')
            else null
        end                                                   as us_state,

        ingested_at
    from source
)

select * from transformed
where iata_code is not null
  and iata_code != '\N'
  and length(iata_code) = 3
