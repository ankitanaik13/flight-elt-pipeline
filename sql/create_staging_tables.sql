-- All DDL uses IF NOT EXISTS — safe to re-run on every DAG trigger.

-- ─────────────────────────────────────────────────────────────
-- Table 1: Raw BTS On-Time Performance flights
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.flight_raw (
    flight_id               SERIAL PRIMARY KEY,
    fl_date                 DATE,
    reporting_airline       VARCHAR(10),
    tail_number             VARCHAR(20),
    flight_number           VARCHAR(10),
    origin_airport_id       INTEGER,
    origin                  VARCHAR(10),
    origin_city_name        VARCHAR(100),
    origin_state            VARCHAR(100),
    dest_airport_id         INTEGER,
    dest                    VARCHAR(10),
    dest_city_name          VARCHAR(100),
    dest_state              VARCHAR(100),
    crs_dep_time            INTEGER,
    dep_time                FLOAT,
    dep_delay               FLOAT,
    dep_delay_minutes       FLOAT,
    taxi_out                FLOAT,
    wheels_off              FLOAT,
    wheels_on               FLOAT,
    taxi_in                 FLOAT,
    crs_arr_time            INTEGER,
    arr_time                FLOAT,
    arr_delay               FLOAT,
    arr_delay_minutes       FLOAT,
    cancelled               FLOAT,
    cancellation_code       VARCHAR(5),
    diverted                FLOAT,
    crs_elapsed_time        FLOAT,
    actual_elapsed_time     FLOAT,
    air_time                FLOAT,
    distance                FLOAT,
    carrier_delay           FLOAT,
    weather_delay           FLOAT,
    nas_delay               FLOAT,
    security_delay          FLOAT,
    late_aircraft_delay     FLOAT,
    ingested_at             TIMESTAMP DEFAULT NOW(),
    source_file             VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_flight_raw_fl_date
    ON staging.flight_raw (fl_date);

CREATE INDEX IF NOT EXISTS idx_flight_raw_reporting_airline
    ON staging.flight_raw (reporting_airline);

CREATE INDEX IF NOT EXISTS idx_flight_raw_origin
    ON staging.flight_raw (origin);

CREATE INDEX IF NOT EXISTS idx_flight_raw_dest
    ON staging.flight_raw (dest);

CREATE INDEX IF NOT EXISTS idx_flight_raw_cancelled
    ON staging.flight_raw (cancelled);

-- ─────────────────────────────────────────────────────────────
-- Table 2: Airport metadata (OpenFlights)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.airport_raw (
    airport_id      SERIAL PRIMARY KEY,
    iata_code       VARCHAR(10),
    icao_code       VARCHAR(10),
    airport_name    VARCHAR(200),
    city            VARCHAR(100),
    country         VARCHAR(100),
    latitude        FLOAT,
    longitude       FLOAT,
    altitude        FLOAT,
    timezone        VARCHAR(100),
    dst             VARCHAR(5),
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_airport_raw_iata_code
    ON staging.airport_raw (iata_code);

-- ─────────────────────────────────────────────────────────────
-- Table 3: Airline metadata (OpenFlights)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.airline_raw (
    airline_id      SERIAL PRIMARY KEY,
    iata_code       VARCHAR(10),
    icao_code       VARCHAR(10),
    airline_name    VARCHAR(200),
    country         VARCHAR(100),
    active          VARCHAR(5),
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_airline_raw_iata_code
    ON staging.airline_raw (iata_code);
