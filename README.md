# US Flight On-Time Performance ELT Pipeline

## Overview

This pipeline ingests monthly US domestic flight data from the Bureau of Transportation Statistics (BTS), loads it into a PostgreSQL data warehouse, and transforms it through a 3-layer dbt architecture (staging → intermediate → marts). Built with Apache Airflow for orchestration, it answers critical business questions about airline reliability, airport congestion, delay causation, and route performance — all from a single Docker Compose command.

## Architecture

```
BTS API → [Airflow: flight_data_ingestion DAG]
              │
              ├── download_flight_zip  (~500 MB ZIP)
              ├── extract_flight_csv
              ├── load_lookup_tables   (airports + airlines)
              └── load_flights_to_staging
                            │
              PostgreSQL: pipeline_db (staging schema)
              ├── staging.flight_raw     (~600K rows/month)
              ├── staging.airport_raw    (~7K rows)
              └── staging.airline_raw    (~400 rows)
                            │
     [Airflow: flight_dbt_transformations DAG]
                            │
              dbt Staging Layer (views)
              ├── stg_flights           ← cleaned + derived columns
              ├── stg_airlines          ← normalised carrier names
              └── stg_airports          ← geo-enriched airports
                            │
              dbt Intermediate Layer (tables)
              └── int_flights_enriched  ← all lookups joined
                            │
              dbt Marts Layer (tables)
              ├── mart_carrier_performance   ← monthly carrier KPIs
              ├── mart_airport_performance   ← airport throughput + delays
              ├── mart_delay_analysis        ← delay cause breakdown
              └── mart_route_analysis        ← route reliability ranking
```

## Business Questions Answered

- Which airlines have the worst on-time arrival rates and highest cancellation rates?
- Which airports are the biggest departure bottlenecks by volume and delay?
- What is the primary cause of delays — carrier, weather, NAS, or late aircraft?
- Which routes are most unreliable, and which carriers operate them?
- How do delays vary by time of day (morning vs. evening)?
- Are weekend flights more or less reliable than weekday flights?
- Which routes have the highest cancellation rates?
- How does flight distance (short/medium/long haul) correlate with delay severity?

## Tech Stack

| Tool              | Version | Purpose                                   |
|-------------------|---------|-------------------------------------------|
| Apache Airflow    | 2.8.0   | DAG orchestration and scheduling          |
| PostgreSQL        | 15      | Data warehouse (staging + marts)          |
| dbt-core          | 1.7.0   | SQL transformation framework              |
| dbt-postgres      | 1.7.0   | dbt adapter for PostgreSQL                |
| pandas            | 2.1.0   | Chunked CSV loading and transformation    |
| SQLAlchemy        | 1.4.50  | Database abstraction layer                |
| psycopg2          | 2.9.9   | PostgreSQL driver                         |
| requests          | 2.31.0  | HTTP download with streaming              |
| Docker Compose    | v2      | Container orchestration                   |

## Prerequisites

- **Docker Desktop** with at least 5 GB RAM allocated
- **Docker Compose v2** (`docker compose` — note: no hyphen)
- **Git**
- **~5 GB free disk space** (PostgreSQL data + BTS ZIP + extracted CSV)

## Quick Start

```bash
# 1. Clone the repository
git clone <repo-url>
cd flight-elt-pipeline

# 2. Copy the example env file and review settings
cp .env.example .env

# 3. Initialise Airflow (creates admin user, migrates DB)
make init

# 4. Start all services
make up

# 5. Wait ~3 minutes for healthchecks to pass, then open the UI
open http://localhost:8080
# Login: admin / admin

# 6. Enable and trigger the ingestion DAG
#    Airflow UI → DAGs → flight_data_ingestion → Enable → Trigger

# 7. After ingestion succeeds, trigger the dbt DAG
#    Airflow UI → DAGs → flight_dbt_transformations → Trigger

# 8. Connect to the warehouse and explore
make db
```

## Verifying the Pipeline

```sql
-- Raw flights loaded
SELECT COUNT(*), MIN(fl_date), MAX(fl_date)
FROM staging.flight_raw;

-- Top 5 most delayed carriers (avg arrival delay, delayed flights only)
SELECT carrier_name,
       ROUND(AVG(avg_arr_delay_minutes), 2) AS avg_delay_min,
       SUM(total_flights)                   AS total_flights
FROM marts.mart_carrier_performance
GROUP BY carrier_name
ORDER BY avg_delay_min DESC NULLS LAST
LIMIT 5;

-- Most cancelled routes
SELECT route, cancellation_rate, total_flights
FROM marts.mart_route_analysis
ORDER BY cancellation_rate DESC
LIMIT 10;

-- Delay causes breakdown
SELECT primary_delay_cause,
       SUM(flight_count)                        AS occurrences,
       ROUND(AVG(avg_arr_delay_minutes), 2)     AS avg_delay_min
FROM marts.mart_delay_analysis
GROUP BY primary_delay_cause
ORDER BY occurrences DESC;

-- Busiest airports by departures
SELECT iata_code, airport_name, total_departures, airport_rank
FROM marts.mart_airport_performance
ORDER BY airport_rank
LIMIT 10;
```

## Makefile Commands

| Command         | Description                                               |
|-----------------|-----------------------------------------------------------|
| `make up`       | Start all Docker services in the background               |
| `make down`     | Stop containers and remove volumes                        |
| `make init`     | Run Airflow DB migration and create admin user            |
| `make restart`  | Restart all services                                      |
| `make logs`     | Tail logs for all services                                |
| `make ps`       | Show status of running containers                         |
| `make db`       | Open a psql shell in `pipeline_db`                        |
| `make dbt-run`  | Run all dbt models manually                               |
| `make dbt-test` | Run all dbt schema + singular tests                       |
| `make dbt-docs` | Generate dbt documentation site                           |
| `make clean`    | Tear down everything, including named volumes             |

## Data Model

### Layer 1 — Staging (views)
Raw data cleaned and normalised. No joins. Views re-run on every query, keeping storage minimal. Key additions: derived flags (`is_cancelled`, `is_delayed_arrival`), time-of-day buckets, delay cause attribution, route shorthand.

### Layer 2 — Intermediate (table)
`int_flights_enriched` joins flights to all four lookup sources (airlines, airports × 2, cancellation codes, delay categories). Materialised as a table so marts don't re-scan raw data on every query.

### Layer 3 — Marts (tables)
Business-facing aggregates answering one analytical domain each. Fully denormalised for BI tool compatibility. Exposed via the `marts` PostgreSQL schema.

## Key Design Decisions

**LocalExecutor over CeleryExecutor** — single-machine development setup; Celery adds Redis dependency without benefit at this scale.

**PostgreSQL over SQLite for the warehouse** — enables concurrent reads, proper schema separation, and `FILTER` aggregates used by dbt models.

**Views for staging, tables for marts** — staging views reflect raw data instantly without storage overhead; mart tables are pre-aggregated for sub-second BI query response.

**50k-row chunk loading** — BTS monthly CSVs exceed 600k rows (~200 MB). Chunked loading caps peak memory at ~100 MB regardless of file size, making the pipeline container-safe.

**IF NOT EXISTS everywhere** — all DDL and dbt models are idempotent; re-triggering a DAG never fails due to already-existing objects.

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Airflow containers not starting | Increase Docker Desktop memory to ≥5 GB |
| BTS download times out | The BTS server can be slow; retry or increase timeout in `bts_flight_loader.py` |
| `dbt profile not found` | Check that `dbt_project/` is mounted to `/opt/airflow/dbt_project` in docker-compose.yml |
| `Connection refused` on postgres | Wait for the postgres healthcheck to pass before triggering DAGs |
| Out of memory during CSV load | Reduce `chunk_size` in `load_to_staging()` from 50000 to 25000 |
| `airflow-init` keeps restarting | It exits 0 on success — this is expected; webserver/scheduler wait for it |

## Resume Bullets

- **Designed and shipped an end-to-end ELT pipeline** ingesting 600K+ monthly US flight records from the BTS API into PostgreSQL using Apache Airflow 2.8, achieving full idempotency via chunked streaming loads and IF-NOT-EXISTS DDL.

- **Modelled a 3-layer dbt transformation architecture** (staging views → enriched intermediate table → 4 analytical marts) with 30+ column-level schema tests covering uniqueness, null-safety, accepted ranges, and accepted values — catching data quality issues before they reach analysts.

- **Containerised the complete data stack** (Airflow, PostgreSQL, dbt) with Docker Compose, including service health checks, named volumes, and a one-command `make up` developer experience, reducing onboarding time to under 5 minutes.

## Dashboard

An interactive Streamlit dashboard visualizes all analytical marts with:
- Carrier reliability leaderboard (Delta #1, Alaska last)
- Delay causes pie chart (Late Aircraft 38.5%)
- Time of day delay analysis (Night worst, Morning best)
- Top 10 most unreliable routes table
- Airport performance comparison (busiest vs most delayed)
- Cancellation rates by airline
- Flight distance vs delay analysis

**Run locally:**
```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

## Key Insights

- **Delta Air Lines** most reliable at 80.4% on-time rate
- **Alaska Airlines** lowest reliability at ~42%
- **Late Aircraft** cascading delays are the #1 cause at 38.5%
- **Night flights** have worst delays, Morning flights are best
- **ATL (Hartsfield-Jackson)** is the busiest airport with 131,575 departures
- **OGG-PDX route** has only 8.7% on-time rate
- **3,283,626** total flight records analyzed
