"""
DAG: flight_data_ingestion

Downloads BTS On-Time Performance data and lookup tables, validates them,
and loads everything into PostgreSQL staging tables.

Idempotent: create_schemas_and_tables uses IF NOT EXISTS; lookup tables are
replaced on each run; flight_raw is appended (deduplicated by source_file
if needed upstream).
"""

import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# DAG defaults
# ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "depends_on_past": False,
}

dag = DAG(
    dag_id="flight_data_ingestion",
    description=(
        "Ingests BTS flight on-time data and OpenFlights lookup tables "
        "into PostgreSQL staging schema."
    ),
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "flights", "bts", "staging"],
    default_args=DEFAULT_ARGS,
)


# ─────────────────────────────────────────────────────────────
# Helper: get env var or raise clearly
# ─────────────────────────────────────────────────────────────
def _env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(f"Required env var {key!r} is not set.")
    return val


# ─────────────────────────────────────────────────────────────
# Task 1 — Create schemas and staging tables
# ─────────────────────────────────────────────────────────────
def create_schemas_and_tables(**context):
    """Execute DDL scripts to create schemas and staging tables in pipeline_db."""
    conn = psycopg2.connect(
        host=_env("PIPELINE_DB_HOST"),
        port=int(_env("PIPELINE_DB_PORT")),
        dbname=_env("PIPELINE_DB"),
        user=_env("PIPELINE_DB_USER"),
        password=_env("PIPELINE_DB_PASSWORD"),
    )
    conn.autocommit = True

    sql_dir = "/opt/airflow/sql"
    for script in ["create_schemas.sql", "create_staging_tables.sql"]:
        path = os.path.join(sql_dir, script)
        with open(path) as fh:
            sql = fh.read()
        with conn.cursor() as cur:
            cur.execute(sql)
        logger.info("Executed %s", script)

    conn.close()
    logger.info("Schemas and tables ready in pipeline_db.")


# ─────────────────────────────────────────────────────────────
# Task 2 — Load lookup tables
# ─────────────────────────────────────────────────────────────
def load_lookup_tables(**context):
    """Download and load airport and airline reference data."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.lookup_loader import LookupLoader

    loader = LookupLoader(conn_string=_env("PIPELINE_CONN_STRING"))

    airport_result = loader.load_airports(_env("AIRPORTS_URL"))
    airline_result = loader.load_airlines(_env("AIRLINES_URL"))

    context["ti"].xcom_push(key="airports_loaded", value=airport_result["rows_loaded"])
    context["ti"].xcom_push(key="airlines_loaded", value=airline_result["rows_loaded"])

    logger.info(
        "Lookups loaded — airports: %d, airlines: %d",
        airport_result["rows_loaded"],
        airline_result["rows_loaded"],
    )


# ─────────────────────────────────────────────────────────────
# Task 3 — Download BTS ZIP
# ─────────────────────────────────────────────────────────────
def download_flight_zip(**context):
    """Stream-download the BTS On-Time Performance ZIP file."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.bts_flight_loader import BtsFlightLoader

    loader = BtsFlightLoader(
        conn_string=_env("PIPELINE_CONN_STRING"),
        zip_url=_env("BTS_FLIGHT_URL"),
        local_zip=_env("BTS_LOCAL_ZIP"),
        local_csv=_env("BTS_LOCAL_CSV"),
    )

    zip_path = loader.download_zip()
    size_mb = os.path.getsize(zip_path) / 1e6
    logger.info("ZIP downloaded: %.1f MB → %s", size_mb, zip_path)

    context["ti"].xcom_push(key="zip_path", value=zip_path)


# ─────────────────────────────────────────────────────────────
# Task 4 — Extract and validate CSV
# ─────────────────────────────────────────────────────────────
def extract_flight_csv(**context):
    """Extract the ZIP and run a pre-load schema validation."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.bts_flight_loader import BtsFlightLoader

    zip_path = context["ti"].xcom_pull(task_ids="download_flight_zip", key="zip_path")

    loader = BtsFlightLoader(
        conn_string=_env("PIPELINE_CONN_STRING"),
        zip_url=_env("BTS_FLIGHT_URL"),
        local_zip=_env("BTS_LOCAL_ZIP"),
        local_csv=_env("BTS_LOCAL_CSV"),
    )

    csv_path = loader.extract_csv(zip_path)
    validation = loader.validate_csv(csv_path)

    logger.info(
        "CSV validated — %d columns, ~%s estimated rows",
        len(validation["columns"]),
        f"{validation['estimated_total_rows']:,}",
    )

    context["ti"].xcom_push(key="csv_path", value=csv_path)
    context["ti"].xcom_push(key="csv_validation", value=validation)


# ─────────────────────────────────────────────────────────────
# Task 5 — Chunk-load flights to staging
# ─────────────────────────────────────────────────────────────
def load_flights_to_staging(**context):
    """Chunk-read the CSV and write all rows to staging.flight_raw."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.bts_flight_loader import BtsFlightLoader

    csv_path = context["ti"].xcom_pull(task_ids="extract_flight_csv", key="csv_path")

    loader = BtsFlightLoader(
        conn_string=_env("PIPELINE_CONN_STRING"),
        zip_url=_env("BTS_FLIGHT_URL"),
        local_zip=_env("BTS_LOCAL_ZIP"),
        local_csv=_env("BTS_LOCAL_CSV"),
    )

    result = loader.load_to_staging(csv_path)
    logger.info(
        "Load complete — %s rows in %.1fs",
        f"{result['total_rows']:,}",
        result["duration_seconds"],
    )

    context["ti"].xcom_push(key="load_result", value=result)


# ─────────────────────────────────────────────────────────────
# Task 6 — Validate and report
# ─────────────────────────────────────────────────────────────
def validate_and_report(**context):
    """Run post-load validation and log a full pipeline summary."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.bts_flight_loader import BtsFlightLoader

    loader = BtsFlightLoader(
        conn_string=_env("PIPELINE_CONN_STRING"),
        zip_url=_env("BTS_FLIGHT_URL"),
        local_zip=_env("BTS_LOCAL_ZIP"),
        local_csv=_env("BTS_LOCAL_CSV"),
    )

    validation = loader.validate_load()
    load_result = context["ti"].xcom_pull(task_ids="load_flights_to_staging", key="load_result")
    airports_loaded = context["ti"].xcom_pull(task_ids="load_lookup_tables", key="airports_loaded")
    airlines_loaded = context["ti"].xcom_pull(task_ids="load_lookup_tables", key="airlines_loaded")

    cancellation_rate = (
        round(validation["total_cancelled"] / validation["total_rows"] * 100, 2)
        if validation["total_rows"]
        else 0
    )

    logger.info("═══════════════════════════════════════════════")
    logger.info("  PIPELINE SUMMARY")
    logger.info("═══════════════════════════════════════════════")
    logger.info("  Flights loaded        : %s", f"{validation['total_rows']:,}")
    logger.info("  Date range            : %s → %s", validation["min_date"], validation["max_date"])
    logger.info("  Unique airlines       : %d", validation["unique_airlines"])
    logger.info("  Unique origin airports: %d", validation["unique_origins"])
    logger.info("  Cancellations         : %s (%.2f%%)", f"{validation['total_cancelled']:,}", cancellation_rate)
    logger.info("  Avg arrival delay     : %.1f min", validation["avg_arr_delay_minutes"] or 0)
    logger.info("  Airports ref loaded   : %d", airports_loaded or 0)
    logger.info("  Airlines ref loaded   : %d", airlines_loaded or 0)
    logger.info("  Load duration         : %.1fs", load_result["duration_seconds"] if load_result else 0)
    logger.info("═══════════════════════════════════════════════")

    if validation["total_rows"] == 0:
        raise ValueError("Pipeline validation failed: 0 flight rows loaded.")


# ─────────────────────────────────────────────────────────────
# Task 7 — Cleanup temp files
# ─────────────────────────────────────────────────────────────
def cleanup_temp_files(**context):
    """Remove ZIP and CSV temp files regardless of upstream task state."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.bts_flight_loader import BtsFlightLoader

    zip_path = context["ti"].xcom_pull(task_ids="download_flight_zip", key="zip_path")
    csv_path = context["ti"].xcom_pull(task_ids="extract_flight_csv", key="csv_path")

    loader = BtsFlightLoader(
        conn_string=_env("PIPELINE_CONN_STRING"),
        zip_url=_env("BTS_FLIGHT_URL"),
        local_zip=_env("BTS_LOCAL_ZIP"),
        local_csv=_env("BTS_LOCAL_CSV"),
    )
    loader.cleanup(zip_path, csv_path)


# ─────────────────────────────────────────────────────────────
# Wire up tasks
# ─────────────────────────────────────────────────────────────
with dag:
    t1_create = PythonOperator(
        task_id="create_schemas_and_tables",
        python_callable=create_schemas_and_tables,
    )

    t2_lookups = PythonOperator(
        task_id="load_lookup_tables",
        python_callable=load_lookup_tables,
    )

    t3_download = PythonOperator(
        task_id="download_flight_zip",
        python_callable=download_flight_zip,
    )

    t4_extract = PythonOperator(
        task_id="extract_flight_csv",
        python_callable=extract_flight_csv,
    )

    t5_load = PythonOperator(
        task_id="load_flights_to_staging",
        python_callable=load_flights_to_staging,
    )

    t6_validate = PythonOperator(
        task_id="validate_and_report",
        python_callable=validate_and_report,
    )

    t7_cleanup = PythonOperator(
        task_id="cleanup_temp_files",
        python_callable=cleanup_temp_files,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Dependencies
    t1_create >> t2_lookups
    t1_create >> t3_download >> t4_extract >> t5_load
    [t2_lookups, t5_load] >> t6_validate >> t7_cleanup
