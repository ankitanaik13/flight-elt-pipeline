"""
DAG: flight_dbt_transformations

Runs the full dbt pipeline in sequence:
debug → deps → seed → staging → intermediate → marts → test → docs.

Triggered manually after flight_data_ingestion succeeds.
All tasks use BashOperator with env vars from the container environment.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "depends_on_past": False,
}

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt_project")

DBT_BASE = (
    f"dbt {{cmd}} "
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profiles-dir {DBT_PROFILES_DIR}"
)

dag = DAG(
    dag_id="flight_dbt_transformations",
    description=(
        "Runs dbt models transforming raw flight staging data "
        "into analytical marts."
    ),
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "transformation", "flights"],
    default_args=DEFAULT_ARGS,
)

with dag:
    t1_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=DBT_BASE.format(cmd="debug") + " || true",
    )

    t2_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=DBT_BASE.format(cmd="deps"),
    )

    t3_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=DBT_BASE.format(cmd="seed"),
    )

    t4_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"dbt run --select staging "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    t5_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=(
            f"dbt run --select intermediate "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    t6_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"dbt run --select marts "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    t7_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_BASE.format(cmd="test") + " || true",
    )

    t8_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"dbt docs generate "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    (
        t1_debug
        >> t2_deps
        >> t3_seed
        >> t4_staging
        >> t5_intermediate
        >> t6_marts
        >> t7_test
        >> t8_docs
    )
