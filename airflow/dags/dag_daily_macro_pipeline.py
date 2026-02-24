"""
dag_daily_macro_pipeline.py
----------------------------
Orchestrates the daily macro data pipeline:
  1. Run dbt staging + mart models
  2. Run dbt data quality tests
  3. Alert on Slack if any step fails

Schedule: Mon-Fri at 06:00 UTC
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Default Args ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

PIPELINE_DIR = os.getenv("PIPELINE_DIR", "/app")
DBT_DIR = f"{PIPELINE_DIR}/dbt_project"
SLACK_CONN_ID = "slack_webhook"

# ── Callbacks ────────────────────────────────────────────────────────────────

def slack_failure_callback(context):
    """Send Slack alert on task failure."""
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    log_url = task_instance.log_url

    return SlackWebhookOperator(
        task_id="slack_failure_alert",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            f":red_circle: *Pipeline Failure*\n"
            f"DAG: `{dag_id}`\n"
            f"Task: `{task_id}`\n"
            f"Time: `{datetime.utcnow().isoformat()}`\n"
            f"<{log_url}|View Logs>"
        ),
    ).execute(context=context)


# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="daily_macro_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily dbt → quality checks",
    schedule_interval="0 6 * * 1-5",  # Mon-Fri 6am UTC
    catchup=False,
    max_active_runs=1,
    tags=["econ", "daily"],
    on_failure_callback=slack_failure_callback,
) as dag:

    # ── Task 1: Run dbt staging models ──────────────────────────────────────
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select staging --profiles-dir . --target dev"
        ),
    )

    # ── Task 2: Run dbt mart models ──────────────────────────────────────────
    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select marts --profiles-dir . --target dev"
        ),
    )

    # ── Task 3: Run dbt tests ────────────────────────────────────────────────
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt test --profiles-dir . --target dev"
        ),
    )

    # ── Task 4: Generate dbt docs ────────────────────────────────────────────
    generate_dbt_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt docs generate --profiles-dir . --target dev"
        ),
        trigger_rule=TriggerRule.ALL_DONE,  # run even if tests fail
    )

    # ── Task 5: Success notification ─────────────────────────────────────────
    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            ":large_green_circle: *Daily Pipeline Complete*\n"
            "dbt models refreshed, tests passed.\n"
            f"Run date: `{{{{ ds }}}}`"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Task Dependencies ────────────────────────────────────────────────────
    (
        run_dbt_staging
        >> run_dbt_marts
        >> run_dbt_tests
        >> generate_dbt_docs
        >> notify_success
    )
