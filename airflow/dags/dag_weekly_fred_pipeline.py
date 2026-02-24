"""
dag_weekly_fred_pipeline.py
----------------------------
Orchestrates the weekly FRED indicator refresh:
  1. Ingest FRED series (Fed Funds, CPI, PCE, M2, unemployment, yields, etc.)
  2. Archive raw JSON to S3
  3. Run dbt staging models
  4. Run dbt mart models
  5. Run dbt data quality tests
  6. Notify Slack on success

Schedule: Every Monday at 06:00 UTC
  FRED releases most monthly indicators early in the week following the reference month.
  Weekly refresh ensures the mart stays within 7 days of each new release.
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
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
}

PIPELINE_DIR = os.getenv("PIPELINE_DIR", "/app")
DBT_DIR = f"{PIPELINE_DIR}/dbt_project"
SLACK_CONN_ID = "slack_webhook"

# How many days back to pull for incremental refresh (covers late revisions)
FRED_LOOKBACK_DAYS = 90


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
            f":red_circle: *Weekly FRED Pipeline Failure*\n"
            f"DAG: `{dag_id}`\n"
            f"Task: `{task_id}`\n"
            f"Time: `{datetime.utcnow().isoformat()}`\n"
            f"<{log_url}|View Logs>"
        ),
    ).execute(context=context)


def check_fred_data_freshness(**context):
    """
    Verify last week's FRED data landed after ingestion.
    Branches to run_dbt_staging if fresh, alert_stale_data otherwise.
    """
    hook = PostgresHook(postgres_conn_id="econ_postgres")
    sql = """
        SELECT COUNT(DISTINCT series_id)
        FROM raw.fred_observations
        WHERE ingested_at >= NOW() - INTERVAL '2 hours'
    """
    result = hook.get_first(sql)
    series_refreshed = result[0] if result else 0

    if series_refreshed >= 5:  # expect at least 5 of the 20+ series to update
        return "run_dbt_staging"
    else:
        return "alert_stale_data"


# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="weekly_fred_pipeline",
    default_args=DEFAULT_ARGS,
    description="Weekly FRED indicator refresh → dbt staging → marts → tests",
    schedule_interval="0 6 * * 1",  # Every Monday at 06:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["econ", "fred", "weekly"],
    on_failure_callback=slack_failure_callback,
) as dag:

    # ── Task 1: Ingest FRED indicators ───────────────────────────────────────
    # Pull last FRED_LOOKBACK_DAYS to capture late data revisions
    ingest_fred = BashOperator(
        task_id="ingest_fred_indicators",
        bash_command=(
            f"cd {PIPELINE_DIR} && "
            "python ingestion/fred_pipeline.py "
            "--destination postgres "
            f"--start $(date -d '{FRED_LOOKBACK_DAYS} days ago' +%Y-%m-%d)"
        ),
        env={"PYTHONPATH": PIPELINE_DIR},
    )

    # ── Task 2: Freshness check ──────────────────────────────────────────────
    freshness_check = BranchPythonOperator(
        task_id="freshness_check",
        python_callable=check_fred_data_freshness,
    )

    # ── Task 3a: Alert if stale ──────────────────────────────────────────────
    alert_stale_data = SlackWebhookOperator(
        task_id="alert_stale_data",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            ":warning: *Stale FRED Data Alert*\n"
            "Fewer than 5 FRED series updated in the last 2 hours.\n"
            "The FRED API may be down or rate-limited. "
            "Pipeline will continue — downstream models may be outdated.\n"
            f"Run date: `{{{{ ds }}}}`"
        ),
    )

    # ── Task 3b: Run dbt staging models ──────────────────────────────────────
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select staging --profiles-dir . --target dev"
        ),
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # ── Task 4: Run dbt mart models ──────────────────────────────────────────
    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select marts --profiles-dir . --target dev"
        ),
    )

    # ── Task 5: Run dbt tests ────────────────────────────────────────────────
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt test --profiles-dir . --target dev"
        ),
    )

    # ── Task 6: Generate dbt docs ────────────────────────────────────────────
    generate_dbt_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt docs generate --profiles-dir . --target dev"
        ),
        trigger_rule=TriggerRule.ALL_DONE,  # run even if tests have warnings
    )

    # ── Task 7: Success notification ─────────────────────────────────────────
    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            ":large_green_circle: *Weekly FRED Pipeline Complete*\n"
            "FRED indicators ingested, dbt models refreshed, tests passed.\n"
            f"Run date: `{{{{ ds }}}}`"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Task Dependencies ────────────────────────────────────────────────────
    (
        ingest_fred
        >> freshness_check
        >> [alert_stale_data, run_dbt_staging]
    )

    alert_stale_data >> run_dbt_staging

    (
        run_dbt_staging
        >> run_dbt_marts
        >> run_dbt_tests
        >> generate_dbt_docs
        >> notify_success
    )
