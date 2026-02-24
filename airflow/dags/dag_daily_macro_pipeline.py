"""
dag_daily_macro_pipeline.py
----------------------------
Orchestrates the full daily macro data pipeline:
  1. Ingest market prices (yfinance) — runs every weekday at 6am ET
  2. Archive raw data to S3
  3. Run dbt staging + mart models
  4. Run dbt data quality tests
  5. Alert on Slack if any step fails or data is stale

Schedule: Mon-Fri at 06:00 UTC (after US market close prior day)
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


def check_data_freshness(**context):
    """
    Check if yesterday's market data landed.
    Returns 'run_dbt' if fresh, 'alert_stale_data' if stale.
    """
    hook = PostgresHook(postgres_conn_id="econ_postgres")
    sql = """
        SELECT COUNT(*) 
        FROM raw.market_prices 
        WHERE trade_date = CURRENT_DATE - INTERVAL '1 day'
          AND ticker = '^GSPC'
    """
    result = hook.get_first(sql)
    count = result[0] if result else 0

    if count > 0:
        return "run_dbt_staging"
    else:
        return "alert_stale_data"


# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="daily_macro_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily market data ingestion → dbt → quality checks",
    schedule_interval="0 6 * * 1-5",  # Mon-Fri 6am UTC
    catchup=False,
    max_active_runs=1,
    tags=["econ", "market-data", "daily"],
    on_failure_callback=slack_failure_callback,
) as dag:

    # ── Task 1: Ingest market prices ─────────────────────────────────────────
    ingest_market_data = BashOperator(
        task_id="ingest_market_data",
        bash_command=(
            f"cd {PIPELINE_DIR} && "
            "python ingestion/yfinance_pipeline.py "
            "--destination postgres "
            "--start $(date -d '7 days ago' +%Y-%m-%d)"  # last 7 days (handles weekends/holidays)
        ),
        env={"PYTHONPATH": PIPELINE_DIR},
    )

    # ── Task 2: Freshness check ──────────────────────────────────────────────
    freshness_check = BranchPythonOperator(
        task_id="freshness_check",
        python_callable=check_data_freshness,
    )

    # ── Task 3a: Alert if stale ──────────────────────────────────────────────
    alert_stale_data = SlackWebhookOperator(
        task_id="alert_stale_data",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            ":warning: *Stale Data Alert*\n"
            "No market data found for yesterday in `raw.market_prices`.\n"
            "Pipeline will continue but downstream models may be outdated."
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
        trigger_rule=TriggerRule.ALL_DONE,  # run even if tests fail
    )

    # ── Task 7: Success notification ─────────────────────────────────────────
    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            ":large_green_circle: *Daily Pipeline Complete*\n"
            "Market data ingested, dbt models refreshed, tests passed.\n"
            f"Run date: `{{{{ ds }}}}`"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Task Dependencies ────────────────────────────────────────────────────
    (
        ingest_market_data
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
