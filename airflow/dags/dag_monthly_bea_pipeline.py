"""
dag_monthly_bea_pipeline.py
----------------------------
Orchestrates the monthly BEA NIPA data refresh:
  1. Ingest BEA NIPA tables (GDP, PCE, personal income, trade, profits)
  2. Archive raw JSON to S3
  3. Write/merge into Delta Lake (handles BEA revisions via MERGE)
  4. Run dbt staging + mart models with --full-refresh (BEA revises past quarters)
  5. Run dbt data quality tests
  6. Notify Slack on success

Schedule: 3rd of each month at 08:00 UTC
  BEA releases advanced GDP estimates ~4 weeks after quarter-end (typically the last
  week of the following month). PCE and personal income are released monthly.
  Running on the 3rd ensures we pick up prior-month PCE/income releases.
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
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
    "retry_exponential_backoff": True,
}

PIPELINE_DIR = os.getenv("PIPELINE_DIR", "/app")
DBT_DIR = f"{PIPELINE_DIR}/dbt_project"
DELTA_DIR = f"{PIPELINE_DIR}/delta_lake"
SLACK_CONN_ID = "slack_webhook"

# BEA releases revise data going back several years; pull the current and prior year
CURRENT_YEAR = datetime.utcnow().year
BEA_START_YEAR = CURRENT_YEAR - 2  # capture revisions from 2 years back


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
            f":red_circle: *Monthly BEA Pipeline Failure*\n"
            f"DAG: `{dag_id}`\n"
            f"Task: `{task_id}`\n"
            f"Time: `{datetime.utcnow().isoformat()}`\n"
            f"<{log_url}|View Logs>"
        ),
    ).execute(context=context)


def check_bea_data_freshness(**context):
    """
    Verify BEA data arrived after the ingestion task.
    Branches to run_delta_merge if fresh, alert_stale_data otherwise.
    """
    hook = PostgresHook(postgres_conn_id="econ_postgres")
    sql = """
        SELECT COUNT(DISTINCT table_key)
        FROM raw.bea_nipa_observations
        WHERE ingested_at >= NOW() - INTERVAL '2 hours'
    """
    result = hook.get_first(sql)
    tables_refreshed = result[0] if result else 0

    # Expect all 7 NIPA tables to have refreshed
    if tables_refreshed >= 5:
        return "run_delta_merge"
    else:
        return "alert_stale_data"


# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="monthly_bea_pipeline",
    default_args=DEFAULT_ARGS,
    description="Monthly BEA NIPA ingest → Delta Lake merge → dbt full-refresh → tests",
    schedule_interval="0 8 3 * *",  # 3rd of each month at 08:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["econ", "bea", "monthly"],
    on_failure_callback=slack_failure_callback,
) as dag:

    # ── Task 1: Ingest BEA NIPA tables ──────────────────────────────────────
    # Fetch the last 2+ years to capture quarterly revisions
    ingest_bea = BashOperator(
        task_id="ingest_bea_nipa",
        bash_command=(
            f"cd {PIPELINE_DIR} && "
            "python ingestion/bea_pipeline.py "
            "--destination postgres "
            f"--start {BEA_START_YEAR}"
        ),
        env={"PYTHONPATH": PIPELINE_DIR},
    )

    # ── Task 2: Freshness check ──────────────────────────────────────────────
    freshness_check = BranchPythonOperator(
        task_id="freshness_check",
        python_callable=check_bea_data_freshness,
    )

    # ── Task 3a: Alert if stale ──────────────────────────────────────────────
    alert_stale_data = SlackWebhookOperator(
        task_id="alert_stale_data",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            ":warning: *Stale BEA Data Alert*\n"
            "Fewer than 5 BEA NIPA tables updated in the last 2 hours.\n"
            "The BEA API may be unavailable. Check https://apps.bea.gov/api/\n"
            "Pipeline will continue — downstream models may use prior-month data.\n"
            f"Run date: `{{{{ ds }}}}`"
        ),
    )

    # ── Task 3b: Merge to Delta Lake ─────────────────────────────────────────
    # Delta Lake MERGE handles BEA revisions to past periods without full reloads.
    # Uses MERGE ON (table_key, series_name, period_date) to upsert revised values.
    run_delta_merge = BashOperator(
        task_id="run_delta_merge",
        bash_command=(
            f"cd {PIPELINE_DIR} && "
            "python delta_lake/delta_writer.py "
            "--table bea_nipa_observations --mode merge"
        ),
        trigger_rule=TriggerRule.NONE_FAILED,
        env={"PYTHONPATH": PIPELINE_DIR},
    )

    # ── Task 4: dbt staging models ───────────────────────────────────────────
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select staging --profiles-dir . --target dev"
        ),
    )

    # ── Task 5: dbt mart models (full-refresh) ───────────────────────────────
    # BEA revises past quarters, so the mart needs a full rebuild to pick up changes.
    # --full-refresh drops and recreates the mart table rather than incremental append.
    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts_full_refresh",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select marts --profiles-dir . --target dev --full-refresh"
        ),
    )

    # ── Task 6: dbt tests ────────────────────────────────────────────────────
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt test --profiles-dir . --target dev"
        ),
    )

    # ── Task 7: Generate dbt docs ────────────────────────────────────────────
    generate_dbt_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt docs generate --profiles-dir . --target dev"
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Task 8: Success notification ─────────────────────────────────────────
    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=(
            ":large_green_circle: *Monthly BEA Pipeline Complete*\n"
            "BEA NIPA data ingested, Delta Lake merged, "
            "dbt full-refresh complete, all tests passed.\n"
            f"Run date: `{{{{ ds }}}}`"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Task Dependencies ────────────────────────────────────────────────────
    (
        ingest_bea
        >> freshness_check
        >> [alert_stale_data, run_delta_merge]
    )

    alert_stale_data >> run_delta_merge

    (
        run_delta_merge
        >> run_dbt_staging
        >> run_dbt_marts
        >> run_dbt_tests
        >> generate_dbt_docs
        >> notify_success
    )
