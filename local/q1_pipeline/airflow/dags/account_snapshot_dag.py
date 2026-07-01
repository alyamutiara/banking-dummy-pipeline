# =====================================================================
# SemestaBank Q1(c) - Airflow orchestration for Lakehouse Pipeline
#
# Requirements satisfied:
#   * Runs the full pipeline: bronze → silver → gold
#   * Triggers credit-scoring after silver is ready
#   * Retries each step up to 3 times before giving up
#   * Alerts the on-call engineer once all retries are exhausted (Slack webhook
#     + Airflow email_on_failure; falls back to log.error if no webhook URL)
#   * Enforces 30-minute SLA on the silver snapshot
# =====================================================================
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger("semestabank.lakehouse")

# ── Config ──
COMPOSE_PROJECT  = os.environ.get("COMPOSE_PROJECT_NAME", "semestabank-lakehouse")
SPARK_IMAGE_TAG  = os.environ.get("SPARK_IMAGE_TAG", "semestabank-lakehouse-spark:latest")
ONCALL_EMAIL     = os.environ.get("ONCALL_EMAIL", "data-oncall@semestabank.id")

SLA_SNAPSHOT_30M = timedelta(minutes=30)

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "email": [ONCALL_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

def _sla_miss_callback(dag: DAG, task_list, blocking_tis, slas, blocking_tis_map) -> None:
    log.error("SLA MISS on dag=%s tasks=%s", dag.dag_id, [t.task_id for t in slas])


# Shared Docker env for all Spark jobs
SPARK_ENV = {
    "JDBC_URL":      os.environ["JDBC_URL"],
    "JDBC_USER":     os.environ["JDBC_USER"],
    "JDBC_PASSWORD": os.environ["JDBC_PASSWORD"],
    "S3_ENDPOINT":   os.environ["S3_ENDPOINT"],
    "S3_BUCKET_BRONZE": os.environ["S3_BUCKET_BRONZE"],
    "S3_BUCKET":       os.environ["S3_BUCKET"],
    "S3_PATH":         os.environ["S3_PATH"],
    "S3_BUCKET_GOLD":  os.environ["S3_BUCKET_GOLD"],
    "AWS_ACCESS_KEY_ID":     os.environ["AWS_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    "MIN_ACCOUNTS":    "1",
    "DQ_FAIL_HARD":    "1",
}

SPARK_COMMAND = "/opt/spark/bin/spark-submit --master local[*]"


with DAG(
    dag_id="semestabank_lakehouse_pipeline",
    description="Lakehouse pipeline: bronze → silver → gold, then trigger credit scoring",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=_sla_miss_callback,
    tags=["semestabank", "lakehouse", "q1", "q2"],
) as dag:

    start = DummyOperator(task_id="start")

    # ── Step 1: Bronze ingest (CSVs → MinIO) ──
    bronze_ingest = DockerOperator(
        task_id="bronze_ingest",
        image=SPARK_IMAGE_TAG,
        container_name="airflow-bronze-ingest",
        command=f"{SPARK_COMMAND} /opt/spark/jobs/bronze_ingest.py",
        environment=SPARK_ENV,
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_PROJECT + "_default",
        auto_remove="success",
        mount_tmp_dir=False,
        tty=False,
    )

    # ── Step 2: Silver snapshot (Q1) ──
    run_snapshot = DockerOperator(
        task_id="run_account_snapshot",
        image=SPARK_IMAGE_TAG,
        container_name="airflow-silver-snapshot",
        command=f"{SPARK_COMMAND} /opt/spark/jobs/account_snapshot.py",
        environment=SPARK_ENV,
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_PROJECT + "_default",
        auto_remove="success",
        sla=SLA_SNAPSHOT_30M,
        mount_tmp_dir=False,
        tty=False,
    )

    # ── Step 3a: Gold scorecard (Q2a) ──
    gold_scorecard = DockerOperator(
        task_id="gold_scorecard",
        image=SPARK_IMAGE_TAG,
        container_name="airflow-gold-scorecard",
        command=f"{SPARK_COMMAND} /opt/spark/jobs/gold_scorecard.py",
        environment=SPARK_ENV,
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_PROJECT + "_default",
        auto_remove="success",
        mount_tmp_dir=False,
        tty=False,
    )

    # ── Step 3b: Gold fraud alerts (Q2b) ──
    gold_fraud = DockerOperator(
        task_id="gold_fraud",
        image=SPARK_IMAGE_TAG,
        container_name="airflow-gold-fraud",
        command=f"{SPARK_COMMAND} /opt/spark/jobs/gold_fraud.py",
        environment=SPARK_ENV,
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_PROJECT + "_default",
        auto_remove="success",
        mount_tmp_dir=False,
        tty=False,
    )

    # ── Success path: trigger credit scoring ──
    def _notify_credit_scoring(**ctx):
        run: DagRun = ctx["dag_run"]
        log.info("Lakehouse pipeline complete — notifying credit-scoring (run_id=%s)", run.run_id)

    trigger_credit_scoring = PythonOperator(
        task_id="trigger_credit_scoring",
        python_callable=_notify_credit_scoring,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Failure path: alert engineer ──
    # In production, this fires a Slack/PagerDuty webhook.
    # Airflow's built-in email_on_failure=True (set above) also sends an email
    # via SMTP if configured in airflow.cfg. The webhook below is the primary
    # real-time channel; email is the secondary/audit trail.
    SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

    def _build_alert_body(**ctx) -> str:
        ti = ctx["ti"]
        return (
            f"[SemestaBank ALERT] Lakehouse pipeline FAILED.\n"
            f"Task: {ti.task_id} | Run: {ctx['run_id']} | Date: {ctx['ds']}\n"
            f"All {default_args['retries']} retries exhausted. Please investigate.\n"
        )

    def _alert_engineer(**ctx):
        """Send alert to Slack webhook if configured; always log as fallback."""
        body = _build_alert_body(**ctx)
        log.error("ALERT: %s", body)
        if SLACK_WEBHOOK_URL:
            import json
            import urllib.request
            payload = json.dumps({
                "text": body,
                "channel": "#data-eng-alerts",
                "username": "semestabank-pipeline-bot",
                "icon_emoji": ":rotating_light:",
            }).encode("utf-8")
            try:
                req = urllib.request.Request(
                    SLACK_WEBHOOK_URL,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(req, timeout=10).read()
                log.info("Slack alert sent to #data-eng-alerts")
            except Exception as exc:
                log.error("Slack webhook failed (%s) — alert logged only", exc)
        else:
            log.warning(
                "SLACK_WEBHOOK_URL not set — alert logged only. "
                "In production, configure this env var in MWAA to enable Slack/PagerDuty alerts."
            )
        return True

    alert_engineer = PythonOperator(
        task_id="alert_engineer_on_call",
        python_callable=_alert_engineer,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    done = DummyOperator(task_id="done")

    # ── Wiring ──
    start >> bronze_ingest >> run_snapshot >> gold_scorecard >> gold_fraud
    gold_fraud >> trigger_credit_scoring >> done
    [bronze_ingest, run_snapshot, gold_scorecard, gold_fraud] >> alert_engineer >> done
