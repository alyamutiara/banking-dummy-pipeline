# =====================================================================
# SemestaBank — Q1(c) orchestration — MWAA (Managed Airflow) variant
# ---------------------------------------------------------------------
# The LOCAL DAG (Final_Answer/local/q1_pipeline/airflow/dags/account_snapshot_dag.py)
# uses DockerOperator to run Spark in sibling containers. On AWS MWAA there is
# no Docker socket — instead each step is an EMR Serverless job. The DAG SHAPE,
# retry policy (3x), SLA (30 min), success-trigger and failure-alert are
# identical; only the operators change.
#
# Deploy: upload this file to s3://<mwaa-bucket>/dags/ . MWAA picks it up.
# Secrets come from AWS Secrets Manager via the Airflow Secrets Backend
# (no credentials in code) — the production answer to Q1 problem #1.
# =====================================================================
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

APP_ID = "{{ var.value.emr_serverless_app_id }}"
EXEC_ROLE = "{{ var.value.emr_exec_role_arn }}"
ARTIFACTS = "s3://semestabank-artifacts/jobs"
ALERT_TOPIC = "{{ var.value.sns_alert_topic_arn }}"

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


def _spark(job_file: str, args: list[str] | None = None) -> dict:
    sub = f"--conf spark.jars={ARTIFACTS}/../jars/ojdbc11.jar"
    return {
        "applicationId": APP_ID,
        "executionRoleArn": EXEC_ROLE,
        "jobDriver": {"sparkSubmit": {
            "entryPoint": f"{ARTIFACTS}/{job_file}",
            "entryPointArguments": args or [],
            "sparkSubmitParameters": sub,
        }},
    }


with DAG(
    dag_id="semestabank_lakehouse_pipeline",
    description="MWAA: bronze -> silver snapshot -> gold -> trigger credit scoring",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["semestabank", "lakehouse", "q1"],
) as dag:

    bronze = EmrServerlessStartJobOperator(
        task_id="bronze_ingest", **_spark("bronze_ingest.py"))

    snapshot = EmrServerlessStartJobOperator(
        task_id="account_snapshot",
        sla=timedelta(minutes=30),                       # 30-min SLA (Q1c)
        **_spark("account_snapshot.py", ["--date", "{{ ds }}"]))

    gold = EmrServerlessStartJobOperator(
        task_id="gold_views", **_spark("gold_scorecard.py"))

    trigger_credit_scoring = TriggerDagRunOperator(
        task_id="trigger_credit_scoring",
        trigger_dag_id="credit_scoring_pipeline",
        conf={"snapshot_date": "{{ ds }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    alert = SnsPublishOperator(
        task_id="alert_engineer_on_call",
        target_arn=ALERT_TOPIC,
        subject="[SemestaBank ALERT] Lakehouse pipeline FAILED after 3 retries",
        message="Pipeline failed for {{ ds }} after all retries — investigate.",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    bronze >> snapshot >> gold >> trigger_credit_scoring
    [bronze, snapshot, gold] >> alert
