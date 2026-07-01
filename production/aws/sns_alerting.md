# Alerting — production (AWS)

The local stack alerts via an Airflow Slack webhook + `email_on_failure`
(see `local/q1_pipeline/airflow/dags/account_snapshot_dag.py`). In production the
same "alert the on-call engineer after 3 failed retries" requirement is met with
**SNS → PagerDuty/Slack**:

```
Spark step fails ──(3 retries exhausted)──▶ SNS topic
                                             semestabank-data-eng-alerts
                                                  ├─▶ PagerDuty (on-call page)
                                                  ├─▶ Slack  #data-eng-alerts
                                                  └─▶ email  data-oncall@semestabank.id
```

Wired from three places, pick per orchestrator:

| Orchestrator            | How the alert fires                                              |
|-------------------------|-----------------------------------------------------------------|
| Step Functions          | `AlertOnCall` state → `sns:publish` (see `step_functions.asl.json`) |
| MWAA / Airflow          | `SnsPublishOperator` task with `trigger_rule=ONE_FAILED` (see `mwaa_dag.py`) |
| Databricks Workflows    | `email_notifications.on_failure` + `webhook_notifications.on_failure` (see `databricks/workflow.json`) |

**SLA (30 min):** the snapshot step carries a hard timeout
(`TimeoutSeconds: 1800` in Step Functions, `timeout_seconds: 1800` +
`RUN_DURATION_SECONDS` health rule in Databricks, `sla=timedelta(minutes=30)` in
MWAA). A breach pages the same SNS topic so the credit-scoring team's 30-minute
consumption SLA is protected.

The SNS topic + subscription are provisioned in `terraform/mwaa.tf`.
