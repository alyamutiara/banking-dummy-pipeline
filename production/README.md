# Production Condition — AWS / Databricks (company-grade)

This folder is **Condition 2**: how the exact same answers run on a production
cloud platform instead of a laptop. The **business logic is unchanged** from
`../local/` — only the *edges* (storage, secrets, compute, orchestration) swap
to managed services. These are **runnable, faithful stubs**, not a live account
setup: the PySpark/SQL is real; the IaC is an illustrative skeleton (see the
header note in each `terraform/*.tf`).

> Pair this folder with the prose walkthrough in
> [`../docs/07_Production_AWS_Databricks.md`](../docs/07_Production_AWS_Databricks.md)
> and the platform design in [`../architecture/`](../architecture/).

## What's here

```
production/
├── databricks/
│   ├── account_snapshot_job.py   # Q1 job: dbutils.secrets + Oracle JDBC + Delta (replaceWhere) + ZORDER
│   ├── gold_views_dlt.sql        # Q2 views as DLT materialized views (+ Snowflake / Postgres variants)
│   └── workflow.json             # Q1c orchestration: snapshot→credit-scoring, 3 retries, 30-min SLA, alerts
├── aws/
│   ├── emr_serverless_job.json   # run the SAME PySpark on EMR Serverless
│   ├── step_functions.asl.json   # AWS-native orchestration + SNS alert after 3 retries
│   ├── mwaa_dag.py               # Managed Airflow variant of the local DAG (EMR Serverless operators)
│   └── sns_alerting.md           # alerting + SLA wiring (PagerDuty/Slack/email)
└── terraform/                    # ILLUSTRATIVE skeleton: s3, msk, secrets, databricks(UC), mwaa
```

## Local → Production mapping

| Concern            | Local (`../local/`)                     | Production (here)                                   |
|--------------------|-----------------------------------------|-----------------------------------------------------|
| Source DB          | PostgreSQL (Oracle stand-in)            | Oracle core banking (JDBC) / Lakehouse Federation   |
| Secrets            | `.env` file                             | AWS Secrets Manager → `dbutils.secrets.get(...)`    |
| Object storage     | MinIO (`s3a://`)                        | S3 + **Delta Lake** (`s3://`) under Unity Catalog   |
| Table format       | Parquet (partitioned)                   | Delta (ACID, time travel, `replaceWhere`, Z-ORDER)  |
| Compute            | Spark `local[*]` in Docker              | Databricks Jobs **or** EMR Serverless               |
| Idempotency        | dynamic partition overwrite             | Delta `replaceWhere = "snapshot_date = …"`          |
| Q2 materialization | Postgres matview + `REFRESH CONCURRENTLY` | DLT materialized view / Snowflake dynamic table   |
| Orchestration      | Airflow + DockerOperator                | Databricks Workflows / Step Functions / MWAA        |
| Retries / SLA      | `retries=3`, `sla=30m`                  | `max_retries:3` + `timeout_seconds:1800` (all three)|
| Alerting           | Slack webhook + email                   | SNS → PagerDuty/Slack/email                          |
| Lineage / catalog  | (n/a locally)                           | Unity Catalog column-level lineage + OpenLineage    |
| Cost               | free (laptop)                           | ~$39K/month of the $50K budget (see `../architecture/BUDGET_BREAKDOWN.md`) |

## Deploy order (runbook)

1. `terraform init && terraform apply` — S3 (tiered) + MSK + Secrets Manager + KMS + Unity Catalog catalog/schemas + MWAA + SNS.
2. Put the Oracle ETL credentials into Secrets Manager (out-of-band, never in code).
3. Upload job code to `s3://semestabank-artifacts/jobs/` (and the Oracle JDBC jar to `…/jars/`).
4. Register bronze/silver/gold tables + the Oracle foreign catalog in Unity Catalog.
5. Create the DLT pipeline from `databricks/gold_views_dlt.sql`.
6. Create the orchestration job: `databricks jobs create --json @databricks/workflow.json` (or deploy `aws/step_functions.asl.json`, or drop `aws/mwaa_dag.py` into the MWAA DAGs bucket).
7. Smoke-run for one `snapshot_date`, confirm the credit-scoring job is triggered and the 30-min SLA holds.

## Why this platform (short version)

One **Databricks lakehouse on AWS** covers batch + streaming + SQL + ML + governance
in a single Premium price, instead of stitching Snowflake + a separate streaming
engine + a separate lineage tool. Full justification and the 11 component
trade-offs are in [`../architecture/PLATFORM_DECISIONS.md`](../architecture/PLATFORM_DECISIONS.md).
