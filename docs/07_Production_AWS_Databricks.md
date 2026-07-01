# 07 · Production Guide — AWS / Databricks (Condition ②)

This is the **company-grade** form of the same answers. The local stack proves the logic; this
shows how it runs on a regulated bank's cloud platform. **The PySpark and SQL are real and
faithful; the Terraform is an illustrative skeleton** (not `apply`-ed — no cloud account in this
exercise; each `.tf` says so in its header).

> Artifacts + the full local→production mapping table + deploy runbook:
> `[production/README.md](../production/README.md)`. Platform rationale: `[architecture/](../architecture/)`.

## The one idea: same logic, different edges

```
            ┌──────────────────────── identical ────────────────────────┐
 LOCAL ①    dedup → status filter → aggregate → join accounts → DQ gates → write
 PROD  ②    dedup → status filter → aggregate → join accounts → DQ gates → write
            └────────────────────────────────────────────────────────────┘
 only these change:  secrets · source · storage format · compute · orchestrator
```

## What runs where


| Concern                  | Production choice                                                                            | File                                                                                                                    |
| ------------------------ | -------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Q1 snapshot job          | Databricks job: `dbutils.secrets` + Oracle JDBC + Delta `replaceWhere` + `OPTIMIZE … ZORDER` | `[production/databricks/account_snapshot_job.py](../production/databricks/account_snapshot_job.py)`                     |
| Q2 gold views            | DLT materialized views (incremental, lineage-tracked); Snowflake/Postgres variants noted     | `[production/databricks/gold_views_dlt.sql](../production/databricks/gold_views_dlt.sql)`                               |
| Q1c orchestration        | Databricks Workflows — snapshot→credit-scoring, 3 retries, 30-min SLA, alerts                | `[production/databricks/workflow.json](../production/databricks/workflow.json)`                                         |
| AWS-native compute       | EMR Serverless (same `spark-submit` job)                                                     | `[production/aws/emr_serverless_job.json](../production/aws/emr_serverless_job.json)`                                   |
| AWS-native orchestration | Step Functions (retry 3 + SNS alert) / MWAA DAG                                              | `[step_functions.asl.json](../production/aws/step_functions.asl.json)` · `[mwaa_dag.py](../production/aws/mwaa_dag.py)` |
| Alerting + SLA           | SNS → PagerDuty/Slack/email                                                                  | `[production/aws/sns_alerting.md](../production/aws/sns_alerting.md)`                                                   |
| Infra                    | S3 (tiered) · MSK · Secrets Manager · Unity Catalog · MWAA                                   | `[production/terraform/](../production/terraform/)`                                                                     |


## Two production paths (pick one)

1. **Databricks-first (recommended; the Q3 design).** Jobs + DLT + Workflows + Unity Catalog, on
  AWS. One platform covers batch, streaming, SQL, ML, and governance — and column-level lineage
   is automatic (the OJK requirement). This is what `[architecture/](../architecture/)` argues for.
2. **AWS-native.** EMR Serverless for Spark + Step Functions/MWAA for orchestration + Glue Catalog
  - Lake Formation. Cheaper at the margins, but you build lineage yourself and lose the unified
   ML story — see the trade-off in
   [PLATFORM_DECISIONS.md](../architecture/PLATFORM_DECISIONS.md) (decisions 2, 6, 8).

Both reuse the **same job code**; that's the point of keeping the logic edge-independent.

## Deploy runbook (summary)

1. `terraform apply` — S3 (lifecycle-tiered) + MSK + Secrets Manager + KMS + Unity Catalog
  catalog/schemas + MWAA + SNS.
2. Put Oracle ETL creds in Secrets Manager (out-of-band; never in code).
3. Upload job code to `s3://semestabank-artifacts/jobs/` and the Oracle JDBC jar to `…/jars/`.
4. Register bronze/silver/gold + the Oracle foreign catalog in Unity Catalog.
5. Create the DLT pipeline from `gold_views_dlt.sql`.
6. Create orchestration: `databricks jobs create --json @workflow.json` (or deploy the Step
  Functions / MWAA variant).
7. Smoke-run one `snapshot_date`; confirm credit scoring is triggered and the 30-min SLA holds.

Full version with the mapping table: `[production/README.md](../production/README.md)`.

## Cost

~**$39K of the $50K/month** budget, $11K headroom — itemized in
[BUDGET_BREAKDOWN.md](../architecture/BUDGET_BREAKDOWN.md), with ramp phases and budget alarms.