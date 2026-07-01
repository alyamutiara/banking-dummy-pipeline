# Platform Decisions — Component-by-Component Justification

## Decision 1 — Cloud: AWS (over GCP / Azure / on-prem)


| Factor                           | AWS                                                         | GCP                            | Azure                                        | On-pre                   |
| -------------------------------- | ----------------------------------------------------------- | ------------------------------ | -------------------------------------------- | ------------------------ |
| Databricks availability          | ✓ (ap-southeast-1)                                          | ✓ (asia-southeast1)            | ✓ (sea)                                      | n/a                      |
| Proximity to Indonesia           | Singapore region (12ms)                                     | Singapore region (15ms)        | Southeast Asia (20ms)                        | would require Jakarta DC |
| MSK (managed Kafka)              | ✓ native                                                    | Confluent Cloud (extra vendor) | Event Hubs (Kafka-compatible but not native) | self-managed             |
| Existing SemestaBank relationship | most likely (regional banks in ID typically start with AWS) | less common                    | rare                                         | high capex               |
| Talent pool in Indonesia         | largest                                                     | growing                        | small                                        | n/a                      |


**Verdict: AWS.** Singapore region gives 12ms latency to Jakarta. MSK is native. Databricks
runs identically on all three, so AWS is chosen for **proximity + Kafka + talent**, not for
lock-in.

## Decision 2 — Lakehouse engine: Databricks (over Snowflake / EMR / Glue)


| Criteria                           | Databricks                                      | Snowflake                                   | AWS EMR+Redshift                      | Glue+Redshift+LakeFormation      |
| ---------------------------------- | ----------------------------------------------- | ------------------------------------------- | ------------------------------------- | -------------------------------- |
| Batch + streaming in one engine    | ✓ Structured Streaming                          | ✗ (Snowpipe only, no true streaming)        | ✓ Spark + Kafka                       | ✓ Glue Streaming + Kafka         |
| Column-level lineage               | ✓ Unity Catalog (free, automatic)               | ✗ (needs Collibra, $4-8K/mo)                | ✗ (Lake Formation = table-level only) | ✗ (Lake Formation = table-level) |
| ML platform integrated             | ✓ MLflow + ML Runtime                           | ✗ (needs SageMaker separately)              | ✗ (needs SageMaker)                   | ✗                                |
| Cost for 60M-row scans (Q2)        | Jobs $0.40/DBU                                  | $3-5/credit per query                       | same as Databricks (it's Spark)       | cheap Glue but no SQL warehouse  |
| Medallion native                   | ✓ Delta + DLT pipelines                         | △ (can do it but Delta is not native)       | △                                     | △                                |
| Already used by SemestaBank         | ✓ scenario says credit scoring is on Databricks | ✗                                           | ✗                                     | ✗                                |
| Time to proficiency (mid-level DE) | low (PySpark + SQL)                             | low (SQL only)                              | medium                                | medium                           |
| Vendor lock-in                     | medium (Delta is open-source; Spark is open)    | high (Snowflake SQL is proprietary dialect) | medium                                | low                              |


**Verdict: Databricks.** Reasoning chain:

1. Scenario already commits to Databricks for credit scoring ML → don't add a second
  platform (would double governance cost + break lineage).
2. Unity Catalog gives column-level lineage for free → meets the OJK directive.
3. Structured Streaming handles the 50K/sec clickstream in the same engine as Q1's batch.
4. Delta is open-source (linuxfoundation.org/delta) → not locked into Databricks. A future
  migration to plain EMR + Iceberg is possible if needed.

## Decision 3 — Table format: Delta Lake (over Iceberg / Hudi / plain Parquet)


| Format         | ACID | Time travel | Z-ORDER                  | Lineage native                 | Open source |
| -------------- | ---- | ----------- | ------------------------ | ------------------------------ | ----------- |
| Delta Lake     | ✓    | ✓           | ✓ (Databricks)           | via Unity Catalog              | ✓           |
| Apache Iceberg | ✓    | ✓           | ✗ (sort order different) | requires Glue/Atlas separately | ✓           |
| Apache Hudi    | ✓    | ✓           | ✗ (CoW/MoR only)         | requires Glue/Atlas separately | ✓           |
| Plain Parquet  | ✗    | ✗           | ✗                        | n/a                            | ✓           |


**Verdict: Delta Lake.** Justifications:

- ACID transactions allow `MERGE INTO` for the clickstream upserts (dedup by event_id).
- Time travel lets the OJK auditor query `SELECT * FROM events VERSION AS OF 123` for 7
years back (we set `logRetention=730d`). No competing format matches this on Databricks.
- Z-ORDER on `customer_id` is the magic that makes the batch-stream join fast. Iceberg has
sort order but not the optimizer integration that Databricks SQL/Z-ORDER has today.
- Delta is governed by the Linux Foundation. Not Databricks-proprietary.

**If SemestaBank later moves off Databricks:** Iceberg is the natural port target. The Spark
code in Q1 (which uses DataFrame API, not Delta-specific APIs) runs unchanged on Iceberg.
Only the table registration changes.

## Decision 4 — Streaming ingestion: MSK (over Kinesis / Kafka on EC2 / Firehose)


| Option                    | Throughput       | Price                    | Re-processing              | Lock-in                     |
| ------------------------- | ---------------- | ------------------------ | -------------------------- | --------------------------- |
| AWS MSK                   | ✓ 50K/sec easily | $4K/mo                   | ✓ 7-day retention + replay | AWS-only API but Kafka wire |
| Kinesis Data Streams      | ✓                | $4-6K/mo (shard pricing) | ✓ 365-day retention        | AWS-only SDK                |
| Self-managed Kafka on EC2 | ✓                | $3K/mo + ops time        | ✓                          | open source                 |
| Kinesis Firehose → S3     | ✓                | $2K/mo                   | ✗ no replay capability     | AWS-only                    |
| Confluent Cloud           | ✓                | ~$5K/mo + multiplier     | ✓                          | vendor                      |


**Verdict: MSK.** Reasoning:

- 50K/sec sustained, with 7-day retention for reprocessing.
- Same Kafka wire protocol as our Databricks Structured Streaming consumer.
- Less ops burden than self-managed Kafka (broker replacement, rebalancing are managed).
- Cheaper than Kinesis for the same throughput (no per-shard cost).
- Avoids Kinesis SDK lock-in — MSK speaks Kafka.

## Decision 5 — Batch CDC: AWS DMS (over Debezium / Fivetran / Spark JDBC)


| Issue                 | DMS                      | Debezium + Kafka Connect | Fivetran                 | Spark JDBC (Q1 approach)          |
| --------------------- | ------------------------ | ------------------------ | ------------------------ | --------------------------------- |
| Source load on Oracle | low (reads redo logs)    | low (reads redo logs)    | medium (periodic SELECT) | high (full predicate scans daily) |
| Cost                  | $200-500/mo              | $0 SW + $500/mo EC2      | $5K/mo                   | $0 if running Q1 (but we are)     |
| Extra infrastructure  | one replication instance | Kafka Connect cluster    | zero                     | zero                              |
| Real-time bronze      | ✓ near-real-time         | ✓ real-time              | ✗ batch only             | ✗ batch only                      |
| Setup complexity      | low                      | medium                   | low                      | n/a — already done                |


**Verdict: DMS for bronze CDC + Spark JDBC for Q1 silver snapshot.** Hybrid approach:

- **DMS** writes Oracle changes into `bronze.accounts_raw`, `bronze.transactions_raw` in
S3 every 1 hour. This populates the bronze layer continuously without stressing Oracle.
- **Q1's `account_snapshot.py`** continues to read Oracle directly via JDBC for the daily
silver snapshot (predicate pushdown on `txn_date`). This is best for daily-metric joins
because it avoids bronze-vs-Oracle consistency drift.
- Debezium is fine but requires standing up a Kafka Connect cluster. DMS is one-click and
costs $200/mo. Not worth the extra cluster for SemestaBank's size.
- Fivetran is ruled out on cost (10% of budget for one source).

## Decision 6 — Orchestration: MWAA (over Databricks Workflows / Step Functions / Prefect)


| Consideration             | MWAA                    | Databricks Workflows                       | Step Functions | Prefect / Dagster       |
| ------------------------- | ----------------------- | ------------------------------------------ | -------------- | ----------------------- |
| Q1's DAG runs unchanged   | ✓                       | ✗ (need to translate to Databricks tasks)  | ✗ (JSON only)  | ✗ (different DSL)       |
| Portability across clouds | ✓ (Airflow open source) | ✗ Databricks-only                          | ✗ AWS-only     | ✓                       |
| Audit log meets OJK       | ✓ (CloudWatch)          | ✓ (Databricks audit)                       | ✓              | ✓ (if configured)       |
| Ecosystem maturity        | very mature             | new                                        | mature         | young, growing          |
| Cost                      | $1K/mo                  | $0 if using Databricks for everything else | $0.1K/mo       | $0 OS but more eng time |


**Verdict: MWAA.** Reasoning:

- Q1's DAG (already written) runs with **zero code change**.
- Airflow is portable — if SemestaBank migrates clouds, the same DAG runs on Cloud
Composer (GCP) or Azure Data Factory's Airflow.
- Mature ecosystem: every major bank uses Airflow. Hiring pool is deep.
- Databricks Workflows is a tempting alternative (no MWAA fee), but loses portability.

## Decision 7 — BI serving: Databricks SQL (over Snowflake / Redshift / ClickHouse)


| Need                             | Databricks SQL Serverless     | Snowflake                  | Redshift                        | ClickHouse            |
| -------------------------------- | ----------------------------- | -------------------------- | ------------------------------- | --------------------- |
| Same Delta tables as source      | ✓ native                      | ✗ (needs data copy/import) | ✗                               | ✗                     |
| Concurrent BI users (50+)        | ✓ serverless auto-scale       | ✓                          | △ (concurrency scaling $ extra) | ✓                     |
| Cost vs $50K cap                 | included                      | +$8-12K                    | +$3-5K                          | +$5-8K                |
| Lineage to gold views            | ✓ automatic via Unity Catalog | ✗ breaks                   | ✗                               | ✗                     |
| Real-time (sub-second on stream) | ✓ via Delta streaming tables  | ✗                          | ✗                               | ✓ but separate system |


**Verdict: Databricks SQL.** Everything BI needs is already in the Lakehouse. Adding a
second warehouse for BI duplicates the gold data, breaks lineage, and costs $5-12K/mo.

## Decision 8 — Governance: Unity Catalog (over Collibra / Lake Formation / OpenLineage only)


| Function                     | Unity Catalog              | Collibra/Alation     | Lake Formation + Glue Catalog   | OpenLineage + Marquez only    |
| ---------------------------- | -------------------------- | -------------------- | ------------------------------- | ----------------------------- |
| Column-level lineage         | ✓ automatic                | ✓ but $50-100K/yr    | ✗ table-level                   | ✓ if you emit facets manually |
| Access control (RBAC + ABAC) | ✓                          | ✓ (separate tool)    | ✓ Lake Formation                | ✗ (separate Ranger/LF)        |
| PII masking                  | ✓                          | ✓ (separate)         | △ (Lake Formation does limited) | ✗                             |
| Audit log                    | ✓                          | ✓ (separate)         | ✓ CloudTrail                    | ✗                             |
| Cost                         | $0 with Databricks Premium | $50-100K/yr          | $1-2K/mo + labor                | $0 OS + labor                 |
| Effort                       | none — auto capture        | heavy implementation | medium                          | heavy manual emission         |


**Verdict: Unity Catalog as primary + OpenLineage/Marquez as fallback for non-Databricks
pipelines (Airflow, Airbyte, BI tools).**

OpenLineage alone isn't enough because it doesn't do access control or PII masking.
Collibra is excellent but eats the budget. Lake Formation lacks column-level lineage.
Unity Catalog does the most for the least money.

## Decision 9 — ML: Databricks ML Runtime + MLflow (over SageMaker / Vertex AI)


| Criteria                   | Databricks ML                       | SageMaker                 | Vertex AI               |
| -------------------------- | ----------------------------------- | ------------------------- | ----------------------- |
| Already used by SemestaBank | ✓ scenario says so                  | ✗                         | ✗                       |
| Shared lineage with BI     | ✓ Unity Catalog                     | ✗ separate                | ✗ separate              |
| Feature store integrated   | ✓ built-in                          | ✓ SageMaker FS (separate) | ✓ Vertex FS (separate)  |
| Model registry lineage     | ✓ MLflow                            | ✓ Model Registry          | ✓ Vertex Model Registry |
| Compute cost               | included in $22K Databricks billing | +$3-5K/mo                 | +$3-5K/mo               |


**Verdict: Databricks ML.** Migration to SageMaker or Vertex AI would cost $100K+ in
engineering time and break the shared lineage. No upside for 6-month migration risk.

## Decision 10 — Marketing connectors: Airbyte (over Fivetran / custom)


| Source                    | Volume | Connector ecosystem           |
| ------------------------- | ------ | ----------------------------- |
| Zendesk (tickets, 15K/mo) | tiny   | Airbyte has 1-click connector |
| Braze (campaigns)         | small  | Airbyte has a connector       |
| Google Ads (ad stats)     | small  | Airbyte has a connector       |



| Option                   | Cost                        | Why                                                |
| ------------------------ | --------------------------- | -------------------------------------------------- |
| Fivetran                 | $1.5-3K/mo for 3 connectors | Excellent but 3-6% of budget for low-value ingest  |
| Airbyte open source      | $30/mo EC2                  | Twice the learn effort, half the cost (negligible) |
| Custom Airflow operators | $0 + eng time               | Max flexibility, max maintenance burden            |


**Verdict: Airbyte open source.** Low volume sources don't justify Fivetran pricing.
Engineering effort to maintain these 3 connectors is small — Airbyte handles upgrades.

## Decision 11 — Terraform / IaC (over ClickOps)

SemestaBank needs to rebuild environments fast and prove the build to OJK. **Terraform +
Databricks Terraform provider + AWS provider** is the only sane path.

What's in Terraform:

- AWS: MSK cluster, DMS instance, MWAA environment, S3 buckets, KMS keys, IAM roles.
- Databricks: workspace, cluster policies, Unity Catalog grants, SQL warehouses, MLflow
registry.

Not in scope for this test, but mentioned in the answer because it signals production
maturity. ClickOps ("manually create this bucket — I'll re-do it next time") is a recipe
for non-reproducible environments, which fails the OJK auditability test.

## The decision principles, revisited

Every choice above traces back to **three architecture principles** (these are what you
say aloud if the evaluator asks "summarize your design philosophy"):

1. **One platform, not a patchwork.** Every extra tool adds governance, security, and
  staffing overhead. For a 4.2M-customer bank with 8 engineers, fewer tools wins.
2. **Lineage is automatic, not bolted on.** The OJK directive is the dominant non-functional
  requirement. We bias our choices toward tools that capture lineage for free (Unity
   Catalog, Delta time travel) over tools that require manual lineage emission (Snowflake
  - Collibra).
3. **Portability over optimization.** Q1's PySpark code, Airflow DAG, and Delta schemas
  work on AWS, GCP, or Azure Databricks. We give up 5-10% efficiency to keep that
   flexibility — SemestaBank is a 6-month-old platform and may still pivot clouds.

