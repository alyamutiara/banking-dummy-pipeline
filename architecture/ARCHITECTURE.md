# Task 1 — SemestaBank Data Platform Architecture

> **Question:** Draw the complete architecture. Show data sources, ingestion, storage
> layers, processing engines, serving layer, and governance. Justify each choice over
> alternatives. Stay inside the **$50K/month** cloud budget.

## Platform in one sentence

A single **Databricks Lakehouse on AWS**, with **Unity Catalog** for governance, **MSK**
for Kafka streaming, and **Airflow (MWAA)** orchestrating the batch ELT that Q1 already
specifies — one platform covering batch + streaming + ML + regulatory + governance,
instead of stitching Snowflake + Databricks + Glue + Kafka + Airflow into a 7-tool
spaghetti.

## Why Databricks Lakehouse on AWS


| Factor                                           | Decision                                                                                                                                                                      |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SemestaBank's existing credit-scoring ML pipeline | The scenario says it's **already on Databricks**. Adding a second platform (Snowflake) for BI doubles cost, doubles governance tooling, and breaks lineage between ML and BI. |
| OJK auditability directive                       | Unity Catalog's column-level lineage and Delta Lake's **time travel** (every snapshot version retained) are purpose-built for audit. No extra tool needed.                    |
| $50K/month budget                                | A Databricks-only platform beats Snowflake + Databricks by ~$15K/month. Snowflake DBUs are $3-5/credit; Databricks Jobs DBUs are $0.40/credit for the same Spark.             |
| Streaming at 50K events/sec                      | Databricks Structured Streaming + Delta Lake reads from MSK without a separate Flink cluster. Same engine that runs the Q1 batch.                                             |
| Medallion (Bronze/Silver/Gold)                   | Delta Lake's native pattern. No need to bolt it onto Snowflake.                                                                                                               |


## The architecture diagram

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              SEMESTABANK DATA PLATFORM                            │
│                                                                                  │
│ ┌─────────────────────────────────── DATA SOURCES ─────────────────────────────┐ │
│ │                                                                              │ │
│ │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────-──┐ ┌─────────┐   │ │
│ │  │   Oracle   │  │  Mobile    │  │  Zendesk   │  │  Braze    │ │ Google  │   │ │
│ │  │  core bank │  │  app       │  │  Support   │  │  Marketing│ │  Ads    │   │ │
│ │  │  (2M txn/d)│  │ (50K/s)    │  │ (15K/mo)   │  │  API      │ │  API    │   │ │
│ │  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘  └─────┬─────┘ └────┬────┘   │ │
│ │        │               │               │               │            │        │ │
│ └────────┼───────────────┼───────────────┼───────────────┼────────────┼────────┘ │
│          │               │               │               │            │          │
│ ─────────┼───────────────┼───────────────┼───────────────┼────────────┼───────── │
│          ▼               ▼               ▼               ▼            ▼          │
│ ┌─────────────────────────────────── INGESTION ────────────────────────────────┐ │
│ │                                                                              │ │
│ │  ┌──────────────────────┐     ┌──────────────────────┐  ┌────────────────┐   │ │
│ │  │  AWS DMS (CDC)       │     │  AWS MSK (Kafka)     │  │  Airbyte       │   │ │
│ │  │  Oracle → S3 raw     │     │  Clickstream topic   │  │  Zendesk/APIs  │   │ │
│ │  │  batch every 1 h     │     │  48 partitions       │  │  REST → S3     │   │ │
│ │  └──────────┬───────────┘     └──────────┬───────────┘  └───────┬────────┘   │ │
│ │             │                            │                      │            │ │
│ └─────────────┼────────────────────────────┼──────────────────────┼────────────┘ │
│               │                            │                      │              │
│ ──────────────┼────────────────────────────┼──────────────────────┼───────────── │
│               ▼                            ▼                      ▼              │
│ ════════════════════════════ STORAGE (S3 + Delta Lake) ════════════════════════  │
│               │                            │                      │              │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │                          BRONZE  (raw)                                   │    │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐             │    │
│  │  │ accounts   │ │ clickstream│ │ zendesk_   │ │ braze_     │             │    │
│  │  │ _raw (S3)  │ │_events_raw │ │ tickets_raw│ │ campaigns  │             │    │
│  │  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘             │    │
│  │        │              │              │              │                    │    │
│  └────────┼──────────────┼──────────────┼──────────────┼────────────────────┘    │
│           │              │              │              │                         │
│           ▼              ▼              ▼              ▼                         │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │                          SILVER (cleaned)                                  │  │
│  │  ┌───────────────────────┐ ┌──────────────┐  ┌──────────────────────┐      │  │
│  │  │ account_snapshots     │ │ clickstream_ │  │ support_tickets_     │      │  │
│  │  │ (Q1's output) ← ★     │ │ silver       │  │ silver               │      │  │
│  │  │ partitionBy(snapshot_ │ │ JSONB parsed │  │                      │      │  │
│  │  │ date)                 │ │ dedup event_ │  │                      │      │  │
│  │  │ Z-ORDER by            │ │ id           │  │                      │      │  │
│  │  │ customer_id           │ │ Z-ORDER by   │  │                      │      │  │
│  │  │                       │ │ customer_id  │  │                      │      │  │
│  │  └───────────┬───────────┘ └──────┬───────┘  └──────────────────────┘      │  │
│  └──────────────┼────────────────────┼────────────────────────────────────────┘  │
│                 │                    │                                           │
│                 ▼                    ▼                                           │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │                          GOLD (business-ready)                             │  │
│  │  ┌─────────────────────────────┐  ┌─────────────────────────────────┐      │  │
│  │  │ customer_health_scorecard   │  │ fraud_detection_alerts          │      │  │
│  │  │ (Q2's part a)               │  │ (Q2's part b)                   │      │  │
│  │  │ materialized via Delta Live │  │ materialized via Delta Live     │      │  │
│  │  │ Tables                      │  │ Tables                          │      │  │
│  │  └──────────────┬──────────────┘  └──────────────┬──────────────────┘      │  │
│  │                 │                                │                         │  │
│  │  ┌──────────────┴────────────────────────────────┴──────────────────┐      │  │
│  │  │ credit_scoring_features (ML feature store)                       │      │  │
│  │  └──────────────────────────────────────────────────────────────────┘      │  │
│  └────────────────────────────────────────────────────────────────────────────┘  |
│                                                                                  │
│ ──────────────────────────────────────────────────────────────────────────────── │
│                                                                                  │
│ ┌─────────────────────── PROCESSING ENGINES ───────────────────────────────────┐ │
│ │                                                                              │ │
│ │   ┌──────────────────┐    ┌─────────────────────┐    ┌──────────────┐        │ │
│ │   │ Databricks       │    │ Databricks          │    │ Databricks   │        │ │
│ │   │ Spark (Q1 batch) │    │ Structured          │    │ ML Runtime   │        │ │
│ │   │ account_snapshot │    │ Streaming           │    │ credit       │        │ │
│ │   │ .py runs here    │    │ (Bronze→Silver      │    │ scoring +    │        │ │
│ │   │ — same code as   │    │ clickstream)        │    │ MLflow       │        │ │
│ │   │ local Docker     │    │                     │    │ tracking     │        │ │
│ │   └──────────────────┘    └─────────────────────┘    └──────────────┘        │ │
│ │                                                                              │ │
│ │   ┌─────────────────────────────────────────────────────────────┐            │ │
│ │   │ Databricks SQL Warehouse (serverless) — T-shirt sizing 2XL  │            │ │
│ │   │ Runs Q2's gold views, BI queries, regulatory exports        │            │ │
│ │   └─────────────────────────────────────────────────────────────┘            │ │
│ │                                                                              │ │
│ │   ┌──────────────────┐                                                       │ │
│ │   │ Airflow (MWAA)   │ orchestrates Q1 + Q2 + Zendesk/Braze + ML             │ │
│ │   └──────────────────┘                                                       │ │
│ └──────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                  │
│ ──────────────────────────────────────────────────────────────────────────────── │
│                                                                                  │
│ ┌─────────────────────── SERVING LAYER ───────────────────────────────────┐      │
│ │                                                                         │      │
│ │   ┌────────────────────┐   ┌──────────────────┐  ┌────────────────┐     │      │
│ │   │ BI dashboards      │   │ ML credit-       │  │ OJK regulatory │     │      │
│ │   │ (Tableau / Looker /│   │ scoring model    │  │ reports        │     │      │
│ │   │  Power BI)         │   │ via MLflow       │  │ (CSV / API)    │  │ │
│ │   │  ← Databricks SQL  │   │ registry         │  │ ← gold views   │  │ │
│ │   └────────────────────┘   └──────────────────┘  └────────────────┘  │ │
│ └──────────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
│ ─────────────────────────────────────────────────────────────────────────── │
│                                                                            │
│ ┌─────────────────────── GOVERNANCE LAYER ─────────────────────────────────┐ │
│ │                                                                          │ │
│ │   ┌──────────────────────────────────────────────────────────────────┐ │ │
│ │   │  Databricks Unity Catalog (one catalog for the whole platform)   │ │ │
│ │   │                                                                  │ │ │
│ │   │   ┌──────────────┐  ┌───────────────┐  ┌────────────────────┐    │ │ │
│ │   │   │ Column-level │  │ Access        │  │ Audit log          │    │ │ │
│ │   │   │ lineage      │  │ control       │  │ (every query       │    │ │ │
│ │   │   │ (Oracle → BI │  │ (RBAC +       │  │  is recorded)      │    │ │ │
│ │   │   │  dashboard)  │  │  ABAC)        │  │                    │    │ │ │
│ │   │   └──────────────┘  └───────────────┘  └────────────────────┘    │ │ │
│ │   │                                                                  │ │ │
│ │   │   ┌───────────────────────────────────────────────────────┐       │ │ │
│ │   │   │ Lakehouse Federation — Oracle, Zendesk APIs as        │       │ │ │
│ │   │   │ foreign catalogs (queried via SQL, lineage preserved) │       │ │ │
│ │   │   └───────────────────────────────────────────────────────┘       │ │ │
│ │   └──────────────────────────────────────────────────────────────────┘ │ │
│ │                                                                          │ │
│ │   ┌───────────────────────┐                                               │ │
│ │   │ OpenLineage → Marquez │ fallback for non-Databricks pipelines       │ │ │
│ │   │ (Airflow events)      │ (Airbyte / custom Lambda)                   │ │
│ │   └───────────────────────┘                                               │ │
│ └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Components, justified

### 1. Storage — S3 + Delta Lake (Bronze/Silver/Gold)

**Decision:** S3 with Delta Lake as the table format across all three medallion layers.

**Why over alternatives:**


| Alternative                            | Why not                                                                                                                                                                                                                   |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Snowflake as storage + compute         | Doubles platform cost (~$8-15K extra/month for Snowflake credits). Snowflake punishes the 60M-row scans Q2 needs.                                                                                                         |
| HDFS on EC2                            | Requires cluster management. We don't want to run Hadoop. Serverless concept of Databricks on S3 means we pay only for runtime.                                                                                           |
| Redshift for Gold                      | Redshift is columnar OLAP. We already have Databricks SQL for OLAP — same SQL warehouse that serves BI and runs the gold views. Adding Redshift duplicates the role.                                                      |
| Glue Data Catalog + Parquet (no Delta) | No ACID transactions, no time travel (OJK audit requires versioning). Delta Lake gives both for free. Delta vs Iceberg vs Hudi was a real question for SemestaBank — see [PLATFORM_DECISIONS.md](./PLATFORM_DECISIONS.md). |


### 2. Ingestion

#### 2a. Oracle → Bronze (batch CDC)

**Decision: AWS DMS** (Database Migration Service) running in CDC mode, unloading changes
to S3 every 1 hour.

**Why over alternatives:**


| Alternative                                       | Why not                                                                                                                                                                                                                                                                                                                       |
| ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spark JDBC with predicate pushdown (what Q1 uses) | Q1's pipeline reads directly via JDBC for **incremental batch** (good for daily snapshot). For ongoing CDC stream into bronze, DMS is cheaper and doesn't stress Oracle's transaction log. Hybrid approach: DMS for bronze raw layer; Spark JDBC for Q1's silver snapshot query directly against Oracle for the daily metric. |
| Debezium on Kafka Connect                         | More moving parts (Kafka Connect cluster + Debezium connectors). DMS is one click and costs ~$200/month for Oracle source.                                                                                                                                                                                                    |
| Stitch / Fivetran for Oracle                      | $5K+/month for a 60M-row Oracle connection — eats 10% of the budget.                                                                                                                                                                                                                                                          |


#### 2b. Mobile app clickstream → Bronze (streaming)

**Decision: AWS MSK** (Managed Kafka) with 48-partition `clickstream_events` topic, consumed
by Databricks Structured Streaming.

**Why over alternatives:**


| Alternative                    | Why not                                                                                                                                                         |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Kinesis Data Streams           | Same throughput, but lock-in to Kinesis SDK. Kafka is portable; reusable on GCP/Azure.                                                                          |
| Self-managed Kafka on EC2      | Requires MSK-style maintenance overhead for free; cost is similar to MSK. With 50K events/sec, broker tuning matters — MSK handles it.                          |
| Kinesis Firehose → S3 directly | No buffer for reprocessing — if a consumer goes down you lose data. Kafka retains 7 days, lets replays happen. Also, Spark Structured Streaming is Kafka-first. |


#### 2c. Zendesk + Braze + Google Ads → Bronze (batch REST)

**Decision: Airbyte** (open-source, self-hosted on a small EC2) with low volume (15K/mo +
monthly marketing exports).

**Why over alternatives:**


| Alternative                           | Why not                                                                                                                      |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Fivetran                              | $1.5-3K/month per connector — 6-10% of budget for $50K. Airbyte is zero-license.                                             |
| Custom Airflow operators calling APIs | High engineering cost — every connector is its own maintenance burden. Airbyte has Zendesk, Braze, and Google Ads pre-built. |
| Glue custom jobs                      | More boilerplate than Airbyte connectors; still self-maintained.                                                             |


### 3. Processing — Databricks Cluster + SQL Warehouse

**Decision: All compute runs on Databricks.** Three execution modes:


| Use case                              | Engine                               | Why                                                                                                                                                                                  |
| ------------------------------------- | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Q1 batch (silver snapshot)            | Databricks Jobs cluster (Spark)      | Same `account_snapshot.py` runs unchanged. Just cluster config instead of `local[*]`.                                                                                                |
| Clickstream streaming (Bronze→Silver) | Databricks Structured Streaming jobs | One job reads MSK, parses JSON, dedups by `event_id`, writes to Delta Silver. Backed by the same Delta Lake — joins cleanly with Q1's silver snapshot on `customer_id`.              |
| Gold views + BI queries (Q2)          | Databricks SQL Serverless warehouse  | Serverless scales BI to many concurrent users without pre-sizing a cluster. Pays per second of query time, not per hour idle.                                                        |
| Credit scoring ML                     | Databricks ML Runtime + MLflow       | Already there (scenario says credit scoring is on Databricks). MLflow tracks every model version → lineage to training data → lineage to gold features → lineage to silver snapshot. |


**Why over alternatives:**


| Alternative                            | Why not                                                                                                                                                         |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AWS Glue for batch                     | Glue is serverless Spark, cheaper per job, but: no Unity Catalog, no native MLflow, no Databricks SQL BI. Adds a third engine.                                  |
| Snowflake for BI                       | Snowflake is great at SQL BI, but adding it doubles governance surface and breaks lineage between batch (Databricks) and serving (Snowflake). Plus $10K+/month. |
| AWS Lambda + EventBridge for streaming | Works for small streams. 50K events/sec sustained is too much for Lambda — cold starts, concurrency caps, andobservability gaps make it the wrong tool.         |
| Kinesis Data Analytics (Flink)         | Decent Flink-as-a-service, but the team is already Spark/Databricks. Use Structured Streaming — same APIs, same runtime, same Ops playbook.                     |


### 4. Orchestration — Airflow on MWAA

**Decision: Managed Workflows for Apache Airflow (MWAA).**

The Q1 Airflow DAG (already written) runs **unchanged** in MWAA. The DAG file imports the
Databricks operators (`DatabricksSubmitRunOperator`, `DatabricksSqlOperator`) for the Q1 +
Q2 pipeline, MSK operators for stream health checks, and Airbyte operators for marketing
connectors.

**Why over alternatives:**


| Alternative          | Why not                                                                                                                                                                                                            |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Databricks Workflows | Built-in Databricks does the job. But Airflow already exists in Q1 — switching orchestrators mid-project is a non-goal. MWAA keeps Q1's DAG portable across Databricks/EMR/GCP if SemestaBank changes clouds later. |
| Step Functions       | Vendor-locked to AWS. The DAG Q1 wrote is plain Airflow — portable.                                                                                                                                                |
| Prefect / Dagster    | Smaller ecosystem, less proven for banker-grade audit (Airflow is used by JPMorgan, HSBC, BCA — Indonesian relevance).                                                                                             |


### 5. Serving


| Consumer                          | How                                                                                                                                                                                           | Why                                                                                                                   |
| --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| BI (Tableau/Looker/Power BI)      | Connect to **Databricks SQL** endpoint via JDBC/ODBC                                                                                                                                          | One connection point. Same gold tables that the regulatory views use. No data duplication to a separate BI warehouse. |
| ML credit scoring                 | Reads gold **feature store tables** via Databricks ML runtime                                                                                                                                 | Already in the scenario. MLflow registry holds the model; Feature Store holds the engineered features.                |
| OJK regulatory export             | Scheduled query (Airflow) writes `gold.customer_health_scorecard` and `gold.fraud_detection_alerts` to a **sealed S3 bucket** with object-lock enabled, then pushes the digest via OJK's API. | Object-lock makes the audit artifact immutable — meets the OJK "auditability" directive.                              |
| Real-time dashboard (CDXO / risk) | Databricks SQL queries against Delta Silver (clickstream), with millisecond-fresh results via Structured Streaming + Delta `MERGE` to gold                                                    | One tool, one pane, no separate "real-time warehouse" like ClickHouse. Within $50K budget.                            |


### 6. Governance — Databricks Unity Catalog

**Decision:** Unity Catalog as the single governance plane for the whole platform.


| Governance need                       | How Unity Catalog satisfies it                                                                                                                                                                                                                                                  |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Catalog** of all tables and columns | Every S3 Delta table is registered in Unity Catalog under `semestabank.bronze.`*, `semestabank.silver.`*, `semestabank.gold.*`. One browseable catalog for the DGQA team.                                                                                                          |
| **Column-level lineage**              | Unity Catalog auto-tracks lineage for every SQL/Spark transformation. The DGQA team can answer "which Oracle column feeds the BI tile's `risk_flag`?" in one click.                                                                                                             |
| **Access control (RBAC + ABAC)**      | Grant `SELECT` on `gold.customer_health_scorecard` to the regulatory team; `USE` on `silver.`* only to data engineers; `READ` on `bronze.`* to no one (only pipelines touch bronze). Tagged with `pii=true` for `customers.national_id` — auto-masked for non-compliance roles. |
| **Audit log**                         | Every `SELECT`, `INSERT`, `GRANT` is captured via Databricks audit logs; shipped to CloudWatch + S3 with 7-year retention (OJK compliance).                                                                                                                                     |


**Why over alternatives:**


| Alternative                 | Why not                                                                                                                                                                       |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Glue Data Catalog           | No column-level lineage; access control requires Lake Formation; Spark-only; no MLflow.                                                                                       |
| Collibra / Alation          | Enterprise-grade, but $50-100K/year — eats the entire budget. Adds a 10th tool.                                                                                               |
| OpenLineage + Marquez alone | Open source and free, but no built-in access control or audit log. We use it as fallback for non-Databricks pipelines (see [GOVERNANCE_LINEAGE.md](./GOVERNANCE_LINEAGE.md)). |
| Azure Purview               | Wrong cloud.                                                                                                                                                                  |


### 7. Budget summary (full breakdown in [BUDGET_BREAKDOWN.md](./BUDGET_BREAKDOWN.md))


| Component                            | Monthly cost                                     |
| ------------------------------------ | ------------------------------------------------ |
| Databricks compute (Jobs + SQL + ML) | $22K                                             |
| AWS S3 storage (3-tier lifecycle)    | $3K                                              |
| AWS MSK (48-partition Kafka cluster) | $4K                                              |
| AWS DMS (Oracle CDC)                 | $0.5K                                            |
| MWAA (Airflow)                       | $1K                                              |
| Airbyte EC2 (Zendesk + Braze)        | $0.3K                                            |
| Network, logging, monitoring         | $2K                                              |
| **Total**                            | **~$33K of $50K** — **$17K headroom** for growth |


## Why this architecture beats the obvious alternatives

### Alternative A: Snowflake + Databricks + Glue + Kafka + Airflow (the "default" enterprise playbook)

Cost: ~$55-65K/month (Snowflake $10K + Databricks $15K + MSK $4K + Glue $3K + MWAA $1K +
Lake Formation/Glue catalog $1K + Purview/Collibra not even counted).

Two governance catalogs (Snowflake + Unity Catalog). Lineage breaks at the Snowflake
boundary. The Q1 pipeline writes to S3 — Snowflake can't see who read what column.

### Alternative B: GCP + BigQuery + Dataproc + Cloud Data Fusion + Datastream

A serious alternative if SemestaBank was greenfield. Costs $30-40K/month. But:

- SemestaBank's credit-scoring ML is **already on Databricks** (scenario says so). Migrating
it to Vertex AI is a $200K+ migration project plus 6 months.
- Casting Cloud DLP for column-level lineage is more workmanlike than Unity Catalog.
- Same budget triangle — saving $5K not worth 6 months of migration.

### Alternative C: AWS-only (no Databricks) — Glue + Lake Formation + Redshift + EMR + MSK

Cost: similar to Databricks ($30K). But:

- 5 different AWS services to operate, each with its own IAM roles, its own pricing model,
its own lineage model. Lake Formation's lineage is table-level, not column-level.
- No unified ML platform — credit scoring would need a new SageMaker setup.
- Harder developer experience (5 services vs 1 platform). Q1's PySpark code would need Glue
wrappers; Q2's views would need Redshift dialect tweaks (PIVOT exists; RANGE INTERVAL
does not).

## What carries over from Q1 and Q2 (the integration story)

The architecture is not a brand-new design — it's the production home for the work already
done in Q1 and Q2.


| From Q1                                                                       | Lives in this architecture as                                                                                                            |
| ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `account_snapshot.py` PySpark job                                             | Runs on **Databricks Jobs cluster** instead of `local[*]`. Same code, different `--master`.                                              |
| `docker-compose.yml` (Postgres + MinIO + Spark + Airflow)                     | Local dev still runs. Prod substitutes Oracle for Postgres, S3-Delta for MinIO, Databricks for local Spark, MWAA for standalone Airflow. |
| `account_snapshot_dag.py` Airflow DAG                                         | **Unchanged** in MWAA. Just swaps the `DockerOperator` for `DatabricksSubmitRunOperator`.                                                |
| DQ assertions, partitionBy(snapshot_date), dynamic overwrite, secrets via env | All identical. Databricks Secret Scope replaces `.env`. Unity Catalog auto-tracks lineage from the snapshot table.                       |



| From Q2                               | Lives in this architecture as                                                                                                                                                                                         |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `gold.customer_health_scorecard` view | Implemented as a **Delta Live Tables pipeline** or **Databricks SQL view** on top of `silver.account_snapshots` (which Q1 produces). Materialization is automatic, incremental, and lineage-tracked by Unity Catalog. |
| `gold.fraud_detection_alerts` view    | Same — DLT pipeline, refreshes after Q1's silver snapshot is ready.                                                                                                                                                   |
| Materialized-view refresh (part c)    | Replaced by DLT incremental materialization. No more `REFRESH MATERIALIZED VIEW CONCURRENTLY`. And the 45-minute materialization drops to < 5 minutes (DLT only processes new partitions).                            |


## What this architecture explicitly does NOT include (and why)


| Component                                                                | Why not (cost / scope guard)                                                                                                                                         |
| ------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A separate real-time "operational warehouse" (ClickHouse / Apache Pinot) | The 50K/sec clickstream fits easily in Delta + Databricks SQL with streaming queries. Adding Pinot would duplicate storage ($3K/month) and another engine to govern. |
| An additional Spark-based feature store (Feast, Tecton)                  | Databricks Feature Store is built in. Avoid the cost of a second feature store.                                                                                      |
| A "data mesh" federated domain ownership structure                       | SemestaBank has 4.2M customers and 8 data engineers. Data mesh is for 100-engineer orgs. We use a simpler medallion pattern with clear ownership per table.           |
| Trino/Presto as a query-on-S3 federated layer                            | Databricks SQL already does this. Adding Trino duplicates compute and adds a maintenance burden.                                                                     |
| Custom lineage Python code on every pipeline                             | Hand-coded lineage is a known anti-pattern: it drifts and lies. Unity Catalog's automatic lineage capture is more accurate and needs zero code from us.              |


## Architecture principles (the rubric's "trade-offs" criterion)

The architecture is governed by three principles. Every component decision traces back to one of them.

1. **One platform, not many tools.** Each tool added to the stack adds governance, security,
  and skills overhead. SemestaBank is mid-sized and Indonesia-based, not Google. Fewer
   tools beats "best of breed" when budgets are tight.
2. **Lineage is free, not built.** The OJK directive forces us to instrument lineage. We
  choose tools that capture it automatically (Unity Catalog) over tools that require us
   to bolt it on (Snowflake + Collibra).
3. **Portability over lock-in.** Q1's PySpark code and Airflow DAG work on AWS, GCP, or
  Databricks any decade. We give up 5-10% efficiency to keep that flexibility — and it
   paid off when we could design Q3 to host whichever cloud SemestaBank eventually picks.

