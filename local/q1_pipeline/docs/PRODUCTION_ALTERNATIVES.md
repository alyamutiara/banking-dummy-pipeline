# Production deployment — lakehouse on cloud infrastructure

The local stack (Docker Compose) proves the full medallion pipeline end-to-end. In production the
**same PySpark code and Airflow DAG run unchanged** — only the infrastructure underneath swaps
out. This file maps each local component to its production equivalent.

## Design principle: same code, different infra

```
Local (Docker)                  →    Production (any of the stacks below)
────────────────────────             ────────────────────────────────────
PostgreSQL   (bronze source)   →    Oracle core banking (unchanged JDBC URL string)
PostgreSQL   (stage tables)    →    Oracle Data Pump / GoldenGate CDC / AWS DMS
MinIO/bronze (raw Parquet)     →    S3 / GCS / ADLS Gen2 bronze bucket
MinIO/silver (clean, partitioned) → S3 / GCS / ADLS Gen2 silver bucket
MinIO/gold   (business views)  →    S3 / GCS / ADLS Gen2 gold bucket
Spark        (local mode)      →    EMR / Dataproc / Databricks / Glue
Airflow      (standalone)      →    MWAA / Cloud Composer / Databricks Workflows
CSV COPY     (init.sql)        →    Oracle Data Pump exports / AWS DMS / CDC stream
Merchant     (hardcoded)       →    QRIS registry / Visa acquiring / payment network
```

The only things that change between environments:

1. `JDBC_URL` — points at the real Oracle source.
2. `S3_BUCKET_BRONZE`, `S3_BUCKET`, `S3_BUCKET_GOLD` — real cloud bucket names.
3. `S3_ENDPOINT` — empty for real S3/GCS/ADLS.
4. Spark `--master` — `local[*]` → cluster endpoint or Databricks job submission.
5. `merchant_locations` — loaded from the payment network's registry, not hardcoded.

All four pipeline jobs (`bronze_ingest.py`, `account_snapshot.py`, `gold_scorecard.py`,
`gold_fraud.py`) and the Airflow DAG structure are **identical** in every stack.

## Component-by-component mapping

| Local (this repo)                  | AWS native                    | GCP native                              | Databricks                                        |
| ---------------------------------- | ----------------------------- | --------------------------------------- | ------------------------------------------------- |
| **PostgreSQL** (source)            | Oracle on RDS / on-prem       | Oracle on Bare Metal Solution           | Oracle JDBC (Databricks Runtime)                  |
| **MinIO bronze** (raw)             | S3 bronze bucket              | GCS bronze bucket                       | DBFS / S3 bronze bucket                           |
| **MinIO silver** (clean)           | S3 silver bucket              | GCS silver bucket                       | DBFS / S3 silver bucket + Unity Catalog table     |
| **MinIO gold** (views)             | S3 gold bucket                | GCS gold bucket                         | DBFS / S3 gold bucket + Unity Catalog table       |
| **Spark container** (compute)      | EMR Serverless / EMR on EC2   | Dataproc Serverless / Dataproc on GCE   | Databricks Runtime (managed Spark)                |
| **Airflow** (orchestration)        | MWAA (managed Airflow)        | Cloud Composer                         | Databricks Workflows                              |
| **Secrets** (`.env`)               | Secrets Manager + IAM role    | Secret Manager + service account        | `dbutils.secrets.get()` + Databricks Secret Scope |
| **IAC** (manual)                   | CloudFormation / CDK          | Deployment Manager / Terraform          | Terraform + Databricks API                        |

## Lakehouse medallion on AWS (production)

```
Oracle core banking
    │  JDBC
    ▼
EMR Serverless (PySpark)
    │  ├─ bronze_ingest.py      reads Oracle → S3 bronze (Parquet)
    │  ├─ account_snapshot.py   reads S3 bronze → S3 silver (partitioned)
    │  ├─ gold_scorecard.py     reads S3 bronze+silver → S3 gold
    │  └─ gold_fraud.py         reads S3 bronze → S3 gold
    │
    ▼
S3 lakehouse
    ├─ s3://semestabank-bronze/     (raw Parquet, partitioned by txn_day)
    ├─ s3://semestabank-silver/     (daily snapshots, partitioned by snapshot_date)
    └─ s3://semestabank-gold/       (regulatory views)
        ├─ customer_health_scorecard/
        └─ fraud_detection_alerts/
    │
    ▼
MWAA (managed Airflow)
    │  runs the same DAG: bronze → silver → gold → credit_scoring
    │
    ├── success → trigger credit-scoring (SageMaker / Databricks ML)
    └── fail ×3 → SNS → PagerDuty + Slack → email (audit trail)
```

**What changes in code:**
- `JDBC_URL=jdbc:oracle:thin:@core-db:1521/banking`
- `S3_ENDPOINT` is removed (defaults to real S3)
- Bucket names: compliant S3 bucket names (e.g., `semestabank-prod-bronze`)
- `merchant_locations` loaded from QRIS registry API, not hardcoded
- Secrets via IAM role + Secrets Manager (no `.env`)
- Spark `--master` → EMR Serverless entry point

**What stays the same:** all PySpark logic, Spark SQL, DQ gates, partitioning, and the
DAG wiring (retry/alert/SLA).

## Lakehouse medallion on Databricks (most realistic for SemestaBank)

Given SemestaBank's scenario already mentions a **credit scoring ML pipeline on Databricks**,
this is the most realistic production path.

```
Oracle core banking
    │  JDBC (Databricks Runtime has Oracle driver)
    ▼
Databricks Workflows
    │  ├─ Task: bronze_ingest       → Unity Catalog: semestabank_bronze.*
    │  ├─ Task: account_snapshot    → Unity Catalog: semestabank_silver.account_snapshots
    │  ├─ Task: gold_scorecard      → Unity Catalog: semestabank_gold.customer_health
    │  └─ Task: gold_fraud          → Unity Catalog: semestabank_gold.fraud_alerts
    │
    ▼
Unity Catalog (medallion)
    ├─ semestabank_bronze.*          (Delta tables, raw)
    ├─ semestabank_silver.*          (Delta tables, clean)
    └─ semestabank_gold.*            (Delta tables, business views)
    │
    ▼
Downstream consumers
    ├─ BI: Power BI / Tableau / Superset (ODBC/JDBC to Databricks SQL)
    ├─ ML:  credit scoring feature store → SageMaker / MLflow
    └─ OJK: audit queries via Delta TIME TRAVEL
```

**Why Databricks for SemestaBank:**
- Unity Catalog provides column-level lineage (OJK auditability directive, Q3)
- Delta Lake's time-travel enables `SELECT * FROM snapshots VERSION AS OF '2025-09-01'`
- Single platform for batch (lakehouse) + ML (credit scoring) — no data movement
- Databricks Workflows replaces Airflow for native retry/alert/SLA within the ecosystem

## What does NOT change regardless of stack

- **Incremental loading** via JDBC predicate pushdown (or CDC watermark).
- **Medallion partitioning**: `txn_day` (bronze), `snapshot_date` (silver), full overwrite (gold).
- **Dynamic partition overwrite** — a Spark config, not a cloud feature.
- **DQ assertions** — pure Python, no cloud dependency.
- **PII masking** — applies at bronze ingest, same logic in every stack.
- **Spark SQL gold views** — portable across any Spark runtime.
- **Retry/alert/SLA pattern** — identical in Airflow, Databricks Workflows, and Step Functions.
