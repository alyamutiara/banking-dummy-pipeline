# Question 1 — Lakehouse Pipeline: End-to-End Medallion Architecture

SemestaBank nightly ELT, implemented as a cloud-agnostic Spark-on-MinIO lakehouse with Bronze → Silver → Gold medallion layers. Covers Q1 (pipeline optimization) and Q2 (regulatory views).

## Architecture

```
PostgreSQL (bronze source)       MinIO Lakehouse
 ┌──────────────────────┐        ┌──────────────────────────────────┐
 │ bronze.customers     │  JDBC  │ semestabank-bronze/               │
 │ bronze.accounts      │ ────→  │   customers/         (Parquet)   │
 │ bronze.transactions  │        │   accounts/                      │
 │ bronze.credit_scores │        │   transactions/                  │
 │ ...                  │        │   merchant_locations/            │
 └──────────────────────┘        └───────────┬──────────────────────┘
                              ┌──────────────┼──────────────────────┐
                              │   Spark      │   MinIO              │
                              │   account_   ▼                      │
                              │   snapshot   semestabank-silver/     │
                              │   .py        account_snapshots/     │
                              │              (partitioned by date)  │
                              └──────────────┬──────────────────────┘
                              ┌──────────────┼──────────────────────┐
                              │   Spark SQL  ▼                      │
                              │   gold_      semestabank-gold/       │
                              │   scorecard  customer_health_       │
                              │   gold_      scorecard/             │
                              │   fraud      fraud_detection_       │
                              │              alerts/                │
                              └─────────────────────────────────────┘
```

## What's in this folder


| Path                                   | Purpose                                                                                                                                                                                                                  |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `spark/jobs/pipeline.sh`               | **End-to-end orchestrator.** Runs bronze → silver → gold in order. Used as the Spark container entrypoint.                                                                                                               |
| `spark/jobs/bronze_ingest.py`          | **Bronze layer.** Reads all 7 tables from PostgreSQL via JDBC, writes raw Parquet to MinIO. Incremental for transactions (watermark), full-overwrite for small dimensions. Includes PII masking and duplicate detection. |
| `spark/jobs/account_snapshot.py`       | **Silver layer (Q1 rewrite).** Reads bronze Parquet, builds daily per-account snapshots partitioned by `snapshot_date`. DQ-gated, idempotent.                                                                            |
| `spark/jobs/backfill_snapshot.py`      | **Historical backfill.** Single-pass: reads all transactions, snapshots every date, writes all partitions in one `parquet()` call (Spark equivalent of Glue's `overwritePartitions`).                                    |
| `spark/jobs/gold_scorecard.py`         | **Gold layer (Q2a).** Monthly customer health scorecard with MoM balance changes, credit utilization, MRR estimation, transaction breakdowns, and risk flagging.                                                         |
| `spark/jobs/gold_fraud.py`             | **Gold layer (Q2b).** Fraud detection: 3 patterns (high-frequency, multi-city, amount anomaly) via window functions, combined with UNION ALL.                                                                            |
| `spark/jobs/query_silver.py`           | **Ad-hoc query helper.** Connects to MinIO, reads silver Parquet, prints schema + stats.                                                                                                                                 |
| `docs/PROBLEMS_AND_FIXES.md`           | **Q1(a) write-up:** 8 pipeline problems + 6 infrastructure issues + fixes.                                                                                                                                               |
| `docs/PRODUCTION_ALTERNATIVES.md`      | How the same code maps to AWS, GCP, Databricks, and Azure.                                                                                                                                                               |
| `airflow/dags/account_snapshot_dag.py` | **Q1(c):** Airflow DAG orchestrates all 4 jobs (bronze → silver → scorecard → fraud) with retries, SLA, and on-call alerting.                                                                                            |
| `docker-compose.yml`                   | One command: Postgres (source) + MinIO (lake) + Spark (compute) + Airflow (orchestration).                                                                                                                               |
| `seed/init.sql`                        | Bronze schema + `COPY` commands bulk-loading `../semestabank_dataset/` CSVs into Postgres.                                                                                                                                |
| `spark/Dockerfile`                     | Spark 3.5.8 with hadoop-aws + postgresql JDBC pre-bundled.                                                                                                                                                               |
| `tests/test_account_snapshot.py`       | 7 pytest unit tests: dedup, status filter, aggregation, zero-fill, balance reconciliation pass/fail.                                                                                                                     |
| `.env.example`                         | All config / secrets (copy to `.env`).                                                                                                                                                                                   |


## Dataset source

The pipeline sources from `../semestabank_dataset/` — 7 CSV files totalling ~2M rows. On first `docker compose up`, `init.sql` bulk-loads them into PostgreSQL via `COPY` commands. The `bronze_ingest.py` job then reads from PostgreSQL and writes to MinIO bronze.


| File                       | Rows      | Postgres Table                | MinIO Path                                |
| -------------------------- | --------- | ----------------------------- | ----------------------------------------- |
| `customers.csv`            | 5,000     | `bronze.customers`            | `semestabank-bronze/customers/`            |
| `accounts.csv`             | 7,954     | `bronze.accounts`             | `semestabank-bronze/accounts/`             |
| `transactions.csv`         | 1,991,349 | `bronze.transactions`         | `semestabank-bronze/transactions/`         |
| `credit_scores.csv`        | 7,452     | `bronze.credit_scores`        | `semestabank-bronze/credit_scores/`        |
| `app_events.csv`           | 500,000   | `bronze.app_events`           | `semestabank-bronze/app_events/`           |
| `support_tickets.csv`      | 16,953    | `bronze.support_tickets`      | `semestabank-bronze/support_tickets/`      |
| `acquisition_channels.csv` | 5,000     | `bronze.acquisition_channels` | `semestabank-bronze/acquisition_channels/` |
| *generated*                | 20        | —                             | `semestabank-bronze/merchant_locations/`   |


## Quick start

```bash
cd local/q1_pipeline
cp .env.example .env

# 1) Start the platform (source DB + MinIO lake with 3 buckets)
docker compose up -d

# 2) Run the full pipeline (bronze → silver → gold)
docker compose --profile run up spark

# 3) Inspect results
# MinIO console: http://localhost:9001 (minioadmin / minioadmin123)
# Query silver:
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" \
    /opt/spark/jobs/query_silver.py
```

**Note:** PostgreSQL is on port **5433** on the host to avoid conflicts with local installations. Internal container-to-container uses 5432.

## Pipeline stages

### Stage 1: Bronze (`bronze_ingest.py`)

Reads from PostgreSQL JDBC → writes raw Parquet to `semestabank-bronze/`:

- **Dimensions** (customers, accounts, credit_scores, etc.): full overwrite every run (small)
- **Transactions**: incremental via high-watermark. Reads `WHERE txn_date >= watermark` pushed down to PostgreSQL. Written partitioned by `txn_day` with dynamic partition overwrite.
- **PII masking**: NIK and phone masked to last 4 digits on customers
- **Duplicate detection**: logs NIK/phone duplicates per OJK requirements
- **Supplementary**: creates `merchant_locations` lookup table (20 rows)

### Stage 2: Silver (`account_snapshot.py`)

Reads bronze Parquet → builds daily per-account snapshots in `semestabank-silver/account_snapshots/`:

- Deduplicates transactions (`dropDuplicates(["txn_id"])`)
- Filters to `status == 'COMPLETED'` only
- Aggregates: `txn_count`, `credit_amount`, `debit_amount`, `txn_total_amount`, `distinct_channels`
- Left-joins to accounts (zero-fill for inactive accounts)
- Partitions by `snapshot_date` with dynamic overwrite (idempotent)
- DQ gates: row count, null keys, non-negative counts, balance reconciliation

Historical backfill runs separately via `backfill_snapshot.py` (single-pass, all dates in one `parquet()` call).

### Stage 3: Gold — Scorecard (`gold_scorecard.py`)

Reads bronze + silver Parquet → `semestabank-gold/customer_health_scorecard/`:

- Monthly balance with MoM change (LAG from silver snapshots)
- Credit utilization (balance / credit_limit)
- MRR estimation (savings fees + credit card fees + loan interest)
- Transaction breakdowns by type and channel
- Latest credit score with probability of default
- Risk flag (high utilization, high PD, or >30% balance drop)

### Stage 3: Gold — Fraud (`gold_fraud.py`)

Reads bronze Parquet (transactions + accounts + merchant_locations) → `semestabank-gold/fraud_detection_alerts/`:

- **Pattern 1**: 5+ transactions within rolling 1-hour window (RANGE)
- **Pattern 2**: 3+ different merchant cities on same day
- **Pattern 3**: single tx > 3x rolling 30-transaction average (ROWS)
- Combined via `UNION ALL` with structured JSON details

## Silver schema (`account_snapshots`)


| Column                   | Type          | Description                                 |
| ------------------------ | ------------- | ------------------------------------------- |
| `account_id`             | string        | PK from bronze.accounts                     |
| `customer_id`            | string        | FK to bronze.customers                      |
| `account_type`           | string        | SAVINGS / LOAN / CREDIT_CARD / INVESTMENT   |
| `product_name`           | string        | e.g. Tabungan Negara Plus                   |
| `opened_date`            | date          | Account opening date                        |
| `status`                 | string        | ACTIVE / DORMANT / CLOSED / SUSPENDED       |
| `balance`                | decimal(15,2) | Current account balance                     |
| `credit_limit`           | decimal(15,2) | Credit card limit (nullable)                |
| `interest_rate`          | decimal(5,4)  | Loan/savings rate (nullable)                |
| `txn_total_amount`       | decimal(25,2) | Sum of all completed txns today             |
| `txn_count`              | long          | Number of completed txns today              |
| `debit_amount`           | decimal(25,2) | Sum of DEBIT type amounts                   |
| `credit_amount`          | decimal(25,2) | Sum of CREDIT type amounts                  |
| `distinct_channels`      | long          | Count of unique channels used               |
| `computed_close_balance` | decimal(27,2) | balance - credits + debits (reconciliation) |
| `snapshot_date`          | date          | Partition key                               |


## Medallion bucket layout

```
semestabank-bronze/
  customers/
  accounts/
  transactions/           (partitioned by txn_day)
  credit_scores/
  app_events/
  support_tickets/
  acquisition_channels/
  merchant_locations/

semestabank-silver/
  account_snapshots/      (partitioned by snapshot_date)

semestabank-gold/
  customer_health_scorecard/
  fraud_detection_alerts/
```

## Orchestration

Airflow DAG (`account_snapshot_dag.py`, nightly 02:00):

```
start → bronze_ingest → run_snapshot → gold_scorecard → gold_fraud
                                                   ↘ alert_engineer (on failure)
                                                   ↗ trigger_credit_scoring (on success)
```

- Retries: 3x with exponential back-off (max 10 min)
- SLA: 30 minutes on the silver snapshot
- Alert: Slack webhook (configurable) + email fallback
- All jobs use the same Docker image (`semestabank-lakehouse-spark:latest`)

## Design decisions

- **Spark as the lakehouse compute**: same engine reads PostgreSQL (JDBC) for staging and Parquet for all downstream layers. No SQL engine switch between layers.
- **MinIO as the lake**: S3-compatible, bucket-per-layer, all Parquet with Snappy compression.
- **Dynamic partition overwrite**: `mode("overwrite")` + `partitionBy()` replaces only matching partitions, enabling idempotent re-runs.
- **Incremental transactions**: high-watermark pushed to PostgreSQL (or Oracle in prod) as a WHERE clause — historical rows never cross JDBC.
- **SQL-first gold layer**: `gold_scorecard.py` and `gold_fraud.py` use Spark SQL temp views with CTEs — same SQL logic as the regulatory SQL scripts in Q2_Regulatory_Views.
- **PII masking at ingestion**: NIK/phone masked in bronze, so no downstream view sees plaintext.
- **Balance reconciliation tolerates <R1 drift**: fails only if mismatch exceeds 2%, avoiding false positives from timing artifacts.

