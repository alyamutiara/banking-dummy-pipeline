# Question 2 — Regulatory Reporting Views

OJK-mandated SQL views for SemestaBank: the Monthly Customer Health Scorecard and fraud
detection alerts, with an incremental materialization strategy.

## Architecture: Lakehouse on MinIO

All three medallion layers live in MinIO (S3-compatible object storage). Spark is the
unified compute engine. The PostgreSQL SQL versions in `sql/` are preserved as reference;
the production equivalents run as PySpark SQL jobs in `Q1_Pipeline_Optimization/spark/jobs/`.

```
PostgreSQL (source,JDBC)──► [bronze_ingest.py] ──► MinIO/bronze/   (raw Parquet)
                                               │
MinIO/bronze ──► [account_snapshot.py] ──► MinIO/silver/   (cleaned, partitioned)
                                               │
MinIO/bronze + silver ──► [gold_scorecard.py] ──► MinIO/gold/  (business views)
                     └──► [gold_fraud.py]                   
```

**Single source of truth:** all data — raw, cleaned, and aggregated — is in MinIO as
Parquet files. No data is scattered across PostgreSQL + S3.

## What's in this folder


| Path                                   | Purpose                                                                                   |
| -------------------------------------- | ----------------------------------------------------------------------------------------- |
| `sql/01_customer_health_scorecard.sql` | **Reference SQL (PostgreSQL):** monthly scorecard with CTEs, LAG, conditional aggregation. MoM balance change reads **real** prior-month history from `silver.account_snapshots` (Q1's output) |
| `sql/02_fraud_detection.sql`           | **Reference SQL (PostgreSQL):** fraud alerts with RANGE/ROWS, JSONB output                |
| `sql/03_optimization_strategy.sql`     | **Materialized-view DDL** (PostgreSQL-only; lakehouse equivalent = overwrite Parquet)     |
| `seed/gold_setup.sql`                  | Creates `gold` schema in PostgreSQL (optional; not needed for lakehouse)                  |
| `seed/silver_account_snapshots.sql`    | Runnable stand-in for Q1's `silver.account_snapshots` (prior-month-end balances from real txn history) — load this before running `sql/01...` on plain PostgreSQL |
| `seed/merchant_locations.sql`          | Supplementary merchant-locations table (PostgreSQL version)                               |


**Production (Spark SQL) equivalents** live in `../Q1_Pipeline_Optimization/spark/jobs/`:

- `gold_scorecard.py` — Q2(a) as Spark SQL reading from MinIO
- `gold_fraud.py` — Q2(b) as Spark SQL reading from MinIO

## How the lakehouse pipeline works

```bash
# From Q1_Pipeline_Optimization/
docker compose --profile run up
```

This runs `spark/jobs/pipeline.sh` which executes in order:

1. `bronze_ingest.py` — PostgreSQL (JDBC) → MinIO bronze (Parquet)
2. `account_snapshot.py` — bronze → silver (Q1)
3. `gold_scorecard.py` — bronze+silver → gold scorecard (Q2a)
4. `gold_fraud.py` — bronze → gold fraud alerts (Q2b)

Results are in MinIO at:

- `s3://semestabank-bronze/` — raw tables
- `s3://semestabank-silver/account_snapshots/` — Q1 output, partitioned by `snapshot_date`
- `s3://semestabank-gold/customer_health_scorecard/` — Q2(a) regulatory view
- `s3://semestabank-gold/fraud_detection_alerts/` — Q2(b) fraud alerts

MinIO console: [http://localhost:9001](http://localhost:9001) (login: minioadmin / minioadmin123)

## How to query

```bash
# Launch PySpark shell connected to MinIO
docker exec -it nb-spark /opt/spark/bin/pyspark \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true

# or
docker compose run --rm --entrypoint "" spark \
  /opt/spark/bin/pyspark \
  --master "local[*]" \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true

# Then in PySpark:
>>> scorecard = spark.read.parquet("s3a://semestabank-gold/customer_health_scorecard/")
>>> scorecard.filter("risk_flag = true").select("customer_id", "total_balance", "credit_utilization_pct").show()
```

Alternatively, the reference PostgreSQL SQL in `sql/` can be run against the PostgreSQL
container for comparison or ad-hoc exploration.

## Key design decisions (lakehouse edition)

1. **MinIO as the single source of truth.** Bronze, silver, and gold are all Parquet
  files in the same object store. No PostgreSQL dependency for the pipeline.
2. **Spark SQL for gold views.** The same CTEs, window functions (LAG, RANGE, ROWS),
  and conditional aggregation from the PostgreSQL SQL run on Spark with minor syntax
   adjustments (`DATE_TRUNC('MONTH', ...)`, `TO_JSON(NAMED_STRUCT(...))` instead of
   `jsonb_build_object(...)`, `COLLECT_SET` instead of `STRING_AGG`).
3. **Pipeline orchestration.** A single shell script chains all four jobs. In production,
  Airflow's DAG triggers each step and only proceeds on success.
4. **Portable SQL.** The PostgreSQL SQL in `sql/` and the Spark SQL in the `.py` jobs
  are semantically identical — including the month-over-month balance change, which both
   compute from the **real** prior-month-end balance in `silver.account_snapshots` (Q1's
   output), not a placeholder. The optimization strategy (materialized views vs
   overwrite-Parquet) is a storage backend choice, not a rewrite.
5. **Supplementary merchant_locations.** Created by `bronze_ingest.py` using real
  dataset `REF`* reference IDs mapped to Indonesian cities.

