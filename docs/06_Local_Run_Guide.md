# 06 · Local Run Guide (Condition ①)

Run and verify the whole assessment on a laptop — no cloud account, no cost. The stack is
PostgreSQL (Oracle stand-in) + MinIO (S3 stand-in) + Spark + Airflow, all in Docker.

## Prerequisites

- **Docker** + Docker Compose (Docker Desktop is fine). ~4 GB free RAM for Spark.
- That's it. `psql` is **not** required on the host — we exec into the Postgres container.

## Dataset note (important)

The 237 MB `semestabank_dataset/` (the CSV inputs, mostly the 187 MB `transactions.csv`) is **not**
copied into `Final_Answer/` — it stays at the **repo root**. The local compose mounts it by
relative path (`../../../semestabank_dataset`). Keep `Final_Answer/` next to `semestabank_dataset/`
(as it is in the repo) and everything resolves automatically.

---

## Part A — Q1 pipeline (bronze → silver → gold)

```bash
cd Final_Answer/local/q1_pipeline
cp .env.example .env                  # local secrets (MinIO + Postgres); never committed

# 1) Start the platform: Postgres (auto-loads ~2M txns via seed/init.sql),
#    MinIO, and the bucket initializer (creates bronze/silver/gold).
docker compose up -d

# 2) Wait ~1-2 min for Postgres to finish COPYing the dataset, then verify:
docker exec nb-postgres psql -U etl_user -d semestabank -c \
  "SELECT count(*) FROM bronze.transactions;"      # expect 1991349

# 3) Run the full pipeline once (bronze ingest → silver snapshots → gold scorecard + fraud).
#    Builds the Spark image on first run (downloads hadoop-aws/postgres JARs), then runs.
#    By default this uses backfill_snapshot.py (all historical dates, one pass)
#    because gold_scorecard.py needs prior-month partitions for MoM balance.
docker compose --profile run up spark

# To run the incremental (single-date) silver job instead (for nightly simulations):
# docker compose --profile run -e SILVER_MODE=incremental up spark

# 4) Inspect the silver + gold Parquet written to MinIO:
docker compose run --rm spark \
  /opt/spark/bin/spark-submit --master local[*] /opt/spark/jobs/query_silver.py
```

What each stage does (code in `[spark/jobs/](../local/q1_pipeline/spark/jobs/)`):


| Stage  | Job                              | Output                                                                                                                                                                                                                 |
| ------ | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Bronze | `bronze_ingest.py`               | Postgres → MinIO Parquet; incremental transactions (`txn_day`), PII masked, merchant_locations (hash-based)                                                                                                            |
| Silver | `backfill_snapshot.py` (default) | ALL historical dates in one pass → all `snapshot_date` partitions. Needed by `gold_scorecard.py` for MoM balance comparison across months.                                                                             |
| Silver | `account_snapshot.py` (nightly)  | Single-date incremental snapshot; deduped, status-filtered, DQ-gated. Use after the initial backfill.                                                                                                                  |
| Gold   | `gold_scorecard.py`              | Reads bronze + silver → `customer_health_scorecard` (Q2a). **Depends on silver:** the MoM balance LAG reads prior-month-end data from `silver.account_snapshots` — if you skip the backfill, MoM metrics are all NULL. |
| Gold   | `gold_fraud.py`                  | Reads bronze only → `fraud_detection_alerts` (Q2b). No silver dependency; can run standalone.                                                                                                                          |


**Why backfill before gold matters:** `gold_scorecard.py` CTE `prev_month_balance` filters `silver.account_snapshots` for dates in the prior month and takes the latest per account via `ROW_NUMBER`. If we only run the single-date `account_snapshot.py`, those prior-month partitions don't exist → the CTE returns 0 rows → `mom_balance_change_pct` and the `risk_flag` (balance drop >30% MoM) are silently broken. The backfill creates every historical partition gold needs.

**Run single stages manually:**

```bash
# Bronze only
docker compose run --rm spark /opt/spark/bin/spark-submit --master local[*] \
  /opt/spark/jobs/bronze_ingest.py

# Silver — historical backfill (all dates, initial load)
docker compose run --rm spark /opt/spark/bin/spark-submit --master local[*] \
  /opt/spark/jobs/backfill_snapshot.py

# Silver — incremental (single date, nightly)
docker compose run --rm spark /opt/spark/bin/spark-submit --master local[*] \
  /opt/spark/jobs/account_snapshot.py --date 2026-02-14

# Gold (run after silver)
docker compose run --rm spark /opt/spark/bin/spark-submit --master local[*] \
  /opt/spark/jobs/gold_scorecard.py
docker compose run --rm spark /opt/spark/bin/spark-submit --master local[*] \
  /opt/spark/jobs/gold_fraud.py
```

**MinIO console:** [http://localhost:9001](http://localhost:9001) (`minioadmin` / `minioadmin123`).

### Orchestration (optional)

```bash
docker compose --profile orchestrate up airflow   # Airflow UI on http://localhost:8080
```

DAG `semestabank_lakehouse_pipeline`: bronze → silver (30-min SLA) → gold → trigger credit scoring;
3 retries; on-call alert after retries exhausted.

---

## Part B — Q2 regulatory SQL (PostgreSQL-native reference)

Part B is a **self-contained PostgreSQL-only path** that runs the Q2 views directly against  
Postgres bronze tables — no Spark, no MinIO.

Part A and Part B produce **equivalent results** through different engines. We can run either, both, or B first then A to compare output.

The PostgreSQL seeds exist because the views need tables the source schema doesn't have:


| Seed                           | Why it's needed                                                                                                                                                                                                                                                                                   |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `gold_setup.sql`               | Creates the `gold` schema. Postgres needs it before `CREATE VIEW gold.…`                                                                                                                                                                                                                          |
| `silver_account_snapshots.sql` | Q1 stand-in: reconstructs each account's prior-month-end balance from real txn history. The scorecard view's `prev_month_balance` CTE reads from `silver.account_snapshots` — without this seed, the MoM `LAG` has no data. (In Part A, `backfill_snapshot.py` produces the equivalent in MinIO.) |
| `merchant_locations.sql`       | Hash-assigns every `reference_id` from `bronze.transactions` to one of 10 Indonesian cities. The fraud view's `MULTI_CITY` pattern joins on this. (In Part A, `bronze_ingest.py` produces the equivalent in MinIO.)                                                                               |


```bash
cd Final_Answer/local/q2_sql

# Seeds: gold schema, silver.account_snapshots (Q1 stand-in), merchant_locations
docker exec -i nb-postgres psql -U etl_user -d semestabank < seed/gold_setup.sql
docker exec -i nb-postgres psql -U etl_user -d semestabank < seed/silver_account_snapshots.sql
docker exec -i nb-postgres psql -U etl_user -d semestabank < seed/merchant_locations.sql

# Views
docker exec -i nb-postgres psql -U etl_user -d semestabank < sql/01_customer_health_scorecard.sql
docker exec -i nb-postgres psql -U etl_user -d semestabank < sql/02_fraud_detection.sql

# Query them
docker exec nb-postgres psql -U etl_user -d semestabank -c \
  "SELECT count(*) FILTER (WHERE risk_flag) AS risk_rows FROM gold.customer_health_scorecard;"
docker exec nb-postgres psql -U etl_user -d semestabank -c \
  "SELECT alert_type, count(*) FROM gold.fraud_detection_alerts GROUP BY alert_type;"

# Optional: materialized views + non-blocking refresh (the Q2f optimization)
docker exec -i nb-postgres psql -U etl_user -d semestabank < sql/03_optimization_strategy.sql
docker exec nb-postgres psql -U etl_user -d semestabank -c \
  "REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_customer_health_scorecard;"
```

---

## ✅ Verified results (run during this build)

The Q2 path was executed end-to-end against the **full 1.99M-row** dataset on local Postgres:

```
bronze.transactions ........................ 1,991,349 rows loaded
seed silver.account_snapshots .............. 7,108 rows / 7,108 accounts
seed bronze.merchant_locations ............. full coverage (every distinct reference_id → 1 of 10 cities)
gold.customer_health_scorecard ............. 3,855 rows (1,150 risk-flagged)
gold.fraud_detection_alerts ................ all three patterns fire (HIGH_FREQUENCY · MULTI_CITY · AMOUNT_ANOMALY)†
MinIO buckets .............................. semestabank-bronze / -silver / -gold created
docker compose config ...................... valid
```

---

## Teardown

```bash
cd Final_Answer/local/q1_pipeline
docker compose --profile run --profile orchestrate down -v   # stops + removes volumes
```

## Troubleshooting


| Symptom                         | Fix                                                                                          |
| ------------------------------- | -------------------------------------------------------------------------------------------- |
| Postgres count errors / 0 rows  | `init.sql` still COPYing — wait, re-check; large `transactions.csv` takes a minute           |
| `transactions.csv` not found    | run from `Final_Answer/local/q1_pipeline`; keep `Final_Answer/` beside `semestabank_dataset/` |
| Port 5433/9000/9001/8080 in use | edit the `ports:` in `docker-compose.yml`                                                    |
| Spark S3A errors                | confirm `minio` + `minio-init` are healthy (`docker compose ps`) before running `spark`      |


