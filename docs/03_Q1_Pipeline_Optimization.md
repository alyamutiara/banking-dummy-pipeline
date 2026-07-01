# 03 · Q1 — Pipeline Optimization

> **The task.** SemestaBank's nightly account-snapshot ELT fails intermittently, takes 4+ hours
> for only 8M rows, and produces inconsistent results. (a) Identify ≥6 problems with production
> impact + fixes. (b) Rewrite it production-grade with incremental loading, `snapshot_date`
> partitioning, secret management, DQ assertions, and idempotency. (c) Design orchestration so
> credit scoring is triggered within 30 min, with retries and on-call alerting.

**Code:** `[local/q1_pipeline/](../local/q1_pipeline/)` (local) · `[production/](../production/)` (AWS/Databricks)

---

## The original (broken) job

```python
accounts = spark.read.format("jdbc").option("url", "...").option("user","etl_user")\
    .option("password","etl_pass123").load()          # hardcoded creds, full table
txns = spark.read.format("jdbc")...option("dbtable","TRANSACTIONS").load()  # reads ALL txns
today_txns = txns.filter(txns.txn_date >= "2026-01-01")       # hardcoded date, no pushdown
snapshot = accounts.join(today_txns,"account_id","left")\
    .groupBy("account_id","customer_id","account_type","balance")\          # balance in groupBy
    .agg({"amount":"sum","txn_id":"count"})            # no dedup, no status filter
snapshot.write.mode("overwrite").parquet("s3://.../account_snapshots/")     # destroys history
```

## (a) Eight problems → impact → fix


| #   | Problem                              | Category              | Production impact                              | Fix                                                                                |
| --- | ------------------------------------ | --------------------- | ---------------------------------------------- | ---------------------------------------------------------------------------------- |
| 1   | Hardcoded `etl_pass123`              | **Security**          | Core-banking creds in Git → compliance breach  | Read from env / Secrets Manager / `dbutils.secrets`                                |
| 2   | Full `TRANSACTIONS` scan every night | **Performance**       | 4+ hrs, grows daily → missed SLA               | Incremental read of only the day's partition (JDBC pushdown + partition pruning)   |
| 3   | Hardcoded date `2026-01-01`          | **Correctness**       | Re-aggregates entire year-to-date nightly      | `resolve_snapshot_date()`: CLI > env > last partition+1 > max(txn_day) > yesterday |
| 4   | `overwrite` whole path               | **Reliability/audit** | No history, no time travel → fails OJK lineage | `partitionBy("snapshot_date")` + dynamic partition overwrite                       |
| 5   | `balance` inside `groupBy`           | **Correctness**       | Duplicate/again-split rows when balance varies | Aggregate first on `account_id`, then join accounts                                |
| 6   | No transaction dedup                 | **Correctness**       | Phantom money in dashboards                    | `dropDuplicates(["txn_id"])`                                                       |
| 7   | No status filter                     | **Correctness**       | PENDING/FAILED/REVERSED counted as real        | `filter(status == "COMPLETED")`                                                    |
| 8   | No data-quality gates                | **Governance**        | Silent bad data reaches regulators             | Row-count, null, non-negative, balance reconciliation + OJK gates                  |


Full write-up with code references: `[local/q1_pipeline/docs/PROBLEMS_AND_FIXES.md](../local/q1_pipeline/docs/PROBLEMS_AND_FIXES.md)`.

## (b) The rewrite — the five "musts"

The rewritten job is `[account_snapshot.py](../local/q1_pipeline/spark/jobs/account_snapshot.py)`. It is
modular (`Config`, `build_spark`, `resolve_snapshot_date`, `build_snapshot`, DQ functions,
`write_silver_snapshot`), logged, and exits non-zero on a DQ failure.


| Must                             | How it's met                                                                                                                                                                                 |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Incremental loading**          | Bronze transactions are partitioned by `txn_day`; the job reads only `txn_day=<snapshot_date>` — directory-level pruning, never a full scan                                                  |
| **Partition by `snapshot_date`** | `write.partitionBy("snapshot_date")` → each night is its own partition for time-travel/audit                                                                                                 |
| **Secret management**            | `Config` reads `os.environ`; nothing hardcoded. Prod uses `dbutils.secrets.get()` / Secrets Manager                                                                                          |
| **DQ assertions**                | `assert_row_count`, `assert_no_nulls`, `assert_non_negative`, `recon_balance` (fails if balance mismatch > 2%), plus OJK gates (KYC compliance, credit-score stability, risk/PD correlation) |
| **Idempotent**                   | `partitionOverwriteMode=dynamic` (local) / Delta `replaceWhere` (prod): re-running a date reproduces the same partition exactly                                                              |


**Core transform (correct version):**

```python
today_txns = today_txns.dropDuplicates(["txn_id"])            # fix #6
real = today_txns.filter(F.col("status") == "COMPLETED")      # fix #7
agg  = real.groupBy("account_id").agg(...)                    # fix #5 (no balance in key)
snapshot = accounts.join(agg, "account_id", "left").na.fill(0)\
    .withColumn("snapshot_date", F.lit(snap_date))            # fix #4 (partition col)
snapshot = snapshot.withColumn("computed_close_balance",
    F.col("balance") - F.col("credit_amount") + F.col("debit_amount"))  # reconciliation
```

The pipeline runs as a medallion flow (`pipeline.sh`): **bronze** (`bronze_ingest.py`: JDBC →
Parquet, incremental, PII-masked) → **silver** (`account_snapshot.py`) → **gold**
(`gold_scorecard.py`, `gold_fraud.py` = the Q2 views). A `backfill_snapshot.py` builds all
historical partitions in one pass.

## (c) Orchestration, SLA & alerting

`[account_snapshot_dag.py](../local/q1_pipeline/airflow/dags/account_snapshot_dag.py)` — Airflow DAG
`semestabank_lakehouse_pipeline`, nightly at 02:00:

```
start → bronze_ingest → run_account_snapshot(SLA=30m) → gold_scorecard → gold_fraud
                                                              → trigger_credit_scoring → done
[any task fails after 3 retries] ─────────────────────────────► alert_engineer_on_call → done
```

- **Triggers credit scoring** after the snapshot + gold succeed (`trigger_credit_scoring`,
`ALL_SUCCESS`).
- **Retries** each task up to **3×** with exponential backoff.
- **Alerts on-call** once retries are exhausted: Slack webhook → falls back to `email_on_failure`
→ falls back to `log.error`.
- **30-minute SLA** on the snapshot task protects the credit-scoring team's consumption window.

---

## ① Local vs ② Production


| Aspect      | ① Local                      | ② Production                                  |
| ----------- | ---------------------------- | --------------------------------------------- |
| Source      | PostgreSQL (Oracle stand-in) | Oracle JDBC / Lakehouse Federation            |
| Secrets     | `.env`                       | AWS Secrets Manager → `dbutils.secrets.get()` |
| Storage     | MinIO Parquet (`s3a://`)     | S3 **Delta** under Unity Catalog              |
| Idempotency | dynamic partition overwrite  | Delta `replaceWhere` + `OPTIMIZE … ZORDER`    |
| Compute     | Spark `local[*]` (Docker)    | Databricks Jobs / EMR Serverless              |
| Orchestrate | Airflow + DockerOperator     | Databricks Workflows / Step Functions / MWAA  |


