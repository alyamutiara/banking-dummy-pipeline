# 02 · Evaluation Criteria Map

The brief lists four evaluation criteria. This page maps each to the **exact artifact** that
satisfies it, so a reviewer can jump straight to the evidence.

## Pipeline Design — *clean, testable, production-ready ETL/ELT with error handling & logging*


| Requirement                            | Where                                                                                                                       | Evidence                                                                                |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Incremental loading                    | `local/q1_pipeline/spark/jobs/account_snapshot.py` → `read_incremental_transactions()`                                      | Reads only the `txn_day=<date>` partition (true partition pruning), not the whole table |
| No hardcoded secrets                   | `account_snapshot.py` → `Config` (env vars); prod `production/databricks/account_snapshot_job.py` → `dbutils.secrets.get()` | Q1 problem #1 fixed both locally and in production                                      |
| Partition by `snapshot_date`           | `write_silver_snapshot()` (`partitionBy`)                                                                                   | Time-travel / audit queries                                                             |
| Data-quality assertions                | `assert_row_count`, `assert_no_nulls`, `recon_balance`, plus OJK gates                                                      | Row-count + balance reconciliation + regulatory rules                                   |
| Idempotency                            | `spark.sql.sources.partitionOverwriteMode=dynamic` (local) / Delta `replaceWhere` (prod)                                    | Re-running a date overwrites only that partition                                        |
| Error handling & logging               | structured `logging` throughout; `DataQualityError` aborts with exit code 3                                                 | —                                                                                       |
| Testable                               | `local/q1_pipeline/tests/test_account_snapshot.py`                                                                          | 7 pytest cases (dedup, status filter, aggregation, reconciliation)                      |
| Orchestration + retries + SLA + alerts | `local/q1_pipeline/airflow/dags/account_snapshot_dag.py`                                                                    | 3 retries, 30-min SLA, Slack+email alert, triggers credit scoring                       |


## SQL Proficiency — *correct, performant queries; window functions, CTEs, optimization*


| Requirement                       | Where                                                                             | Evidence                                                                                             |
| --------------------------------- | --------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| CTEs                              | `local/q2_sql/sql/01_customer_health_scorecard.sql`                               | 7 named CTEs                                                                                         |
| Window functions — `LAG`          | scorecard CTE 3                                                                   | Month-over-month balance change off real history                                                     |
| Window functions — `RANGE`/`ROWS` | `local/q2_sql/sql/02_fraud_detection.sql`                                         | `RANGE … INTERVAL '1 hour'` (high-frequency) + `ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING` (anomaly) |
| PIVOT / conditional aggregation   | scorecard CTE 4                                                                   | `COUNT/SUM(CASE WHEN txn_type …)` and per-channel `AVG(CASE WHEN channel …)`                         |
| Optimization strategy + DDL       | `local/q2_sql/sql/03_optimization_strategy.sql` + `docs/OPTIMIZATION_STRATEGY.md` | Materialized view + unique index + `REFRESH … CONCURRENTLY`; incremental refresh                     |
| Verified correct                  | [Local Run Guide](06_Local_Run_Guide.md)                                          | Ran on 1.99M rows: 3,855 scorecard rows, fraud alerts produced                                       |


## Architecture Thinking — *warehouse design, medallion, scalability, trade-offs*


| Requirement                                         | Where                                | Evidence                                                                                                     |
| --------------------------------------------------- | ------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| Full architecture diagram + component justification | `architecture/ARCHITECTURE.md`       | ASCII diagram across sources→ingestion→storage→processing→serving→governance, each justified vs alternatives |
| 50K events/sec streaming design                     | `architecture/STREAMING_PIPELINE.md` | MSK (48 partitions) → Structured Streaming → Delta; throughput math                                          |
| Batch-stream join                                   | same                                 | Z-ORDER on `customer_id` + file skipping                                                                     |
| Hot/warm/cold storage                               | same                                 | S3 Standard → Standard-IA → Glacier IR → Deep Archive with costs                                             |
| Column-level lineage (Oracle→BI)                    | `architecture/GOVERNANCE_LINEAGE.md` | Unity Catalog auto-capture + OpenLineage fallback; ~6-line Q1 integration                                    |
| Budget within $50K                                  | `architecture/BUDGET_BREAKDOWN.md`   | $39K/mo itemized, $11K headroom                                                                              |
| Explicit trade-offs                                 | `architecture/PLATFORM_DECISIONS.md` | 11 component decisions, each with rejected alternatives                                                      |


## Explanation — *clear reasoning; thought process valued as much as the solution*


| Where                                                   | What                                                                                                          |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| [08 · Concepts & Deep Dives](08_Concepts_Deep_Dives.md) | Window functions, incremental loading, idempotency, secret management, lineage — the "why" behind each choice |
| Per-question docs (03/04/05)                            | Each states the sub-question, the decision, and the reasoning                                                 |
| Inline code comments                                    | Every job/SQL file documents *why*, not just *what*                                                           |
| `local/q1_pipeline/docs/PROBLEMS_AND_FIXES.md`          | The 8 problems with production impact + fix                                                                   |


> The original teaching-style write-ups (`Q1_Explanation/`, `Q2_Explanation/`, `Q3_Explanation/`
> at the repo root) remain available as optional deep-dive reading; the essential points are
> folded into this wiki.

