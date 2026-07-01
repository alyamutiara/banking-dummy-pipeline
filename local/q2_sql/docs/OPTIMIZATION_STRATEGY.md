# Optimization Strategy for Nightly Materialization

## The problem

The two gold-layer views (`customer_health_scorecard`, `fraud_detection_alerts`) are
materialized nightly. The current refresh takes **45 minutes**. SemestaBank needs it
faster — the credit-scoring team has a 30-minute SLA (from Q1) and the regulator needs
reports before 08:00.

## Platform context

The scenario specifies Oracle core banking as the source and Databricks as the ML/BI
platform (see Q3). This Q2 question asks us to evaluate three materialization options for
the nightly refresh **within whichever platform SemestaBank chooses for regulatory
reporting**. The analysis below covers PostgreSQL materialized views (the option the
question lists that requires zero new tooling) and then bridges to the Q3 target
platform, where the same SQL becomes a Databricks Delta Live Tables pipeline — **a
one-line translation**.

| Option | Best for | SemestaBank fit? | Complexity |
|--------|----------|-----------------|------------|
| **Snowflake dynamic tables** | Already on Snowflake | ❌ SemestaBank is not on Snowflake. Adding it just for two views is a $20K+/month platform migration. Not justified. | Low (Snowflake-managed) |
| **dbt incremental models** | 20+ models, team uses dbt | Maybe future, not now. dbt adds an orchestration dep + a new tool for the team to learn. Overkill for 2 views. | Medium |
| **PostgreSQL materialized views** | Current stack, simple, portable | ✅ Zero new tools. Matviews with `REFRESH CONCURRENTLY` is the simplest thing that works if regulatory reporting stays on the existing OLAP database. | Low |
| **Databricks DLT** (Q3 target platform) | Already adopting Databricks for ML | ✅ If SemestaBank standardizes on Databricks (Q3's recommendation), the same SQL becomes a `CREATE OR REFRESH STREAMING TABLE` — one line. | Low |

### My recommendation

**Short answer:** Use the option that matches the production platform. If regulatory
reporting stays on the current OLAP database, **PostgreSQL materialized views with
`REFRESH CONCURRENTLY`** is the simplest path. If SemestaBank standardizes on Databricks
(as Q3 recommends), the same SQL body becomes a **Delta Live Tables** pipeline — zero
rewrite, shown below.

**Why PostgreSQL matviews work today:**
1. **No new tool.** A materialized view is a built-in database feature — no platform migration.
2. **Non-blocking reads.** `REFRESH CONCURRENTLY` lets BI dashboards query the matview
   while the refresh runs — no downtime window.
3. **The query is already optimized.** Each view only scans the current month's
   transactions (the `WHERE txn_date >= DATE_TRUNC('month', CURRENT_DATE)` predicate),
   not the full 60M-row history. This alone cuts the 45-minute runtime to minutes.
4. **Idempotent.** Re-refreshing replaces the matview content — same result every time,
   no duplicates.

**Why the DLT path is the production target (Q3):**
- Q3 recommends Databricks + Unity Catalog as the enterprise data platform. In that
  target architecture, the matview translation is literally one line:
  ```sql
  CREATE OR REFRESH STREAMING TABLE gold.customer_health_scorecard
    AS SELECT ... ;  -- same body as the view
  ```
- DLT handles incremental refresh, schema evolution, and lineage automatically. The
  `REFRESH CONCURRENTLY` concern (non-blocking reads) disappears — Delta readers always
  see the latest committed version without locks.

**Why not dbt incremental:**
- dbt incremental models shine when you have 20-50 models with shared macros, tests, and
  documentation. For two views it's overkill — you'd introduce dbt-core, a dbt profile, a
  CI pipeline, and team training just to replace `REFRESH MATERIALIZED VIEW`.
- *If* SemestaBank later grows to 20+ regulatory views, dbt would be the right next step.
  The SQL in these views is already dbt-compatible (standard CTEs, no PG-specific syntax).

**Why not Snowflake dynamic tables:**
- SemestaBank is not on Snowflake. Introducing it for two views is a platform migration,
  not an optimization.
- *If* SemestaBank later migrates to Snowflake, the same `SELECT ...` body becomes a
  `CREATE DYNAMIC TABLE ... TARGET_LAG = '1 hour'` with zero rewrite.

## The optimization, step by step

### Step 1: The view queries are already incremental

Look at `01_customer_health_scorecard.sql`:
```sql
WHERE t.txn_date >= DATE_TRUNC('month', CURRENT_DATE)
  AND t.txn_date <  DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
```
This predicate means the query only scans **this month's** transactions. For a monthly
scorecard, last month's data hasn't changed — no need to re-aggregate it. At 60M
transactions/year, a single month is ~5M rows — manageable in seconds, not minutes.

### Step 2: Materialize the result (don't compute on every query)

```sql
CREATE MATERIALIZED VIEW gold.mv_customer_health_scorecard AS
    SELECT * FROM gold.customer_health_scorecard
WITH DATA;
```

BI dashboards query `mv_customer_health_scorecard` (a static table on disk), not the view
(which would re-run the full query every time). The matview computes once, reads many times.

### Step 3: Index the matview

```sql
CREATE UNIQUE INDEX idx_mv_scorecard_customer_month
    ON gold.mv_customer_health_scorecard (customer_id, scorecard_month);

CREATE INDEX idx_mv_scorecard_risk_flag
    ON gold.mv_customer_health_scorecard (risk_flag)
    WHERE risk_flag = TRUE;        -- partial index: only risky customers
```

The unique index serves double duty: fast lookups by `(customer_id, month)` and
**enables `REFRESH CONCURRENTLY`** (PG requires a unique index for concurrent refresh).

### Step 4: Refresh concurrently (non-blocking)

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_customer_health_scorecard;
```

`CONCURRENTLY` means:
- BI dashboards keep reading the *old* matview while the new one is built.
- When the refresh finishes, PG atomically swaps the old for the new.
- **Zero downtime.** No "dashboard is broken during refresh" window.

### Step 5: Orchestrate the refresh

The refresh runs nightly **after** Q1's silver snapshot is ready. In Airflow:

```
run_account_snapshot (Q1) → REFRESH CONCURRENTLY scorecard → REFRESH CONCURRENTLY fraud → done
```

This chains the dependency: Q1's output feeds Q2's views, and Q2's matviews refresh only
after the upstream data is ready.

## Expected performance

| | Before (plain view) | After (matview + concurrent refresh) |
|---|---|---|
| Query time (BI dashboard) | 45 min (full recompute every time) | < 1 sec (reads from disk) |
| Refresh time (nightly) | 45 min | 2-5 min (scans current month only) |
| Downtime during refresh | Full table locked | None (concurrent) |
| Cost | Existing database (built-in) | $0 additional (DLT included in Databricks Premium) |

## The same SQL on other platforms

The view body is portable SQL — the materialization strategy is a config choice, not a
rewrite. In each platform it becomes a one-liner:

**Snowflake:**
```sql
CREATE DYNAMIC TABLE gold.customer_health_scorecard
  TARGET_LAG = '1 hour'
  WAREHOUSE = wh_regulatory
  AS SELECT ... [same body as the view];
```
Snowflake handles incremental refresh automatically. You just declare the lag you can
tolerate and it computes deltas.

**Databricks (Delta Live Tables):**
```sql
CREATE OR REFRESH STREAMING TABLE gold.customer_health_scorecard
  AS SELECT ... FROM STREAM(bronze.accounts) JOIN STREAM(bronze.transactions)...;
```

**dbt (if they later adopt dbt):**
```sql
{{ config(materialized='incremental', unique_key='customer_id,scorecard_month') }}
SELECT ... FROM bronze.accounts
{% if is_incremental() %}
  WHERE txn_date >= (SELECT MAX(scorecard_month) FROM {{ this }})
{% endif %}
```

All three use the *same core SQL*. The materialization strategy is a config choice, not a
rewrite. That's the portability advantage of keeping the view logic clean.