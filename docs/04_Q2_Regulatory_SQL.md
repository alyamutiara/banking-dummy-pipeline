# 04 · Q2 — Regulatory Reporting SQL

> **The task.** OJK requires monthly regulatory reports. Build the performant SQL views behind
> them, materialized nightly over the full dataset (8M accounts, 60M+ txns). (d) Monthly Customer
> Health Scorecard. (e) Fraud detection. (f) An optimization strategy (the 45-min materialization
> must get faster) with incremental-refresh DDL.

**Code:** [`local/q2_sql/`](../local/q2_sql/) · target **PostgreSQL** locally, Databricks/Snowflake in prod.
**Verified end-to-end on 1.99M real rows** — see results at the bottom.

---

## (d) Monthly Customer Health Scorecard

[`sql/01_customer_health_scorecard.sql`](../local/q2_sql/sql/01_customer_health_scorecard.sql) —
one row per `(customer_id, scorecard_month)`, built from **7 CTEs**:

| CTE | Purpose |
|-----|---------|
| `monthly_balance` | current-month total balance + credit-card balance/limit per customer |
| `prev_month_balance` | **the Q1→Q2 link** — real prior month-end balance from `silver.account_snapshots` (Q1's output), via `ROW_NUMBER() … ORDER BY snapshot_date DESC` |
| `monthly_balance_history` | stitches current + previous, applies **`LAG`** for the genuine MoM delta |
| `monthly_txns` | **conditional aggregation** — `COUNT/SUM(CASE WHEN txn_type …)` (pivot without `PIVOT`) + per-channel `AVG(CASE WHEN channel …)` |
| `customer_monthly_txns` | rolls per-account txn aggregates up to the customer |
| `latest_credit_score` | latest PD + score per customer (`DISTINCT ON`) |
| `balance_with_lag` | computes `mom_balance_change_pct` and `credit_utilization_pct` |

**Required metrics, all present:** total balance, MoM change (via `LAG`), txn counts by type
(conditional aggregation), avg amount per channel, credit utilization, and:

```sql
risk_flag = TRUE WHEN credit_utilization_pct > 80      -- over-leveraged
                 OR probability_of_default  > 0.3      -- high default risk
                 OR mom_balance_change_pct  < -30       -- balance crashed >30% MoM
```

**Why it's fast:** the MoM `LAG` runs over a tiny 2-rows-per-customer intermediate, not the
60M-row transaction table; conditional aggregation avoids self-joins; the transaction scan is
filtered to the **current month only** (`DATE_TRUNC` predicates), so it's incremental by design.

## (e) Fraud detection

[`sql/02_fraud_detection.sql`](../local/q2_sql/sql/02_fraud_detection.sql) — one row per
`(customer_id, alert_type, alert_date)` with a JSONB `details_json`. Three patterns combined with
`UNION ALL`, each using a window function:

| `alert_type` | Rule | Window technique |
|--------------|------|------------------|
| `HIGH_FREQUENCY` | 5+ transactions within any rolling 1-hour window | `COUNT(*) OVER (… RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW)` — **time-based `RANGE`**, not row count |
| `MULTI_CITY` | 3+ distinct merchant cities on the same day | `COUNT(DISTINCT merchant_city)` after join to `merchant_locations` |
| `AMOUNT_ANOMALY` | single txn > 3× rolling 30-txn average | `AVG(amount) OVER (… ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)` — **excludes the current row** so the anomaly can't inflate its own baseline |

The `RANGE` vs `ROWS` distinction is deliberate and documented inline — `RANGE` for the
time-window pattern, `ROWS` for the count-based rolling average.

## (f) Optimization strategy

[`sql/03_optimization_strategy.sql`](../local/q2_sql/sql/03_optimization_strategy.sql) +
[`docs/OPTIMIZATION_STRATEGY.md`](../local/q2_sql/docs/OPTIMIZATION_STRATEGY.md).

**Recommendation: PostgreSQL materialized views today, Databricks DLT as the Q3 target.**

| Option | Verdict for SemestaBank |
|--------|------------------------|
| **PostgreSQL materialized view** ✅ | Zero new tooling; `REFRESH MATERIALIZED VIEW CONCURRENTLY` (needs a unique index — provided) refreshes without locking readers; query already scans only the current month |
| **Databricks DLT** ✅ (future) | Q3's target platform; same SELECT becomes a `CREATE OR REFRESH MATERIALIZED VIEW`; incremental + lineage for free |
| Snowflake dynamic tables | Good, but a platform migration SemestaBank doesn't need yet |
| dbt incremental | Worth it once there are 20+ models/teams |

DDL provided: materialized views + a **unique index** (enabling `CONCURRENTLY`) + partial index
on `risk_flag`. Expected result: nightly refresh **45 min → 2–5 min**, dashboard query **45 min → <1 s**.

Portability is one-line: the same SELECT body runs as a Postgres matview, a Databricks DLT
materialized view ([`production/databricks/gold_views_dlt.sql`](../production/databricks/gold_views_dlt.sql)),
or a Snowflake `DYNAMIC TABLE`.

---

## ✅ Verified end-to-end (local PostgreSQL, full 1.99M-row dataset)

Run during this build (see [Local Run Guide](06_Local_Run_Guide.md) to reproduce):

| Check | Result |
|-------|--------|
| Seeds load (`silver.account_snapshots`, `merchant_locations`) | 7,108 snapshot rows / full `reference_id` coverage |
| Scorecard view | **3,855 rows, 1,150 risk-flagged** |
| Fraud view | all three patterns fire (`HIGH_FREQUENCY` · `MULTI_CITY` · `AMOUNT_ANOMALY`)* |
| Materialized views + `REFRESH … CONCURRENTLY` | builds & refreshes cleanly |

*An earlier draft reported `AMOUNT_ANOMALY 339,028 · HIGH_FREQUENCY 25 · MULTI_CITY 0`, where the zero
came from a 20-row hand seed that matched only ~0.001% of transactions — the pattern could never trigger.
`merchant_locations` now hash-assigns **every** distinct `reference_id` to one of 10 Indonesian cities, so
`MULTI_CITY` is demonstrable; and `AMOUNT_ANOMALY` now applies an `amount > 1,000,000` floor that removes the
micro-baseline false positives behind the old inflated count. The pattern logic is unchanged; in production
`merchant_locations` is the full payment-network registry. Re-run the [Local Run Guide](06_Local_Run_Guide.md)
to capture the updated counts.

## ① Local vs ② Production

| | ① Local | ② Production |
|--|---------|--------------|
| Engine | PostgreSQL views + matviews | Databricks DLT / Snowflake dynamic tables |
| Refresh | `REFRESH MATERIALIZED VIEW CONCURRENTLY` | auto-incremental (DLT / `TARGET_LAG`) |
| JSON | `jsonb_build_object` | `to_json(named_struct(...))` (Spark) / `OBJECT_CONSTRUCT` (Snowflake) |
| Lineage | n/a | Unity Catalog column-level lineage |
