# 08 · Concepts & Deep Dives

## Incremental loading (why the 4-hour job becomes minutes)

The broken job read the **entire** `TRANSACTIONS` table every night, then filtered in Spark — so
work grew with history. The fix has two parts:

1. **Predicate pushdown at the source.** When reading from the DB, push `WHERE txn_date = <day>`
  *into* the query so only that day's rows cross the wire (`read_incremental_transactions`).
2. **Partition pruning in the lake.** Bronze transactions are written partitioned by `txn_day`, so
  the silver job filtering `txn_day=<date>` opens *only that directory* — never the others. This
   is directory-level pruning, strictly stronger than a filter on a non-partition column (which
   only skips row-groups inside files already opened).

Deeper: `Q1_Explanation/04_Incremental_Loading_and_Partitioning.md`.

## Idempotency (re-running a date is safe)

A nightly job *will* be re-run (retries, backfills, manual reruns). Idempotent = same inputs →
same output, no duplicates. Mechanism: **dynamic partition overwrite** (local Parquet) /
**Delta `replaceWhere`** (prod) — re-running `2026-02-14` replaces exactly that partition and
touches nothing else. Contrast the original `mode("overwrite")` on the whole path, which destroyed
all history. Deeper: `Q1_Explanation/07_Idempotency_Explained.md`.

## Secret management

Hardcoded `etl_pass123` = core-banking credentials in Git. Three escalating fixes, all used here:
env vars (local `.env`, never committed) → cloud secret store (AWS Secrets Manager) → platform
secrets (`dbutils.secrets.get()`). The code never sees a literal credential. Deeper:
`Q1_Explanation/05_Secret_Management_Explained.md`.

## Data-quality gates (fail fast, before regulators see it)

Bad data silently reaching an OJK report is worse than a failed job. The silver job asserts
row-count sanity, no null keys, non-negative counts, and **balance reconciliation**
(`balance ≈ balance − credit + debit`, fail if >2% mismatch), plus OJK-specific gates (KYC
compliance, credit-score stability <100-pt jumps, risk/PD correlation). A failure raises
`DataQualityError` and exits non-zero so the orchestrator stops the run. Deeper:
`Q1_Explanation/06_Data_Quality_Assertions_Explained.md`.

## Window functions — `RANGE` vs `ROWS`

The single most important SQL idea in Q2. A window function computes over a frame **without
collapsing rows**.

- `**RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW`** — the frame is a *time interval*.
Used for "5+ transactions in a rolling hour" (fraud pattern 1): the right tool when the window is
measured in time, not rows.
- `**ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING`** — the frame is a *fixed count of rows*, and
excluding the current row keeps an anomalous transaction from inflating its own baseline (fraud
pattern 3).
- `**LAG(total_balance) OVER (PARTITION BY customer ORDER BY month)`** — reach back one row for the
month-over-month delta (scorecard).

Deeper: `Q2_Explanation/04_Window_Functions_Explained.md`.

## PIVOT vs conditional aggregation

The scorecard needs txn counts per type and avg amount per channel as *columns*. Rather than a
dialect-specific `PIVOT`, it uses `COUNT/SUM/AVG(CASE WHEN … )` — portable across Postgres, Spark,
and Snowflake, and it does the whole pivot in **one pass** with no self-joins. Deeper:
`Q2_Explanation/05_PIVOT_vs_Conditional_Aggregation.md`.

## Medallion as contracts (not just folders)

Bronze = raw + immutable (replayable source of truth). Silver = cleaned + conformed + deduped
(Q1's `account_snapshots`). Gold = business-owned, query-ready (Q2's scorecard + fraud). Each layer
is a *contract*: downstream consumers depend on silver/gold shapes, so changes are governed.
Deeper: `Q3_Explanation/02_Medallion_in_Production.md`.

## Lakehouse vs warehouse vs mesh

Why a **lakehouse** (Databricks) over a pure warehouse (Snowflake) or a data mesh: SemestaBank is
mid-scale with a single ~8-person platform team and a hard requirement to unify **ML + BI +
lineage**. A lakehouse gives one storage layer (S3/Delta) serving Spark ML, SQL BI, and streaming,
with Unity Catalog lineage — without copying data into a separate warehouse. A mesh's federated
domain ownership is overkill at this size. Deeper:
`Q3_Explanation/01_Lakehouse_vs_Warehouse_vs_Mesh.md`.

## Column-level lineage (the OJK driver)

OJK wants to trace any customer-facing metric back to its Oracle source columns. The design gets
this *automatically* from Unity Catalog for in-platform work, fills gaps with OpenLineage
(Airflow/Airbyte/BI), and pulls Oracle source columns in via Lakehouse Federation — so the answer
is "≈6 lines of integration", not "build a lineage system". Deeper:
`architecture/GOVERNANCE_LINEAGE.md` and `Q3_Explanation/05_Column_Level_Lineage.md`.

## Budget engineering

Cost is an architecture concern, not an afterthought. The big lever isn't shaving instance
sizes — it's **not buying** a second BI warehouse, a separate lineage tool, a feature store, or a
real-time DB, because one Databricks Premium platform includes them. That's how the design lands at
$39K of $50K with headroom. Deeper: `architecture/BUDGET_BREAKDOWN.md`,
`Q3_Explanation/06_Budget_Engineering.md`.