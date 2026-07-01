# Task 3 — Column-Level Lineage (DGQA Integration)

> **Question:** The DGQA team needs **column-level lineage** from source (Oracle) to
> consumption (BI dashboard). How would you instrument pipelines to enable this? How does
> this integrate with the Q1 pipeline?

## TL;DR

- **Unity Catalog** captures lineage automatically for every Spark/SQL transform on
  Databricks — no code changes needed in Q1's pipeline.
- For non-Databricks pieces (Airflow, Airbyte, official Oracle JDBC reads), we layer
  **OpenLineage** events on top. They flow to a Marquez/Atlas backend that the DGQA
  team uses for end-to-end lineage.
- Q1's pipeline changes are **3 lines** of init code and **zero lines** of job code.

## The lineage picture the DGQA team wants

```
Oracle core banking               Databricks data platform            BI dashboard
─────────────────                  ─────────────────────────          ──────────────

accounts.balance            ──►   silver.account_snapshots     ──►   gold.customer_health_scorecard    ──►   "Customer Risk" tile
(accounts column)                 .current_balance                    .month_end_balance                     (Tableau tile column)
                                  (Delta column)                      (Delta column)                        (BI column)

customer.national_id        ──►   silver.account_snapshots     ──►   gold.customer_health_scorecard    ──►   (hidden — PII mask)
(customers column)               .national_id_masked                 .national_id_masked

transactions.amount         ──►   silver.account_snapshots     ──►   gold.customer_health_scorecard    ──►   "Total Txn" tile
(transactions column)            .total_debit_amount                  .total_debit_amount
```

The DGQA team must answer, for **every column** on the BI dashboard, which Oracle column
fed it, and every transform in between.

## Three layers of lineage instrumentation

```
                  ┌─────────────────────────────────────────────────────┐
                  │                DGQA Lineage Plane                     │
                  │                                                       │
                  │   ┌──────────────────────────────────────────┐       │
                  │   │  Unity Catalog (automatic, primary)       │       │
                  │   │  Spark/SQL on Databricks → no code         │       │
                  │   │  Captures column-level: source/transform/ │       │
                  │   │  sink                                    │       │
                  │   └─────────────────┬────────────────────────┘       │
                  │                     │                                │
                  │   ┌─────────────────┴────────────────────────┐       │
                  │   │  OpenLineage (events, fallback)            │       │
                  │   │  Airflow, Airbyte, custom Python           │       │
                  │   │  Captures job/run-level lineage             │       │
                  │   └─────────────────┬────────────────────────┘       │
                  │                     │                                │
                  │   ┌─────────────────┴────────────────────────┐       │
                  │   │  BI connector instrumentation             │       │
                  │   │  Tableau CWM, Looker LookML, Power BI    │       │
                  │   │  dataset metadata → sends column usage   │       │
                  │   │  events back to Marquez                  │       │
                  │   └────────────────────────────────────────────┘       │
                  └─────────────────────────────────────────────────────┘
```

## Layer 1 — Unity Catalog (automatic, no-code)

Every Databricks Spark/SQL job automatically creates lineage edges in Unity Catalog. The
DGQA team just queries `information_schema.column_lineage`:

```sql
SELECT
    source_catalog, source_schema, source_table, source_column,
    sink_catalog,   sink_schema,   sink_table,   sink_column,
    transformation_text
FROM system.information_schema.column_lineage
WHERE sink_table LIKE '%customer_health_scorecard%'
ORDER BY source_schema, sink_schema;
```

Returns something like:
```
src=oracle.bronze.accounts.balance         → snk=silver.account_snapshots.current_balance
src=silver.account_snapshots.current_balance → snk=gold.customer_health_scorecard.month_end_balance
src=gold.customer_health_scorecard.month_end_balance → snk=bi_dashboard."Customer Risk".y_axis_value
```

This is the **automatic** layer — it required zero code from us in Q1. As soon as Q1's
`account_snapshot.py` writes the Delta table, Unity Catalog records the lineage.

## Layer 2 — OpenLineage (for non-Databricks pipelines)

Unity Catalog doesn't see Airflow DAG dependencies, Airbyte connector runs, or external
Oracle tables that aren't yet federated. We instrument those with **OpenLineage**:

### 2a. Airflow → OpenLineage (Q1's pipeline)

In MWAA, set two environment variables:

```bash
AIRFLOW__OPENLINEAGE__ENABLED=true
AIRFLOW__OPENLINEAGE__TRANSPORT={"type": "http", "url": "https://marquez.semestabank.id"}
```

Add a dataset producer in Q1's DAG (literally 3 lines):

```python
# in account_snapshot_dag.py
from openlineage.airflow import OpenLineageOperator  # already in Airflow 2.7+

# at the end of the snapshot task:
emit_lineage(
    inputs=[Dataset(namespace="oracle", name="bronze.accounts"),
            Dataset(namespace="oracle", name="bronze.transactions")],
    outputs=[Dataset(namespace="s3://semestabank-silver",
                     name="silver.account_snapshots",
                     facets={"schema": {...current columns}})]
)
```

That emission captures the **DAG → Dataset → downstream DAG** link. The DGQA team now sees
"this DAG consumed `bronze.accounts` and produced `silver.account_snapshots`."

### 2b. Airbyte → OpenLineage

Airbyte has built-in OpenLineage emission for all connectors. Configure the destination
URL once in Airbyte's connection settings. Zendesk and Braze lineage flows automatically
to Marquez.

### 2c. Microsoft Power BI / Tableau → OpenLineage (BI consumption)

When a BI tool reads `gold.customer_health_scorecard`, we want to know **which dashboard
tile used which column**.

- **Tableau Catalog** (Tableau Server) exposes its catalog via the Metadata API. A small
  scheduled job in Airflow pulls the catalog and emits column-usage events to OpenLineage
  with the dashboard and tile as the "sink".
- **Power BI**: similar via Power BI REST API (`datasets` + `reports` endpoints).
- **Looker**: LookML itself is the manifest; a `lkml` parser emits the lineage event once
  per model change.

These emissions add `consumed_at`, `consumer_application`, `consumer_user` facets to the
column's lineage — the last hop from "data" to "dashboard."

## Layer 3 — Lakehouse Federation (lineage for Oracle reads)

Unity Catalog might not natively see the Oracle source tables if Q1's pipeline reads Oracle
directly via JDBC. We solve this with **Databricks Lakehouse Federation**: register Oracle
as a **foreign catalog** in Unity Catalog.

```sql
CREATE CONNECTION oracle_core
  TYPE ORACLE
  OPTIONS (host 'core-db.semestabank.id', port 1521);

CREATE FOREIGN CATALOG core_banking
  CONNECTION oracle_core
  OPTIONS (database 'BANKING');
```

After this, Unity Catalog sees the Oracle tables as if they were Delta tables. The
column-level lineage includes **the Oracle source column** as the very first edge — no
manual mapping needed. The DGQA team sees the full path:

```
Oracle.banking.accounts.balance
  → Databricks.spark_job "account_snapshot" (Q1)          [Unity Catalog — automatic]
    → Unity Catalog silver.account_snapshots.current_balance
      → Databricks.dlt_pipeline "scorecard" (Q2)          [Unity Catalog — automatic]
        → Unity Catalog gold.customer_health_scorecard.month_end_balance
          → BI dashboard "Customer Risk" tile "y_axis"    [Tableau Metadata API — separate connector]
```

Five of six edges are **fully automatic** via Unity Catalog (edges 1–5: Oracle → Spark →
silver → DLT → gold). The sixth edge (gold → BI dashboard tile) is **not** captured by Unity
Catalog — column lineage generally stops at the Databricks SQL boundary. We close this last
hop with the **Tableau Metadata API** (or Power BI REST API), which we already cover in
Layer 2c above. The BI-connector job emits the column-to-tile mapping as an OpenLineage
event that Marquez stitches into the unified graph. So the end-to-end lineage is complete,
but it's "5 automatic + 1 connector-instrumented," not "6 automatic."

## Layer 4 — PII masking tied to lineage (the OJK bonus)

Unity Catalog tags let you label a column as PII and auto-mask it for unprivileged roles.
Lineage still flows through masked columns — the masking is a read-time decoration, not
a data rewrite:

```sql
ALTER TABLE silver.account_snapshots
  COLUMN national_id SET TAGS ('pii' = 'true');

CREATE MASKING FUNCTION mask_national_id (val STRING)
  RETURN CASE WHEN is_member('regulatory_team') THEN val
              ELSE CONCAT(LEFT(val, 4), '********', RIGHT(val, 2)) END;

ALTER TABLE silver.account_snapshots
  COLUMN national_id SET MASK mask_national_id;
```

The audit log shows **who accessed what**, **when**, and **with what mask** — directly
answering the OJK directive's "auditability" requirement.

## Integration with Q1's pipeline — the diff list

Q1's pipeline is already written (in `account_snapshot.py` and `account_snapshot_dag.py`).
What changes in Q3 production?

| Q1 component | Change to make lineage work | LoC |
|---|---|---|
| `account_snapshot.py` PySpark job | **Nothing.** As soon as it's run on Databricks, Unity Catalog captures the lineage. | 0 |
| `account_snapshot_dag.py` Airflow DAG | Add `OpenLineageOperator` at start of the snapshot task (or rely on Airflow's automatic dataset emission in 2.7+). Set `MARQUEZ_URL` in MWAA env. | 3 |
| Docker Compose stack | Not production. The local stack stays as-is for development. | 0 |
| DQ assertions (BalanceRecon, etc.) | Their pass/fail logs flow into MWAA → OpenLineage as **run facets**. The DGQA team sees "this run had 0 DQ failures." | 0 (auto) |
| Secrets management | `.env` → Databricks Secret Scope + MWAA Variables. Same code, different read path. | 0 |
| Write to MinIO | S3 in prod. Same `s3a://` URL pattern (just `s3a://semestabank-silver/...` instead of `s3a://minio:9000/...`). | 1 (env var) |
| Unity Catalog registration | One-time `CREATE TABLE … USING DELTA LOCATION …` registration; lineage flows automatically. | 2 lines, once |

**Total: ~6 lines of code change**, all configuration, no logic.

## How the DGQA team actually uses this

```
DGQA Analyst → Marquez UI (https://marquez.semestabank.id)
  │
  ├── "Where does gold.customer_health_scorecard.month_end_balance come from?"
  │      → graph renders the 6-edge lineage path
  │      → shows each transformation's SQL, run history, and DQ results
  │      → shows the last 10 BI users who queried it
  │
  ├── "Who saw customers.national_id in the last 90 days?"
  │      → Unity Catalog audit_log query
  │      → returns user, timestamp, mask status
  │
  ├── "Show me the lineage broken right now"
  │      → Marquez + Unity Catalog alert on failed runs
  │      → flagged SLA breaks show up as broken edges in the graph
  │
  └── "Generate the OJK audit report for Q1 2026"
         → Marquez API: lineage dump for gold.* → CSV
         → Unity Catalog API: column-mask audit → PDF
         → multi-page regulatory-grade compliance artifact
         → shipped to OJK via S3+API
```

## Why we don't go full OpenLineage-only (avoid over-engineering)

We could disable Unity Catalog and use only OpenLineage + Marquez for everything. Why not?

| Trade-off | Unity Catalog | OpenLineage only |
|---|---|---|
| Column-level on Databricks | Free no-code | Need to emit `columnLineage` facets manually for every Spark op — fragile |
| Access control + lineage unified | One system | Two systems (Marquez for lineage, Lake Formation or Ranger for ACLs) |
| BI dashboard column lineage | Lakehouse Federation sees it directly | Need a separate BI connector emission job |
| Cost | Included in Databricks | Free OSS, but engineering labor to wire it = ~2 engineer-months |

Decision: **Unity Catalog is the primary lineage system**, OpenLineage is the fallback
for the non-Databricks edges (Airflow, Airbyte, BI). One unified lineage graph in Marquez
subscribes to Unity Catalog events via the Unity REST API and unifies both worlds.

## What's NOT instrumented (and the explicit reasoning)

| What we don't track | Why we skipped it |
|---|---|
| Individual cell-level lineage | Too granular; column-level meets OJK + improves clarity vs cell-level |
| Ad-hoc notebook queries lineage | Notebooks are for exploration, not production. Production is via Jobs/DLT → tracked. |
| Reverse lineage (who consumes this column) | Built into Unity Catalog already — no need to add |
| Manual Excel exports lineage | Out of scope; user-driven exports aren't "platform" lineage |

## The cost of lineage (yes, it fits the $50K)

- Unity Catalog comes **included** with Databricks Premium / Enterprise tier.
- Marquez on a small EC2 t3.medium: **$30/month**.
- OpenLineage Airflow plugin: free OSS.
- Lakehouse Federation: included in Premium.
- Engineering time to wire it: **2 eng-weeks** once, $0 ongoing.

The DGQA team's lineage capability costs **less than $100/month** on top of the platform.