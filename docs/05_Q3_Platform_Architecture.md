# 05 · Q3 — Platform Architecture

> **The task.** Design SemestaBank's end-to-end platform supporting the Q1 batch ELT, the Q2
> regulatory views, 50K-events/sec clickstream, the credit-scoring ML pipeline, and DGQA
> governance (lineage, catalog, access control) — within a **$50K/month** budget. (g) Architecture
> diagram + per-component justification. (h) Real-time ingestion + batch-stream join +
> hot/warm/cold storage. (i) Column-level lineage Oracle→BI and how it integrates with Q1.

**Full docs:** [`architecture/`](../architecture/) — [ARCHITECTURE](../architecture/ARCHITECTURE.md) ·
[STREAMING_PIPELINE](../architecture/STREAMING_PIPELINE.md) ·
[GOVERNANCE_LINEAGE](../architecture/GOVERNANCE_LINEAGE.md) ·
[BUDGET_BREAKDOWN](../architecture/BUDGET_BREAKDOWN.md) ·
[PLATFORM_DECISIONS](../architecture/PLATFORM_DECISIONS.md)

---

## Platform in one sentence

**One Databricks Lakehouse on AWS** — S3 + Delta for storage, Databricks for batch + streaming +
SQL + ML, MSK for clickstream, Unity Catalog for governance, MWAA for orchestration — instead of
stitching together Snowflake + a separate streaming engine + a separate lineage tool. One
platform, one price, lineage for free.

## (g) The architecture — condensed

```
 SOURCES            INGESTION              STORAGE (S3 + Delta)        PROCESSING / SERVING        GOVERNANCE
 Oracle  ──DMS CDC──▶                  ┌─ BRONZE  raw ─────────┐                                ┌────────────────┐
 Mobile ──MSK 48p──▶  ─────────────▶   │  SILVER  cleaned ★Q1  │ ─▶ Databricks Jobs (Q1)        │ Unity Catalog  │
 Zendesk─Airbyte──▶                    │  GOLD    business ★Q2 │ ─▶ Structured Streaming (Q3h)   │  • column-level│
 Braze ──Airbyte──▶                    └───────────────────────┘ ─▶ DLT gold views (Q2)         │    lineage     │
 GAds  ──Airbyte──▶                       (Delta: ACID +        ─▶ Databricks SQL → BI          │  • RBAC + PII  │
                                           time travel)         ─▶ ML Runtime → credit scoring  │  • audit log   │
                                                                ─▶ OJK export (Delta time travel)│ +OpenLineage   │
                                                                                                 └────────────────┘
```
Full diagram (every box + the alternatives rejected for each) is in
[ARCHITECTURE.md](../architecture/ARCHITECTURE.md).

**Component choices** (justified vs alternatives in [PLATFORM_DECISIONS.md](../architecture/PLATFORM_DECISIONS.md)):
Storage **S3 + Delta** (vs Snowflake/Iceberg/Hudi) · Stream **MSK** (vs Kinesis) · CDC **AWS DMS**
(vs Debezium/Fivetran) · Compute **Databricks** (vs EMR/Glue) · Orchestration **MWAA** (Q1's DAG
runs unchanged) · BI **Databricks SQL** (no data copy) · Governance **Unity Catalog + OpenLineage**
· ML **Databricks ML + MLflow** (already in use) · IaC **Terraform**.

## (h) Real-time clickstream (50K events/sec)

```
Mobile app → MSK (topic app_events, 48 partitions, 7-day retention)
           → Databricks Structured Streaming (checkpointed, exactly-once)
           → SILVER clickstream_silver (Delta, Z-ORDER by customer_id)
           → GOLD real-time aggregates / joined to account snapshots
```

- **Throughput math:** 50K evt/s × ~0.5 KB = **25 MB/s**; 3× `m5.large` MSK brokers carry it with
  ~8× headroom; 48 partitions → up to 48 parallel consumers. ~4.3B events/day ≈ 2.2 TB/day.
- **Batch-stream join (the heart of part h):** clickstream silver and Q1's `account_snapshots`
  are both Delta, both **Z-ORDERed on `customer_id`**, so a join uses file skipping — ~100× less
  I/O (30-sec dashboard vs 30-min scan). A real-time risk query joins a customer's live events to
  their latest snapshot balance.
- **Hot / warm / cold:** S3 **Standard** (0–30 d, ~$1.5K) → **Standard-IA** (30 d–2 y, ~$3.1K) →
  **Glacier IR / Deep Archive** (2–7 y, ~$3K). One Delta table spans all tiers transparently;
  `logRetentionDuration=730d` underpins the 7-year OJK audit trail. Details:
  [STREAMING_PIPELINE.md](../architecture/STREAMING_PIPELINE.md).

## (i) Column-level lineage (Oracle → BI)

Three layers, so every edge is covered ([GOVERNANCE_LINEAGE.md](../architecture/GOVERNANCE_LINEAGE.md)):

1. **Unity Catalog** — automatic column-level lineage for everything that runs in Databricks
   (Oracle-read → silver → gold → BI). No code. Query `system.information_schema.column_lineage`.
2. **OpenLineage** — fallback for non-Databricks edges (Airflow, Airbyte, BI tools) into Marquez.
3. **Lakehouse Federation** — Oracle registered as a foreign catalog, so source columns appear in
   the graph automatically.

**Integration with Q1 = ~6 lines.** The snapshot job needs **0** changes (Unity Catalog
auto-captures it); the Airflow DAG gets ~3 lines to emit OpenLineage events; Unity Catalog
registration is a one-time 2 lines. PII masking (`nik`, `phone`) is tied to lineage tags and
auto-masks for unprivileged roles — the OJK bonus.

## Budget — fits with headroom

**$39K of $50K** ([BUDGET_BREAKDOWN.md](../architecture/BUDGET_BREAKDOWN.md)): Databricks $22K ·
MSK $4K · S3 (tiered) $9K · MWAA $1K · networking $2K · ingestion/observability ~$1K. The $11K
headroom absorbs growth. The single biggest saving is **not** buying a separate BI warehouse,
lineage tool, feature store, or real-time DB — Databricks Premium includes them.

---

## ① Local vs ② Production

Q3 is inherently the **production** design. Its **local proof** is the Q1+Q2 Docker stack:
MinIO ≙ S3/Delta, Spark ≙ Databricks, Airflow ≙ MWAA, Postgres ≙ Oracle. The same medallion
(bronze→silver→gold), the same jobs, the same SQL — which is exactly why the architecture's
"portability over lock-in" principle holds. → [Deploy guide](07_Production_AWS_Databricks.md)
