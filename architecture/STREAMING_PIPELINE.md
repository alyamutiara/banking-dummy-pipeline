# Task 2 — Real-Time Clickstream Pipeline (50K events/sec)

> **Question:** Design the real-time ingestion from Kafka to analytics-ready state. How is
> this data joinable with the batch account data from Q1? Hot/warm/cold storage strategy
> for event data?

## The flow

```
Mobile app
   │  SDK batch (10 ms / 500 events per batch)
   ▼
┌─────────────────────────────────────────────────────────────────┐
│  AWS MSK — Kafka cluster                                         │
│  topic: clickstream_events  (48 partitions)                      │
│  retention: 7 days                                                │
│  partition key: customer_id (events from same customer land on  │
│  same partition → ordering guaranteed per customer)              │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     │  50,000 events/sec at peak
                     │  ~150 KB/sec per partition (well under the
                     │  5 MB/sec per-partition cap)
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│  Databricks Structured Streaming job (Bronze → Silver)           │
│                                                                  │
│  readStream  ← Kafka MSK                                            │
│  parse JSON  (event_id, customer_id, event_type, ts, ...)       │
│  dedup       by event_id  (watermark = 24 h)                    │
│  writeStream → Delta S3 (bronze.clickstream_events_raw)         │
│    mode = append                                                │
│    partitionBy("event_date")                                    │
│    checkpoint at S3 (idempotent restart)                         │
│                                                                  │
│  And in parallel:                                                │
│  Bronze → Delta Live Tables (Silver)                             │
│    DLT pipeline: dedupe + enrich + sessionization                │
│    output: silver.clickstream_enriched                           │
│    partitionBy("customer_id", "event_date")                     │
│    Z-ORDER BY customer_id                                       │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│  Gold — customer session features                                │
│  Delta Live Tables aggregation:                                  │
│   - sessions_per_day, last_active_at, app_usage_min_7d,         │
│     screens_viewed,txn_attempts_d7d                              │
│   partitionBy feature_date                                       │
│  Written to: gold.clickstream_features                          │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│  Serving                                                         │
│  - Databricks SQL: real-time risk dashboard (1-2s latency)      │
│  - ML credit-scoring: reads gold features every inference        │
│  - BI Tableau: joins features to customer profile                │
└─────────────────────────────────────────────────────────────────┘
```

## Why this design instead of alternatives

| Alternative | Why not |
|---|---|
| Lambda architecture (separate batch + speed layers) | 2x codebase, 2x lineage, 2x bugs to fix. Delta + Structured Streaming is the **Kappa architecture**: one code path for batch and streaming. The Q1 batch job and the streaming job both write Delta — same-serving pattern. |
| Flink on Kinesis Data Analytics | Solid choice. But SemestaBank is on Databricks; adding Flink means another engine to staff, monitor, govern. Structured Streaming does the same job with one runtime. |
| Kafka Streams inside the app | Requires each consumer app to know about analytics — coupling that violates the data engineering contract. Mobile SDKs should only push to Kafka, not compute features. |
| Direct Kinesis Firehose → S3 | No reprocessing buffer; one consumer outage = data lost. Kafka's 7-day retention lets us replay the stream into a brand-new enriched table if needed. |

## Throughput math (proving 50K/sec fits)

| Metric | Value | Capacity check |
|---|---|---|
| Peak events/sec | 50K | |
| Avg event size | 0.5 KB | 25 MB/sec total |
| MSK broker count | 3 (m5.large) | Each handles ~25 MB/sec ingest — 8x headroom |
| Partitions | 48 | 1K events/sec per partition — well under Kafka's 5K/sec per-partition limit |
| Structured Streaming micro-batch | every 10 sec → ~500K events per batch | 10 sec is fast enough for fraud pattern detection, slow enough to keep cloud cost low |
| Daily volume | 50K/sec × 86,400 sec ≈ 4.3B events/day | 2.2 TB/day → ~65 TB/month (PB-grade scale fits S3 with tiering) |

## Joining with Q1's batch data — the heart of Task 2

The challenge: clickstream arrives continuously; Q1's account snapshot refreshes daily. How
do you join "live" app clicks with "yesterday's" account balance?

### The trick: range join on `customer_id`

Both layers are **Delta tables partitioned by date** and **Z-ORDERED by `customer_id`**.

```sql
-- Real-time credit risk query: score each transaction attempt using
-- the customer's latest snapshot balance (last night's data) joined to
-- today's clickstream signals.

SELECT
    c.event_id,
    c.customer_id,
    c.event_type,
    c.ts,
    s.current_balance AS snapshot_balance,
    s.month_over_month_change_pct AS snapshot_mom_change,
    s.risk_flag AS snapshot_risk_flag,
    c.session_id,
    c.app_usage_min_7d
FROM silver.clickstream_enriched     AS c
JOIN  silver.account_snapshots        AS s
   ON s.customer_id    = c.customer_id
  AND s.snapshot_date = (
      SELECT MAX(snapshot_date)
      FROM   silver.account_snapshots
      WHERE  snapshot_date <= DATE(c.ts)
  )
WHERE c.event_type IN ('txn_attempt', 'password_change', 'cvv_viewed')
```

> **Production streaming note:** the correlated subquery `SELECT MAX(snapshot_date) ...
> WHERE snapshot_date <= DATE(c.ts)` is fine for ad-hoc / Databricks SQL queries. In a
> true Structured Streaming pipeline you would **broadcast the latest snapshot as a static
> DataFrame** and use a **stream-static join** instead:
>
> ```python
> # Broadcast the latest snapshot once per micro-batch (it's small: ~4M rows)
> latest_snapshot = spark.read.table("silver.account_snapshots") \
>     .filter("snapshot_date = (SELECT MAX(snapshot_date) FROM silver.account_snapshots)")
>
> # Stream-static join: streaming clickstream (hot) ← broadcast snapshot (warm)
> enriched = (spark.readStream.table("silver.clickstream_enriched")
>     .join(F.broadcast(latest_snapshot), "customer_id", "left"))
> ```
>
> This avoids per-row subquery re-evaluation and leverages Spark's broadcast join
> optimization. The SQL version above is the conceptual equivalent for ad-hoc analysis.

Without it, the join is a shuffle of multi-terabyte tables. With it, Databricks physically
clusters rows with the same `customer_id` into the same file → file skipping kicks in →
the join touches maybe 100 files instead of 100,000.

For each customer's 4.2M IDs in a 65 TB-monthly clickstream table, file skipping reduces
I/O by ~100×. That's the difference between a 30-second dashboard refresh and a 30-minute one.

### The conceptual model

```
Hot batch data (Q1's silver snapshot)        Streaming data (this pipeline)
    snapshot_date = 2026-06-21                   event_ts = 2026-06-22 14:32
    customer_id = CUST12345                     customer_id = CUST12345

                ╲             ╱
                  ╲           ╱
                    ╲       ╱
                       JOIN
                        │
                        ▼
            gold.realtime_risk_features
            (one row per live event + snapshot context)
```

The batch layer is the **state** ("what we know about the customer up to last night"). The
streaming layer is the **signal** ("what the customer is doing right now"). Joining them
yields the risk decision surface.

## Hot / Warm / Cold storage strategy

S3 lifecycle policies auto-tier the clickstream data based on event age. The boundary
between tiers is enforced by **S3 Lifecycle rules**, not by application code — the SQL
queries keep working because Unity Catalog transparently fetches from any tier.

```
┌──────────────────────────────────────────────────────────────────┐
│  HOT — last 30 days of clickstream events                        │
│                                                                  │
│  S3 Standard                                                     │
│  Storage cost: $23/TB-month                                      │
│  Why: real-time dashboards, ML inference, fraud alerting         │
│  Volume: ~65 TB (≈ 50K/sec × 0.5KB × 30d × 86.4K/d)             │
│  Compute: Databricks SQL Serverless auto-scales to user demand  │
│  Cost: ~$1,500/mo for storage                                   │
└──────────────────────────────────────────────────────────────────┘
                            │
                            │  After 30 days → S3 Lifecycle rule
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  WARM — 1 month to 2 years                                       │
│                                                                  │
│  S3 Standard-IA (Infrequent Access)                              │
│  Storage cost: $12.5/TB-month (save ~45% vs Standard)            │
│  Why: monthly marketing campaign analysis, model training,       │
│       quarterly trend reports                                    │
│  Volume: ~250 TB (24 × monthly buckets)                         │
│  Cost: ~$3,100/mo                                               │
│  Pattern: queried in batch (Spark/DLT), not real-time BI         │
│  Access latency: ~50 ms (fine for batch queries; not for BI)   │
└──────────────────────────────────────────────────────────────────┘
                            │
                            │  After 2 years → S3 Lifecycle rule
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  COLD — 2–7 years                                                │
│                                                                  │
│  S3 Glacier Instant Retrieval (years 2-3)                        │
│  Storage cost: $4/TB-month                                       │
│  Why: OJK retention mandates 7-year audit trail                 │
│  Volume: ~1 PB after 3 years of operations                      │
│  Cost: ~$4,000/mo                                               │
│                                                                  │
│  S3 Glacier Deep Archive (years 3-7)                             │
│  Storage cost: $1/TB-month                                       │
│  Volume: ~2 PB flat future                                     │
│  Cost: ~$2,000/mo                                               │
│  Retrieval: 12 hours; only needed if OJK audits raw clicks       │
│  Pattern: lifecycle rule moves Delta files; tables remain         │
│  registered in Unity Catalog; SELECT bypasses Glacier by default │
│  unless explicitly queried                                       │
└──────────────────────────────────────────────────────────────────┘
```

### The clever part — Delta table sees all tiers transparently

The Delta log lists every file. S3 lifecycle rules move the files between tiers
transparently. A query against the Delta table:
- Hits S3 Standard for the last 30 days (fast)
- Hits S3 IA if you scan the last year (acceptable for batch)
- Hits Glacier only if you explicitly query events older than 2 years (audit; rare)

Unity Catalog lineage **follows the data across all tiers**, so even a 4-year-old audit
query produces full column-level lineage back to source.

### What happens when business intelligence doesn't want to wait for S3 lifecycle?

You can skip the IA tier for the **aggregated Gold features** (20 MB/day, not 2 TB/day).
Gold features stay in S3 Standard for the entire 7-year retention window. The tiering only
applies to **raw event dumps** that BI never touches.

| Layer | Held in Hot (30d) | Held in Warm (2y) | Held in Cold (7y) |
|---|:---:|:---:|:---:|
| Bronze.clickstream_events_raw | ✓ | ✓ | ✓ |
| Silver.clickstream_enriched | ✓ | ✓ | ✗ (rolled up to gold after 2 years) |
| Gold.clickstream_features | ✓ | ✓ ( remodel into aggregates ) | ✓ |
| Q1's silver.account_snapshots | ✓ (last 90 d) | ✓ (90 d – 5 y) | ✗ (regenerated from bronze) |

The table above is the **storage design** in your submission.

## Critical S3 / Delta settings used

| Setting | Value | Why |
|---|---|---|
| Delta `delta.logRetentionDuration` | `interval 730 days` (2 years) | Lets us time-travel for audits |
| Delta `delta.deletedFileRetentionDuration` | `interval 35 days` | Above the 30-day hot tier; safe |
| Delta Z-ORDER BY | `customer_id` (Silver), `event_date` (Bronze) | File skipping magic for the joins |
| S3 bucket lifecycle | Standard 0-30d → Standard-IA 30d-2y → Glacier IR 2-3y → Glacier DR 3-7y | Tiering |
| S3 bucket versioning | enabled | Audit compliance for OJK |
| S3 Object Lock | Compliance mode, 7 years | OJK-grade immutability for gold features |

## The exact failure modes the pipeline handles

| Failure | What happens |
|---|---|
| MSK broker down | Other brokers serve the partition; structured streaming retries within seconds (no data loss) |
| Databricks streaming job crashes | Checkpoint in S3 lets it resume exactly where it left off (at-least-once + dedup by event_id = effectively once) |
| Late event arrives (e.g., 6h late from device offline) | Structured Streaming watermark of 24h accepts it; `MERGE` updates the silver row |
| S3 write fails | Delta transaction is atomic — either the new files commit, or they don't. Half-written data never appears. |
| Schema evolves (new event type `loan_applied`) | Spark reads the schema from the latest Delta commit; new columns auto-merge with `mergeSchema=true` |
| Order across customers is needed | Partition key is customer_id; Kafka guarantees order within partition; Structured Streaming preserves per-partition order |