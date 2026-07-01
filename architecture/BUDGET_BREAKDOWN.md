# $50K/Month Budget Breakdown

The CDO said **$50K/month**. This file shows exactly what fits, and what we deliberately
left out.

All figures are USD/month, assuming **AWS ap-southeast-1 (Singapore)** pricing — closest
region to Indonesia with full AWS + Databricks availability.

## Total

| Component | Service | Monthly cost |
|---|---|---:|
| Compute (Lakehouse core) | Databricks Jobs + SQL + ML | $22.0K |
| Streaming ingestion | AWS MSK (3 × m5.large) | $4.0K |
| Storage | S3 (lifecycle-tiered, 65 TB hot + 250 TB warm + cold Glacier) | $9.0K |
| Batch ingestion | AWS DMS (Oracle CDC, m5.large) | $0.5K |
| Marketing connectors | Airbyte on 1 × t3.medium | $0.1K |
| Orchestration | MWAA (m5.large Airflow) | $1.0K |
| Data egress + networking | VPC, NAT gateways, transfer | $2.0K |
| Observability | CloudWatch logs, Databricks audit log retention | $0.5K |
| Marquez (lineage viz) | t3.micro EC2 | $0.05K |
| **Total** | | **~$39K** |
| **Headroom** | | **~$11K** |

## Detailed estimate per component

### Compute — Databricks ($22K)

| Workload | Sizing | $/mo |
|---|---|---:|
| Databricks Job batch (Q1's silver snapshot + Zendesk/Braze zones) | 1 medium cluster (`r5.2xl × 4`) running ~4 h/day × 30 d @ $0.40/DBU | $2.5K |
| Databricks Structured Streaming (Bronze → Silver clickstream) | 1 always-on enhanced autoscaling cluster (`r5.2xl × 3-8`) @ Jobs tier | $5.0K |
| Delta Live Tables (Gold: scorecard + fraud alerts, Q2) | Serverless, runs ~30 min/day | $1.5K |
| Databricks SQL Serverless (BI + OJK export queries) | 2XL serverless, autoscaling 2-50, ~8h/user-day × 30 d | $5.0K |
| ML Runtime (credit scoring, retraining + inference) | One ML cluster (`g4dn.xl` GPU, training 2h/day + inference on endpoint) | $5.5K |
| Unity Catalog (governance, audit, lineage, included in Premium tier) | | $0.0K |
| Premium tier markup (10%) | Adds ~$1.0K to total compute | $1.0K |
| Misc Databricks charge (cluster template overhead, instance profiles) | | $1.5K |
| **Compute subtotal** | | **$22.0K** |

### Streaming — AWS MSK ($4K)

- 3 brokers × `m5.large` (2 vCPU, 8 GB).
- ~120 GB EBS per broker (7-day retention at 25 MB/sec × 604,800 sec ≈ 1 TB raw; with
  replication factor 2, ~2 TB across brokers).
- 48 partitions on the `clickstream_events` topic — handles 50K events/sec peak easily.

### Storage — S3 tiers ($9K)

| Tier | Volume | Rate | Cost |
|---|---|---|---:|
| S3 Standard (last 30d hot) | 65 TB | $0.023/GB | $1.5K |
| S3 Standard-IA (30d-2y warm) | ~250 TB | $0.0125/GB | $3.1K |
| S3 Glacier Instant Retrieval (2-3y) | ~250 TB | $0.004/GB | $1.0K |
| S3 Glacier Deep Archive (3-7y audit) | ~1 PB after a year, ~3 PB after 4 years | $0.00099/GB | $1-3K (ramps slowly) |
| Delta log overhead, transactional writes, versioning | 10% buffer | | $0.4K |
| **Subtotal (year ~2 steady state)** | | | **~$9.0K** |

### Ingestion extras ($0.6K)

- **AWS DMS** for Oracle CDC: one `m5.large` replication instance (always-on but only
  moving ~2M transactions/day, not heavy) → ~$200/mo for instance + storage.
- **Airbyte** self-hosted on a `t3.medium` EC2 instance, runs marketing connectors twice
  a day → ~$30/mo.

### Orchestration — MWAA ($1K)

- MWAA m5.large sized environment (1 worker, 1 scheduler); 1 WebServer.
- Airflow DAGs are lightweight — the heavy lifting is done on Databricks.
- Includes Airflow metadata DB and pooling.

### Observability + governance ($0.5K)

- CloudWatch Logs (Databricks audit log forwarding): ingestion + storage = ~$300/mo.
- Marquez UI on a `t3.micro` (sufficient for ~50 lineage events/sec): ~$15/mo.
- S3 dataset lineage snapshot dumps (CSV exports to S3 with object lock): ~$150/mo.

### Networking ($2K)

- VPC peering between MSK / Databricks workspaces / S3 endpoints.
- NAT gateway for outbound to Zendesk / Braze / OJK APIs (~$0.045/GB out).
- KMS customer-managed keys for S3 + DynamoDB encryption: ~$5/key + per-request ~$1/mo.

## What we deliberately didn't buy (and why)

| Skipped component | Monthly cost if added | Why we don't need it |
|---|---:|---|
| Snowflake warehouse (separate from Databricks) | $8-12K | Databricks SQL already serves BI + Q2 gold views + ML inference. Adding Snowflake doubles compute and breaks lineage. |
| Fivetran for Zendesk/Braze | $1.5-3K | Airbyte open-source handles the low volume (15K tickets/12 campaigns per year) for ~$30/mo. |
| AWS Glue Data Catalog + Lake Formation | $3-5K incl. Glue ETL jobs | Databricks Unity Catalog replaces both, included with Premium. |
| ClickHouse / Apache Pinot for "real-time warehouse" | $5-8K (EC2 + ops) | Delta + Databricks SQL on the same S3 path is enough at 50K/sec. Adding Pinot duplicates storage and ops. |
| Collibra / Alation lineage tool | $4-8K license + ops | Unity Catalog has free column-level lineage — the OJK regulator wants lineage, not a fancy enterprise lineage UI. |
| SageMaker Pipelines for ML | $3-5K | Databricks MLflow + ML Runtime already in the platform; switching would mean 6-month migration plus duplicated governance. |
| AWS EMR Serverless (running in parallel to Databricks) | $5-10K | One Spark engine is enough. EMR + Databricks would mean two Spark clusters and two lineage planes. |
| Dedicated consultant / managed services contract | $10-20K | Too expensive — these would push us over $50K. We rely on the small SemestaBank data team + AWS/Databricks support. |
| **Total skipped** | **~$50-70K/mo if added** | consciously budget-engineered out |

## Budget over time (the ramp)

SemestaBank's data platform will not be at full scale on Day 1. The budget ramps:

```
Months 1-3   (build phase, no customer-facing traffic):
- Databricks dev-tier clusters           ~$5K
- MSK with low partitions                ~$2K
- S3 small volume                        ~$1K
- TOTAL                                  ~$10K/mo  (vs. $50K cap)

Months 4-6  (Q1 batch + Q2 views in production):
- Databricks prod + SQL warehouse        ~$15K
- MSK scaled for 50K/sec                 ~$4K
- S3 hot tier populates                  ~$5K
- DMS + Airbyte                           ~$1K
- TOTAL                                  ~$28K/mo

Months 7+   (steady state, full governance enrolled):
- All components at scale                ~$39K/mo
- Headroom for growth                    $11K
```

This means SemestaBank doesn't actually need $50K from day one. The first 3 months cost 20%
of the cap; full steady state hits ~80% of cap. Good CFO conversation.

## What triggers a budget alarm

| Threshold | Alert target | Action |
|---|---|---|
| > $45K monthly run-rate | CFO email + Slack #data-platform | Reduce Databricks SQL warehouse auto-scaling upper bound |
| > $48K monthly run-rate | CDO + CFO + Eng Manager | Trigger cost review meeting; investigate spike |
| > $50K monthly run-rate | Auto/page on-call | Pause non-priority Databricks jobs (labels: "research", "dev"). Resume when budget resets |
| Databricks SQL warehouse idle > 30 min | Slack #data-platform | Auto-stop cluster (built into serverless) |
| S3 storage growth > 10% MoM | CFO email | Investigate; consider accelerating Glacier tier transition |

AWS Budgets + Databricks billing exports feed these checks daily. We don't wait for the
end of the month to be surprised.

## The CFO defense line

When the CDO asks "why does this cost $39K of my $50K?", the answer is:

1. The Databricks billing **already includes** governance (Unity Catalog), SQL warehouse,
   Spark engine, ML runtime, and audit log. Five enterprise products for the price of one.
2. MSK (Kafka) is the only streaming option that survives **50K events/sec peak** reliably.
   Kinesis costs similar; self-managed Kafka costs more once we add ops time.
3. S3 tiered storage keeps audit retention (the 7-year OJK obligation) cheap — Glacier
   Deep Archive is $1/TB-month.
4. Total cost stays well below $50K, leaving $11K of headroom for growth (more customers,
   new products, additional BI concurrency).