# Architecture — Q3 Platform Design

The full design docs for **Q3 (Platform Architecture)**. For the summarized answer with a
condensed diagram, see `[../docs/05_Q3_Platform_Architecture.md](../docs/05_Q3_Platform_Architecture.md)`.


| Doc                                            | Test task  | Covers                                                                             |
| ---------------------------------------------- | ---------- | ---------------------------------------------------------------------------------- |
| [ARCHITECTURE.md](ARCHITECTURE.md)             | (g)        | Complete architecture diagram + component-by-component justification               |
| [STREAMING_PIPELINE.md](STREAMING_PIPELINE.md) | (h)        | 50K events/sec Kafka→analytics pipeline, batch-stream join, hot/warm/cold storage  |
| [GOVERNANCE_LINEAGE.md](GOVERNANCE_LINEAGE.md) | (i)        | Column-level lineage (Unity Catalog + OpenLineage) + Q1 integration (~6-line diff) |
| [BUDGET_BREAKDOWN.md](BUDGET_BREAKDOWN.md)     | budget     | $50K/month itemized ($39K used), ramp phases, deliberately-skipped purchases       |
| [PLATFORM_DECISIONS.md](PLATFORM_DECISIONS.md) | trade-offs | 11 component decisions, each vs 3–4 rejected alternatives                          |


**Reading order:** ARCHITECTURE → STREAMING_PIPELINE → GOVERNANCE_LINEAGE, with
BUDGET_BREAKDOWN and PLATFORM_DECISIONS as appendices.

**One-sentence design:** a single Databricks Lakehouse on AWS (S3 + Delta, MSK, Unity Catalog,  
MWAA) covering batch + streaming + SQL + ML + governance — not a multi-tool stack.