#!/usr/bin/env bash
# =====================================================================
# SemestaBank Lakehouse Pipeline — end-to-end orchestration
# Runs inside the Spark container. Executes all jobs in order:
#   bronze → silver → gold
#
# Two silver modes (set SILVER_MODE env, default "backfill"):
#   SILVER_MODE=backfill    → backfill_snapshot.py  (initial load: all
#                             historical dates in one pass; creates every
#                             snapshot_date partition gold_scorecard.py
#                             needs for MoM balance)
#   SILVER_MODE=incremental → account_snapshot.py  (nightly: single date;
#                              SNAPSHOT_DATE env recommended)
# =====================================================================
set -euo pipefail

SILVER_MODE="${SILVER_MODE:-backfill}"

echo "============================================================"
echo " SemestaBank Lakehouse Pipeline  (silver mode: ${SILVER_MODE})"
echo "============================================================"

SPARK_SUBMIT="/opt/spark/bin/spark-submit --master local[*]"
JOBS_DIR="/opt/spark/jobs"

# ── Step 1: Bronze ingestion (Postgres → MinIO Parquet) ──
echo ""
echo "[1/4] Bronze ingestion — loading source tables into MinIO/bronze..."
${SPARK_SUBMIT} ${JOBS_DIR}/bronze_ingest.py
echo " ✓ Bronze ready"

# ── Step 2: Silver — Q1 account snapshots ──
echo ""
if [ "$SILVER_MODE" = "backfill" ]; then
    echo "[2/4] Silver — running backfill_snapshot (ALL dates, one pass → MinIO silver)..."
    ${SPARK_SUBMIT} ${JOBS_DIR}/backfill_snapshot.py
else
    echo "[2/4] Silver — running account_snapshot (single date → MinIO silver)..."
    ${SPARK_SUBMIT} ${JOBS_DIR}/account_snapshot.py
fi
echo " ✓ Silver ready"

# ── Step 3: Gold — Q2(a) customer health scorecard ──
echo ""
echo "[3/4] Gold — running Q2(a) customer_health_scorecard (MinIO → MinIO gold)..."
${SPARK_SUBMIT} ${JOBS_DIR}/gold_scorecard.py
echo " ✓ Gold scorecard ready"

# ── Step 4: Gold — Q2(b) fraud detection alerts ──
echo ""
echo "[4/4] Gold — running Q2(b) fraud_detection_alerts (MinIO → MinIO gold)..."
${SPARK_SUBMIT} ${JOBS_DIR}/gold_fraud.py
echo " ✓ Gold fraud alerts ready"

echo ""
echo "============================================================"
echo " Pipeline complete: bronze → silver → gold in MinIO "
echo "  s3://semestabank-bronze/    — raw ingested data        "
echo "  s3://semestabank-silver/    — Q1 cleaned snapshots      "
echo "  s3://semestabank-gold/      — Q2 regulatory views       "
echo "============================================================"
