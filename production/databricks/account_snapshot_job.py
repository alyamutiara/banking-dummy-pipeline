# =====================================================================
# SemestaBank — Q1 Account Snapshot — PRODUCTION (Databricks) variant
# ---------------------------------------------------------------------
# This is the production-grade twin of the LOCAL job at:
#   Final_Answer/local/q1_pipeline/spark/jobs/account_snapshot.py
#
# The TRANSFORMATION LOGIC is identical (dedup → status filter → aggregate →
# left-join accounts → reconciliation column → DQ gates). Only the EDGES
# change for a company-grade Databricks-on-AWS deployment:
#
#   LOCAL                              PRODUCTION (here)
#   ---------------------------------  -------------------------------------
#   os.environ credentials             dbutils.secrets.get(scope, key)
#   JDBC → local Postgres (Oracle      JDBC → Oracle core banking (or
#     stand-in)                          Lakehouse Federation foreign table)
#   write Parquet to MinIO (s3a://)    write Delta to Unity Catalog table
#   dynamic partition overwrite        Delta MERGE / replaceWhere (idempotent)
#   spark-submit --master local[*]     Databricks Job on an autoscaling cluster
#   manual S3A config                  workspace instance profile / UC creds
#
# Run as a Databricks notebook task or a Python wheel task. Secrets live in a
# Databricks secret scope backed by AWS Secrets Manager (see terraform/).
# =====================================================================

import logging
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# In a Databricks notebook, `spark` and `dbutils` are injected automatically.
# For a wheel/spark_python_task we fetch them explicitly.
try:
    spark  # type: ignore[name-defined]
except NameError:
    spark = SparkSession.builder.getOrCreate()

try:
    dbutils  # type: ignore[name-defined]
except NameError:  # pragma: no cover - only for local import/lint
    dbutils = None

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | SILVER-PROD | %(message)s")
log = logging.getLogger("account_snapshot_prod")

# ── Config: Unity Catalog three-level namespace, secrets, params ────────
CATALOG = "semestabank"                      # Unity Catalog catalog
BRONZE  = f"{CATALOG}.bronze"
SILVER  = f"{CATALOG}.silver"
SILVER_TABLE = f"{SILVER}.account_snapshots"

# Job parameter (Databricks Workflows passes this via {{job.parameters.snapshot_date}})
SNAPSHOT_DATE_PARAM = dbutils.widgets.get("snapshot_date") if dbutils else ""
DQ_FAIL_HARD = (dbutils.widgets.get("dq_fail_hard") if dbutils else "1") != "0"


def secret(key: str) -> str:
    """Read a secret from the Databricks secret scope (backed by AWS Secrets Manager).
    NEVER hardcode credentials — this is the production answer to Q1 problem #1."""
    return dbutils.secrets.get(scope="semestabank-core-banking", key=key)


def resolve_snapshot_date() -> date:
    """CLI/job param > latest Delta partition + 1 > yesterday (same policy as local)."""
    if SNAPSHOT_DATE_PARAM:
        return datetime.strptime(SNAPSHOT_DATE_PARAM, "%Y-%m-%d").date()
    today = date.today()
    if spark.catalog.tableExists(SILVER_TABLE):
        mx = (spark.table(SILVER_TABLE)
              .agg(F.max("snapshot_date").alias("m")).collect()[0]["m"])
        if mx is not None and (mx + timedelta(days=1)) <= today:
            return mx + timedelta(days=1)
    return today - timedelta(days=1)


# ── Bronze reads ────────────────────────────────────────────────────────
# Option A (shown): direct Oracle JDBC with predicate pushdown — the same
# incremental strategy as Q1 (only the target day's transactions).
# Option B (preferred at scale): read bronze Delta tables maintained by a CDC
# (AWS DMS) job, e.g. spark.table(f"{BRONZE}.transactions").
ORACLE_URL = "jdbc:oracle:thin:@core-db:1521/banking"


def read_oracle(table: str, predicate: str | None = None):
    reader = (spark.read.format("jdbc")
              .option("url", ORACLE_URL)
              .option("user", secret("oracle_user"))
              .option("password", secret("oracle_password"))
              .option("driver", "oracle.jdbc.OracleDriver")
              .option("fetchsize", "10000"))
    if predicate:
        # Pushed down to Oracle — only the needed rows cross the wire.
        reader = reader.option("dbtable", f"(SELECT * FROM {table} WHERE {predicate}) t")
    else:
        reader = reader.option("dbtable", table)
    return reader.load()


def build_snapshot(accounts, today_txns, snap_date: date):
    """IDENTICAL business logic to the local job."""
    today_txns = today_txns.dropDuplicates(["txn_id"])
    real = today_txns.filter(F.col("status") == "COMPLETED")
    agg = (real.groupBy("account_id").agg(
        F.sum("amount").alias("txn_total_amount"),
        F.count("txn_id").alias("txn_count"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
        F.countDistinct("channel").alias("distinct_channels")))
    snap = (accounts.join(agg, "account_id", "left")
            .na.fill({"txn_total_amount": 0.0, "txn_count": 0, "debit_amount": 0.0,
                      "credit_amount": 0.0, "distinct_channels": 0})
            .withColumn("snapshot_date", F.lit(snap_date).cast("date"))
            .withColumn("computed_close_balance",
                        F.col("balance") - F.col("credit_amount") + F.col("debit_amount")))
    return snap, real


# ── Data quality (same gates as local) ──────────────────────────────────
class DataQualityError(RuntimeError):
    pass


def run_dq(snapshot, cfg_min=1):
    n = snapshot.count()
    if n < cfg_min:
        raise DataQualityError(f"row_count {n} < {cfg_min}")
    nulls = snapshot.filter(
        F.col("account_id").isNull() | F.col("customer_id").isNull()
        | F.col("snapshot_date").isNull()).count()
    if nulls and DQ_FAIL_HARD:
        raise DataQualityError(f"{nulls} null keys")
    mism = snapshot.filter(~((F.col("txn_count") == 0)
                | (F.abs(F.col("balance") - F.col("computed_close_balance")) < 1.0))).count()
    pct = 100.0 * mism / n if n else 0.0
    log.info("DQ | rows=%d null_keys=%d balance_mismatch=%.2f%%", n, nulls, pct)
    if pct > 2.0 and DQ_FAIL_HARD:
        raise DataQualityError(f"balance reconciliation mismatch {pct:.2f}% > 2%")


def write_delta_idempotent(snapshot, snap_date: date):
    """Idempotent write via Delta `replaceWhere` — re-running the same date
    overwrites exactly that partition and nothing else (the production
    equivalent of the local dynamic-partition-overwrite). Time-travel and
    OJK audit come for free from the Delta transaction log."""
    (snapshot.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"snapshot_date = '{snap_date.isoformat()}'")
        .option("overwriteSchema", "false")
        .partitionBy("snapshot_date")
        .saveAsTable(SILVER_TABLE))
    log.info("Wrote %s partition snapshot_date=%s (Delta).", SILVER_TABLE, snap_date)


def main():
    snap_date = resolve_snapshot_date()
    log.info("============ snapshot_date=%s ============", snap_date)

    accounts = read_oracle("ACCOUNTS")
    # Incremental: only this day's transactions, filtered AT THE SOURCE.
    txns = read_oracle("TRANSACTIONS",
                       predicate=f"TRUNC(txn_date) = DATE '{snap_date.isoformat()}'")
    snapshot, _ = build_snapshot(accounts, txns, snap_date)
    run_dq(snapshot)
    write_delta_idempotent(snapshot, snap_date)

    # Optimize for downstream readers (credit scoring + BI).
    spark.sql(f"OPTIMIZE {SILVER_TABLE} WHERE snapshot_date = '{snap_date.isoformat()}' "
              f"ZORDER BY (customer_id)")
    log.info("SUCCESS snapshot_date=%s", snap_date)


if __name__ == "__main__":
    main()
