# =====================================================================
# SemestaBank - Historical Backfill (lakehouse edition)
# Reads ALL transactions from MinIO bronze once, snapshots every date
# in one pass, writes all partitions to MinIO silver.
# =====================================================================
import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("backfill")

S3_ENDPOINT   = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_BRONZE     = os.environ.get("S3_BUCKET_BRONZE", "semestabank-bronze")
S3_SILVER_BKT = os.environ.get("S3_BUCKET", "semestabank-silver")
S3_PATH       = os.environ.get("S3_PATH", "account_snapshots")
AWS_KEY       = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET    = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

builder = (
    SparkSession.builder
    .appName("semestabank_backfill")
    .config("spark.sql.shuffle.partitions", "64")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
)
if AWS_KEY:
    builder = builder.config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)

spark = builder.getOrCreate()
log.info("Spark ready. App id=%s", spark.sparkContext.applicationId)

# ── 1. Read bronze from MinIO ──
accounts = spark.read.parquet(f"s3a://{S3_BRONZE}/accounts/")
n_accounts = accounts.count()
log.info("Accounts: %d", n_accounts)

all_txns = (
    spark.read.parquet(f"s3a://{S3_BRONZE}/transactions/")
    .dropDuplicates(["txn_id"])
    .filter(F.col("status") == "COMPLETED")
)
log.info("Completed txns: %d", all_txns.count())

# ── 2. Aggregate per account per date in ONE pass ──
txn_agg = (
    all_txns
    .withColumn("snapshot_date", F.col("txn_date").cast("date"))
    .groupBy("snapshot_date", "account_id")
    .agg(
        F.sum("amount").alias("txn_total_amount"),
        F.count("txn_id").alias("txn_count"),
        F.sum(F.when(F.col("txn_type") == "DEBIT",  F.col("amount")).otherwise(0)).alias("debit_amount"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
)

# ── 3. Cross-join accounts x all dates for zero-activity days ──
all_dates = txn_agg.select("snapshot_date").distinct()
accounts_cross = accounts.crossJoin(all_dates)

# ── 4. Left-join aggregates, fill nulls ──
snapshot = (
    accounts_cross
    .join(txn_agg, ["account_id", "snapshot_date"], "left")
    .na.fill({"txn_total_amount": 0.0, "txn_count": 0, "debit_amount": 0.0,
              "credit_amount": 0.0, "distinct_channels": 0})
    .withColumn("computed_close_balance",
                F.col("balance") - F.col("credit_amount") + F.col("debit_amount"))
)

n_rows = snapshot.count()
log.info("Snapshot rows: %d  (%d accounts x %d dates)", n_rows, n_accounts, all_dates.count())

# ── 5. DQ checks ──
assert n_rows >= int(os.environ.get("MIN_ACCOUNTS", "1")), f"Too few rows: {n_rows}"
for col in ["account_id", "customer_id", "snapshot_date"]:
    nulls = snapshot.filter(F.col(col).isNull()).count()
    assert nulls == 0, f"Nulls in {col}: {nulls}"
neg = snapshot.filter(F.col("txn_count") < 0).count()
assert neg == 0, f"Negative txn_count rows: {neg}"
mismatch = snapshot.filter(
    F.round(F.col("computed_close_balance") - F.col("balance"), 2) != 0
).count()
log.info("DQ | rows=%d  null_keys=0  neg_txn=0  balance_mismatch=%d (%.2f%%)",
         n_rows, mismatch, 100.0 * mismatch / n_rows)

# ── 6. Write all partitions ──
output = f"s3a://{S3_SILVER_BKT}/{S3_PATH}"
snapshot.write.partitionBy("snapshot_date").mode("overwrite").parquet(output)
log.info("Backfill complete -> %s", output)
spark.stop()
