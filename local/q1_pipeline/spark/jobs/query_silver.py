# =====================================================================
# SemestaBank - Query helper (lakehouse edition)
# Reads silver Parquet from MinIO, prints schema + stats.
# =====================================================================
import os
from pyspark.sql import SparkSession, functions as F

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET   = os.environ.get("S3_BUCKET", "semestabank-silver")
S3_PATH     = os.environ.get("S3_PATH", "account_snapshots")
AWS_KEY     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET  = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

spark = SparkSession.builder \
    .appName("query_silver") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", AWS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

snap = spark.read.parquet(f"s3a://{S3_BUCKET}/{S3_PATH}")

print("\n=== Schema ===")
snap.printSchema()

print(f"\n=== Total rows: {snap.count()} ===\n")

print("=== Partitions ===")
parts = snap.select("snapshot_date").distinct().orderBy("snapshot_date").collect()
print(f"  Count: {len(parts)}")
if parts:
    print(f"  Range: {parts[0].snapshot_date} -> {parts[-1].snapshot_date}")

print("\n=== Top 10 accounts by txn_count (latest date) ===")
latest = parts[-1].snapshot_date
snap.filter(F.col("snapshot_date") == latest) \
    .select("account_id", "customer_id", "account_type", "txn_count", "credit_amount", "debit_amount", "balance") \
    .orderBy(snap.txn_count.desc()) \
    .show(10, truncate=False)

print("\n=== Gold: Customer Health Scorecard ===\n")
scorecard = spark.read.parquet("s3a://semestabank-gold/customer_health_scorecard/")
scorecard.printSchema()
print(f"\nTotal scorecard rows: {scorecard.count()}")
print(f"Customers with risk_flag=true: {scorecard.filter('risk_flag = true').count()}")

print("\n=== Gold: Fraud Detection Alerts ===\n")
fraud = spark.read.parquet("s3a://semestabank-gold/fraud_detection_alerts/")
fraud.groupBy("alert_type").count().show()

spark.stop()
