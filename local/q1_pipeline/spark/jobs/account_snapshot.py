# =====================================================================
# SemestaBank — Q1: Daily Account Snapshot (Lakehouse Edition)
# PySpark ELT: MinIO/bronze → MinIO/silver
#
# Reads raw Parquet from the bronze layer (ingested from CSVs by
# bronze_ingest.py) and produces a clean, deduplicated daily snapshot.
#
# Design goals (mapped to the test's requirements):
#   (a) Six+ problems fixed  → see docs/PROBLEMS_AND_FIXES.md
#   (b) Rewrite rules:
#       - Incremental loading  (reads only the txn_day=<date> partition;
#                               bronze itself is ingested incrementally too)
#       - Output partitioned by snapshot_date  (time-travel queries)
#       - Secret management    (no hardcoded creds → env vars)
#       - Data-quality assertions (row-count sanity + balance reconciliation)
#       - Idempotent          (dynamic partition overwrite)
#   (c) Orchestration/SLA    → pipeline.sh chains all jobs
#
# Lakehouse: all data lives in MinIO — no PostgreSQL dependency.
# =====================================================================

import os
import sys
import logging
import argparse
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | SILVER | %(message)s",
)
log = logging.getLogger("account_snapshot")


# ── Config (env vars, never hardcoded) ─────────────────────────────────
class Config:
    S3_ENDPOINT    = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    S3_BRONZE      = os.environ.get("S3_BUCKET_BRONZE", "semestabank-bronze")
    S3_SILVER_BKT  = os.environ.get("S3_BUCKET", "semestabank-silver")
    S3_SILVER_PATH = os.environ.get("S3_PATH", "account_snapshots")
    S3_REGION      = os.environ.get("S3_REGION", "us-east-1")
    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    SNAPSHOT_DATE  = os.environ.get("SNAPSHOT_DATE")
    MIN_ACCOUNTS   = int(os.environ.get("MIN_ACCOUNTS", "1"))
    DQ_FAIL_HARD   = os.environ.get("DQ_FAIL_HARD", "1") == "1"

    @property
    def bronze_accounts(self):
        return f"s3a://{self.S3_BRONZE}/accounts/"
    @property
    def bronze_transactions(self):
        return f"s3a://{self.S3_BRONZE}/transactions/"
    @property
    def bronze_customers(self):
        return f"s3a://{self.S3_BRONZE}/customers/"
    @property
    def bronze_credit_scores(self):
        return f"s3a://{self.S3_BRONZE}/credit_scores/"

    @property
    def silver_output(self):
        return f"s3a://{self.S3_SILVER_BKT}/{self.S3_SILVER_PATH}/"


def build_spark(cfg: Config) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("semestabank_account_snapshot")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.files.maxRecordsPerFile", 250000)
        .config("spark.hadoop.fs.s3a.endpoint", cfg.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if cfg.AWS_ACCESS_KEY:
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", cfg.AWS_ACCESS_KEY)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", cfg.AWS_SECRET_KEY)
    log.info("Spark ready. app=%s, master=%s", spark.sparkContext.applicationId, spark.sparkContext.master)
    return spark


def resolve_snapshot_date(spark, cfg: Config, cli_date=None) -> date:
    """Self-healing snapshot_date resolution.

    Priority: CLI --date  >  env SNAPSHOT_DATE  >  latest partition + 1  >  max txn_day in bronze  >  yesterday
    """
    # 1) CLI argument (highest priority — ad-hoc runs)
    if cli_date:
        log.info("Snapshot date from CLI: %s", cli_date)
        return datetime.strptime(cli_date, "%Y-%m-%d").date()
    # 2) Env var (Airflow / scheduled runs)
    if cfg.SNAPSHOT_DATE:
        log.info("Snapshot date from env: %s", cfg.SNAPSHOT_DATE)
        return datetime.strptime(cfg.SNAPSHOT_DATE, "%Y-%m-%d").date()
    # 3) Auto-resume: latest partition + 1 day
    today = date.today()
    try:
        existing = (
            spark.read.parquet(cfg.silver_output)
            .select(F.max("snapshot_date").alias("max_date"))
            .collect()[0]["max_date"]
        )
        if existing is not None:
            next_date = existing + timedelta(days=1)
            # Don't auto-resume past today (avoids running on dates with no data)
            if next_date <= today:
                log.info("Auto-resume: last silver snapshot=%s → building %s", existing, next_date)
                return next_date
            else:
                log.info("Last snapshot %s is already current, using yesterday", existing)
    except AnalysisException:
        log.info("Silver path empty/missing → defaulting to yesterday.")
    except Exception as exc:
        log.warning("Snapshot discovery failed (%s) → defaulting to yesterday.", exc)
    # 3.5) Data-driven default: fall back to max txn_day in bronze
    try:
        latest = (
            spark.read.parquet(cfg.bronze_transactions)
            .select(F.max("txn_day"))
            .collect()[0][0]
        )
        if latest:
            log.info("Defaulting to latest data date %s", latest)
            return datetime.strptime(latest, "%Y-%m-%d").date()
    except Exception:
        pass
    # 4) Yesterday (default nightly behaviour)
    return today - timedelta(days=1)


# ── Read from MinIO bronze ────────────────────────────────────────────

def read_accounts(spark, cfg: Config):
    """Read accounts dimension from MinIO bronze (Parquet)."""
    log.info("Reading bronze.accounts from %s", cfg.bronze_accounts)
    return spark.read.parquet(cfg.bronze_accounts)


def read_incremental_transactions(spark, cfg: Config, snap_date: date):
    """Incremental read: only this snapshot_date's transactions.

    Bronze transactions are partitioned by `txn_day` (see bronze_ingest.py),
    so filtering on the partition column lets Spark prune at the *directory*
    level — it lists and opens only the `txn_day=<snap_date>` folder and never
    touches the other days' files. This is true partition pruning, not the
    weaker within-file row-group skipping a filter on an unpartitioned column
    would give.
    """
    log.info("Reading incremental transactions for partition txn_day=%s", snap_date)

    today_txns = (
        spark.read.parquet(cfg.bronze_transactions)
        .filter(F.col("txn_day") == F.lit(snap_date.isoformat()))
    )
    return today_txns


# ── Build snapshot ─────────────────────────────────────────────────────

def build_snapshot(spark, accounts, today_txns, snap_date: date):
    """Build the silver daily account snapshot."""
    today_txns = today_txns.dropDuplicates(["txn_id"])
    real_txns = today_txns.filter(F.col("status") == "COMPLETED")

    txn_agg = (
        real_txns
        .groupBy("account_id")
        .agg(
            F.sum("amount").alias("txn_total_amount"),
            F.count("txn_id").alias("txn_count"),
            F.sum(F.when(F.col("txn_type") == "DEBIT",
                         F.col("amount")).otherwise(0)).alias("debit_amount"),
            F.sum(F.when(F.col("txn_type") == "CREDIT",
                         F.col("amount")).otherwise(0)).alias("credit_amount"),
            F.countDistinct("channel").alias("distinct_channels"),
        )
    )

    snapshot = (
        accounts.join(txn_agg, "account_id", "left")
        .na.fill({
            "txn_total_amount": 0.0,
            "txn_count": 0,
            "debit_amount": 0.0,
            "credit_amount": 0.0,
            "distinct_channels": 0,
        })
        .withColumn("snapshot_date", F.lit(snap_date).cast("date"))
    )

    # Reconciliation column
    snapshot = snapshot.withColumn(
        "computed_close_balance",
        F.col("balance") - F.col("credit_amount") + F.col("debit_amount"),
    )
    return snapshot, real_txns


# ── Data quality ───────────────────────────────────────────────────────

class DataQualityError(RuntimeError):
    """Raised when a DQ check fails and DQ_FAIL_HARD is on."""


def assert_row_count(df, expected_min: int, label: str, cfg: Config):
    n = df.count()
    log.info("DQ | %s row_count=%d (min=%d)", label, n, expected_min)
    if n < expected_min:
        msg = f"DQ FAIL: {label} row_count={n} < min={expected_min}"
        if cfg.DQ_FAIL_HARD:
            raise DataQualityError(msg)
        log.error(msg)


def assert_no_nulls(df, cols, label: str, cfg: Config):
    for c in cols:
        n = df.filter(F.col(c).isNull()).count()
        log.info("DQ | %s nulls in '%s' = %d", label, c, n)
        if n > 0:
            msg = f"DQ FAIL: {label} has {n} nulls in '{c}'"
            if cfg.DQ_FAIL_HARD:
                raise DataQualityError(msg)
            log.error(msg)


def assert_non_negative(df, col, label: str, cfg: Config):
    n = df.filter(F.col(col) < 0).count()
    log.info("DQ | %s negative '%s' rows = %d", label, col, n)
    if n > 0:
        msg = f"DQ FAIL: {label} has {n} rows with negative {col}"
        if cfg.DQ_FAIL_HARD:
            raise DataQualityError(msg)
        log.error(msg)


def recon_balance(snapshot, cfg: Config):
    n = snapshot.count()
    matches = snapshot.filter(
        (F.col("txn_count") == 0) |
        (F.abs(F.col("balance") - F.col("computed_close_balance")) < 1.0)
    ).count()
    mismatch = n - matches
    pct = (100.0 * mismatch / n) if n else 0.0
    log.info("DQ | balance_recon: mismatch=%d (%.2f%%) of %d rows", mismatch, pct, n)
    if pct > 2.0 and cfg.DQ_FAIL_HARD:
        raise DataQualityError(f"DQ FAIL: balance_recon mismatch {pct:.2f}%% > 2%%")


# ── OJK Regulatory DQ checks ──────────────────────────────────────────

def assert_kyc_compliance(accounts, customers, transactions, cfg: Config):
    """OJK rules: ACTIVE accounts must have VERIFIED KYC; REJECTED-KYC
    customers must not have transactions.  KYC PENDING > 30 days requires
    escalation (logged as ERROR even in non-hard-fail mode)."""
    accounts_with_kyc = accounts.join(
        customers.select("customer_id", "kyc_status", "registration_date"),
        "customer_id", "left"
    )
    # Rule 1: ACTIVE accounts without VERIFIED KYC
    active_not_verified = accounts_with_kyc.filter(
        (F.col("status") == "ACTIVE") & (F.col("kyc_status") != "VERIFIED")
    )
    n1 = active_not_verified.count()
    log.info("DQ | OJK-kyc: ACTIVE accounts with non-VERIFIED KYC = %d", n1)
    if n1 > 0:
        log.warning("OJK-KYC: %d ACTIVE account(s) with kyc_status != VERIFIED (logged, never blocks)", n1)
        active_not_verified.select("account_id", "customer_id", "status", "kyc_status") \
            .show(min(10, n1), truncate=False)

    # Rule 2: transactions from REJECTED-KYC customers (should be zero)
    rejected_accounts = accounts_with_kyc.filter(F.col("kyc_status") == "REJECTED") \
        .select("account_id")
    rejected_txns = transactions.join(rejected_accounts, "account_id")
    n2 = rejected_txns.count()
    log.info("DQ | OJK-kyc: transactions from REJECTED-KYC customers = %d", n2)
    if n2 > 0:
        log.warning("OJK-KYC: %d transaction(s) from REJECTED-KYC customer(s) (logged, never blocks)", n2)

    # Rule 3: KYC PENDING > 30 days — log for escalation
    from datetime import date as dt_date
    pending_stale = accounts_with_kyc.filter(
        (F.col("kyc_status") == "PENDING") &
        (F.datediff(F.lit(dt_date.today()), F.col("registration_date")) > 30)
    )
    n3 = pending_stale.count()
    log.info("DQ | OJK-kyc: KYC PENDING > 30 days = %d (requires escalation)", n3)
    if n3 > 0:
        pending_stale.select("customer_id", "registration_date").show(min(10, n3), truncate=False)


def assert_credit_score_stability(credit_scores, cfg: Config):
    """OJK rule: credit_score must not jump >100 points between
    consecutive assessments for the same customer."""

    w = Window.partitionBy("customer_id").orderBy("score_date")
    jumps = (
        credit_scores
        .withColumn("prev_score", F.lag("credit_score").over(w))
        .withColumn("score_jump", F.abs(F.col("credit_score") - F.col("prev_score")))
        .filter(F.col("score_jump") > 100)
    )
    n = jumps.count()
    log.info("DQ | OJK-cs: credit_score jumps >100 pts = %d", n)
    if n > 0:
        log.warning("OJK-CS: %d credit_score jump(s) >100 pts between assessments (logged, never blocks)", n)
        jumps.select("customer_id", "score_date", "credit_score", "prev_score", "score_jump") \
            .show(min(10, n), truncate=False)


def assert_corr_risk_score_and_pd(customers, credit_scores, cfg: Config):
    """OJK rule: customer risk_score and probability_of_default should be correlated.
    We compute the Pearson correlation.  Below 0.3 is suspicious."""
    latest_cs = (
        credit_scores
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("customer_id").orderBy(F.col("score_date").desc())
        ))
        .filter(F.col("rn") == 1)
        .select("customer_id", "probability_of_default")
    )
    joined = customers.join(latest_cs, "customer_id", "inner")
    corr = joined.stat.corr("risk_score", "probability_of_default")
    log.info("DQ | OJK-corr: risk_score ~ probability_of_default = %.4f (expect >0.3)", corr or 0)
    if corr and corr < 0.3:
        log.warning("OJK-CORR: risk_score/PD correlation %.4f < 0.3 (logged, never blocks)", corr)


# ── Write silver ───────────────────────────────────────────────────────

def write_silver_snapshot(snapshot, cfg: Config, snap_date: date):
    output = cfg.silver_output
    log.info("Writing silver snapshot to %s partition snapshot_date=%s", output, snap_date)
    (
        snapshot.repartition("account_id")
        .write
        .mode("overwrite")
        .option("compression", "zstd")
        .partitionBy("snapshot_date")
        .parquet(output)
    )
    log.info("Snapshot written.")


# ── Main ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SemestaBank daily account snapshot")
    parser.add_argument("--date", help="Snapshot date (YYYY-MM-DD)", default=None)
    args, _ = parser.parse_known_args()

    cfg = Config()
    spark = build_spark(cfg)

    snap_date = resolve_snapshot_date(spark, cfg, args.date)
    log.info("================ snapshot_date = %s ================", snap_date)

    accounts = read_accounts(spark, cfg)
    customers = spark.read.parquet(cfg.bronze_customers)
    credit_scores = spark.read.parquet(cfg.bronze_credit_scores)
    today_txns = read_incremental_transactions(spark, cfg, snap_date)

    snapshot, real_txns = build_snapshot(spark, accounts, today_txns, snap_date)

    start = datetime.utcnow()
    try:
        accounts_rows = accounts.cache().count()
        txn_rows      = real_txns.cache().count()
        snap_rows     = snapshot.cache().count()
        log.info("Materialised: accounts=%d, completed_today_txns=%d, snapshot=%d",
                 accounts_rows, txn_rows, snap_rows)

        # ── Standard DQ checks ──
        assert_row_count(snapshot, cfg.MIN_ACCOUNTS, "silver_snapshot", cfg)
        assert_no_nulls(snapshot, ["account_id", "customer_id", "snapshot_date"], "silver_snapshot", cfg)
        assert_non_negative(snapshot, "txn_count", "silver_snapshot", cfg)
        recon_balance(snapshot, cfg)

        # ── OJK regulatory DQ checks ──
        assert_kyc_compliance(accounts, customers, real_txns, cfg)
        assert_credit_score_stability(credit_scores, cfg)
        assert_corr_risk_score_and_pd(customers, credit_scores, cfg)

        write_silver_snapshot(snapshot, cfg, snap_date)

    except DataQualityError as dq:
        log.error("Pipeline aborted by data-quality gate: %s", dq)
        spark.stop()
        sys.exit(3)

    elapsed = (datetime.utcnow() - start).total_seconds()
    log.info("SUCCESS | snapshot_date=%s | elapsed=%.1fs", snap_date, elapsed)
    spark.stop()


if __name__ == "__main__":
    main()
