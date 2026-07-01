# =====================================================================
# SemestaBank — Bronze Ingestion (Database → MinIO Parquet)
#
# Reads tables from the source PostgreSQL (standing in for Oracle core
# banking) via JDBC and writes them as raw Parquet to the MinIO bronze
# layer. Also creates the supplementary merchant_locations table.
#
# Transactions (the only large, append-mostly table) is ingested
# INCREMENTALLY: a high-water mark (max txn_day already in bronze) is
# pushed down to the source as `WHERE txn_date >= watermark`, so historical
# rows never cross the JDBC connection. It is written partitioned by
# `txn_day` with dynamic overwrite, so each night only the new day's
# partition is touched and downstream silver reads prune by directory.
# Dimensions (customers, accounts, …) stay full-overwrite — they are small.
#
# Production equivalent:
#   Oracle → AWS DMS (CDC) → S3 raw  OR  Databricks JDBC → Delta Bronze
#
# PII masking and duplicate detection happen at ingestion time per
# SemestaBank's data governance policy.
# =====================================================================

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, \
    IntegerType, DecimalType, FloatType
from pyspark.sql.utils import AnalysisException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | BRONZE | %(message)s",
)
log = logging.getLogger("bronze_ingest")


# ── Config (all from env) ──────────────────────────────────────────────
class Config:
    # Source DB (PostgreSQL, standing in for Oracle core banking)
    JDBC_URL       = os.environ["JDBC_URL"]
    JDBC_USER      = os.environ["JDBC_USER"]
    JDBC_PASSWORD  = os.environ["JDBC_PASSWORD"]
    JDBC_FETCH     = int(os.environ.get("JDBC_FETCH", "50000"))
    JDBC_NUM_PARTS = int(os.environ.get("JDBC_NUM_PARTS", "8"))

    # Incremental ingest watermark override (backfills / re-loads).
    # When unset, the high-water mark is discovered from the bronze layer.
    TXN_LOAD_FROM  = os.environ.get("TXN_LOAD_FROM")

    # MinIO sink
    S3_ENDPOINT    = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    S3_BRONZE      = os.environ.get("S3_BUCKET_BRONZE", "semestabank-bronze")
    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    @property
    def bronze_path(self):
        return f"s3a://{self.S3_BRONZE}/"


def build_spark(cfg: Config) -> SparkSession:
    """Spark session wired for both JDBC (source) and S3 (sink)."""
    builder = (
        SparkSession.builder
        .appName("semestabank_bronze_ingest")
        .config("spark.sql.shuffle.partitions", "16")
        # Dynamic overwrite: an incremental write replaces only the txn_day
        # partitions present in the new data, leaving historical days intact.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.endpoint", cfg.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if cfg.AWS_ACCESS_KEY:
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", cfg.AWS_ACCESS_KEY)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", cfg.AWS_SECRET_KEY)
    return spark


# ── JDBC read helper ───────────────────────────────────────────────────
def read_table(spark, cfg: Config, schema_table: str):
    """Read a table (or pushdown sub-query) from the source DB via JDBC.

    `schema_table` may be a bare table name or a parenthesised sub-query
    aliased as a derived table — the latter lets us push a WHERE filter
    down to the database so the rows are filtered *at the source*.
    """
    log.info("Reading %s from source DB...", schema_table)
    df = (
        spark.read.format("jdbc")
        .option("url", cfg.JDBC_URL)
        .option("dbtable", schema_table)
        .option("user", cfg.JDBC_USER)
        .option("password", cfg.JDBC_PASSWORD)
        .option("fetchsize", cfg.JDBC_FETCH)
        .load()
    )
    log.info("  %s → %d rows", schema_table, df.count())
    return df


def resolve_txn_watermark(spark, cfg: Config):
    """High-water mark for incremental transaction ingest.

    Priority: TXN_LOAD_FROM env (backfill override)  >  max(txn_day) already
    in bronze  >  None (first load → full history).  Returned as a 'YYYY-MM-DD'
    string suitable for inlining into the JDBC pushdown predicate.
    """
    if cfg.TXN_LOAD_FROM:
        log.info("Watermark from TXN_LOAD_FROM env: %s", cfg.TXN_LOAD_FROM)
        return cfg.TXN_LOAD_FROM
    try:
        hw = (
            spark.read.parquet(f"{cfg.bronze_path}transactions/")
            .select(F.max("txn_day").alias("hw"))
            .collect()[0]["hw"]
        )
        if hw is not None:
            log.info("Watermark discovered from bronze: max(txn_day)=%s", hw)
            return hw.isoformat()
    except AnalysisException:
        log.info("No existing bronze.transactions → first (full) load.")
    return None


def read_incremental_transactions(spark, cfg: Config, load_from):
    """Read only new transactions, pushing the date filter down to the DB.

    The predicate runs *in the source database* (it is part of the dbtable
    sub-query), so historical rows never cross the JDBC connection. We use
    `>=` on the watermark day so the boundary day is fully re-read; combined
    with dynamic partition overwrite, re-running is idempotent and catches
    late-arriving same-day rows. A `txn_day` (DATE) column is derived for
    directory-level partitioning in the bronze layer.
    """
    if load_from is None:
        dbtable = "bronze.transactions"
        log.info("Full transactions load (no watermark).")
    else:
        # Parenthesised sub-query → WHERE executes on PostgreSQL/Oracle.
        dbtable = (
            f"(SELECT * FROM bronze.transactions "
            f"WHERE txn_date >= '{load_from}') AS incr_txns"
        )
        log.info("Incremental transactions load: txn_date >= %s (pushed to source)", load_from)

    df = read_table(spark, cfg, dbtable)
    return df.withColumn("txn_day", F.to_date("txn_date"))


def write_bronze(df, name: str, cfg: Config, partition_by: str = None):
    output = f"{cfg.bronze_path}{name}/"
    log.info("Writing bronze.%s → %s", name, output)
    if partition_by:
        # Partitioned + dynamic overwrite: only the partitions in `df` are
        # replaced. No coalesce(1) — that would funnel every partition through
        # a single task and defeat parallel writes.
        (
            df.write.mode("overwrite")
            .partitionBy(partition_by)
            .parquet(output)
        )
    else:
        df.coalesce(1).write.mode("overwrite").parquet(output)
    log.info("  bronze.%s written", name)


# ── PII masking ────────────────────────────────────────────────────────
def mask_pii(df):
    """Mask NIK and phone per dataset rules: keep only last 4 digits."""
    return df \
        .withColumn("nik", F.concat(F.lit("XXXX-XXXX-XXXX-"), F.substring(F.col("nik"), -4, 4))) \
        .withColumn("phone", F.concat(F.lit("XXXX-XXXX-"), F.substring(F.col("phone"), -4, 4)))


def detect_duplicates(df, label: str):
    """Log duplicate NIK and phone numbers (OJK reconciliation check)."""
    nik_dupes = df.groupBy("nik").count().filter(F.col("count") > 1)
    n_nik = nik_dupes.count()
    if n_nik > 0:
        log.warning("PII-DUP | %d duplicate NIK(s) in %s", n_nik, label)

    phone_dupes = df.groupBy("phone").count().filter(F.col("count") > 1)
    n_phone = phone_dupes.count()
    if n_phone > 0:
        log.warning("PII-DUP | %d duplicate phone(s) in %s", n_phone, label)
    return n_nik + n_phone


# ── Ingest all tables from source DB ────────────────────────────────────
def ingest_tables(spark, cfg: Config):
    """Read each table from the source database and write to MinIO bronze."""

    # Tables that exist in the bronze schema (loaded by Q1's seed/init.sql)
    # Schema-qualified names as they appear in PostgreSQL.
    ingest = [
        ("customers", "bronze.customers"),
        ("accounts", "bronze.accounts"),
        ("transactions", "bronze.transactions"),
        ("credit_scores", "bronze.credit_scores"),
        ("app_events", "bronze.app_events"),
        ("support_tickets", "bronze.support_tickets"),
        ("acquisition_channels", "bronze.acquisition_channels"),
    ]

    for name, schema_table in ingest:
        # Transactions is the only large, append-mostly table → ingest it
        # incrementally and partition it by day so downstream silver reads
        # can prune at the directory level. Dimensions stay full-overwrite.
        if name == "transactions":
            load_from = resolve_txn_watermark(spark, cfg)
            df = read_incremental_transactions(spark, cfg, load_from)
            write_bronze(df, name, cfg, partition_by="txn_day")
            continue

        df = read_table(spark, cfg, schema_table)

        if name == "customers":
            detect_duplicates(df, name)
            df = mask_pii(df)

        write_bronze(df, name, cfg)


# ── Create supplementary merchant_locations ────────────────────────────
def create_merchant_locations(spark, cfg: Config):
    """Supplementary lookup table — hash-based city assignment.

    The source database has no merchant geo-location data — this is a
    realistic gap. In production, this table would be loaded from the
    payment network (QRIS registry, Visa acquiring data, etc.).

    For the demo: read every distinct reference_id from bronze.transactions
    and deterministically hash it to one of 10 Indonesian cities. A 20-row
    hand seed matches <0.001% of 942k+ distinct reference_ids, making the
    MULTI_CITY fraud pattern structurally dead. With this hash coverage,
    ~10% of merchants per city lets 3-city same-day patterns fire naturally.
    """
    log.info("Creating supplementary bronze.merchant_locations (hash-based)")

    transactions_df = spark.read.parquet(f"{cfg.bronze_path}transactions/")

    cities = [
        ("Jakarta",    -6.2088, 106.8456),
        ("Surabaya",   -7.2575, 112.7521),
        ("Bandung",    -6.9175, 107.6191),
        ("Medan",       3.5952,  98.6722),
        ("Makassar",   -5.1477, 119.4327),
        ("Semarang",   -6.9932, 110.4193),
        ("Palembang",  -2.9911, 104.7570),
        ("Denpasar",   -8.6705, 115.2126),
        ("Balikpapan", -1.2379, 116.8612),
        ("Yogyakarta", -7.7956, 110.3740),
    ]
    from pyspark.sql.functions import col as spark_col

    df = (
        transactions_df
        .select("reference_id")
        .filter(spark_col("reference_id").isNotNull())
        .distinct()
        .withColumn("city_idx", F.abs(F.hash("reference_id")) % 10)
        .withColumn(
            "merchant_name",
            F.concat(F.lit("Merchant "), F.col("reference_id"))
        )
        .withColumn("mcc_category", F.lit("General"))
    )

    city_expr = F.when(F.col("city_idx") == 0, F.lit(cities[0][0]))
    lat_expr  = F.when(F.col("city_idx") == 0, F.lit(cities[0][1]))
    lon_expr  = F.when(F.col("city_idx") == 0, F.lit(cities[0][2]))
    for i, (name, lat, lon) in enumerate(cities[1:], 1):
        city_expr = city_expr.when(F.col("city_idx") == i, F.lit(name))
        lat_expr  = lat_expr.when(F.col("city_idx") == i, F.lit(lat))
        lon_expr  = lon_expr.when(F.col("city_idx") == i, F.lit(lon))

    df = (
        df
        .withColumn("merchant_city", city_expr)
        .withColumn("merchant_lat", lat_expr)
        .withColumn("merchant_lon", lon_expr)
    )

    df = df.select(
        "reference_id", "merchant_name", "merchant_city",
        "merchant_lat", "merchant_lon", "mcc_category"
    )

    write_bronze(df, "merchant_locations", cfg)


# ── Main ───────────────────────────────────────────────────────────────
def main():
    cfg = Config()
    spark = build_spark(cfg)

    log.info("===== Bronze ingestion =====")
    log.info("Source: %s  |  MinIO: %s", cfg.JDBC_URL, cfg.bronze_path)

    ingest_tables(spark, cfg)
    create_merchant_locations(spark, cfg)

    # Summary
    for t in ["customers", "accounts", "transactions", "credit_scores",
              "app_events", "support_tickets", "acquisition_channels",
              "merchant_locations"]:
        df = spark.read.parquet(f"{cfg.bronze_path}{t}/")
        log.info("  bronze.%s → %d rows", t, df.count())

    log.info("===== Bronze ingestion complete =====")
    spark.stop()


if __name__ == "__main__":
    main()
