# =====================================================================
# SemestaBank Q1 — Unit tests for account_snapshot transformation logic
#
# Tests cover:
#   * build_snapshot: deduplication, status filter, aggregation correctness
#   * recon_balance: matching and mismatching balance reconciliation
#
# Run:  pip install pytest pyspark  &&  pytest tests/ -v
# (Requires Java 8/11 installed for PySpark's JVM backend.)
# =====================================================================

import sys
import os
from datetime import date
from decimal import Decimal

# Ensure Spark workers use the same Python as the driver
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pytest

# ── Make the spark/jobs directory importable ──
_jobs_dir = os.path.join(os.path.dirname(__file__), "..", "spark", "jobs")
sys.path.insert(0, os.path.abspath(_jobs_dir))

from account_snapshot import build_snapshot, recon_balance, DataQualityError  # noqa: E402

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, IntegerType, DateType,
)


# ── Spark session fixture (shared across all tests) ──

@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ── Helpers ──

ACCOUNTS_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("balance", DecimalType(18, 2), False),
])

TXN_SCHEMA = StructType([
    StructField("txn_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("txn_date", DateType(), False),
    StructField("txn_type", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("txn_day", DateType(), True),
])


def make_accounts(spark, rows):
    return spark.createDataFrame(rows, ACCOUNTS_SCHEMA)


def make_txns(spark, rows):
    return spark.createDataFrame(rows, TXN_SCHEMA)


class _MockConfig:
    DQ_FAIL_HARD = True


SNAP_DATE = date(2026, 1, 15)


# ── Test: deduplication (duplicate txn_ids are removed) ──

def test_build_snapshot_dedup_removes_duplicate_txn_ids(spark):
    accounts = make_accounts(spark, [
        ("ACC001", "CUST001", Decimal("100000")),
    ])
    txns = make_txns(spark, [
        ("TXN1", "ACC001", SNAP_DATE, "DEBIT", Decimal("10000"), "COMPLETED", "MOBILE_APP", SNAP_DATE),
        ("TXN1", "ACC001", SNAP_DATE, "DEBIT", Decimal("10000"), "COMPLETED", "MOBILE_APP", SNAP_DATE),
        ("TXN2", "ACC001", SNAP_DATE, "DEBIT", Decimal("5000"), "COMPLETED", "ATM", SNAP_DATE),
    ])

    snapshot, real_txns = build_snapshot(spark, accounts, txns, SNAP_DATE)

    assert snapshot.count() == 1
    row = snapshot.collect()[0]
    assert row.txn_count == 2


# ── Test: status filter (only COMPLETED transactions counted) ──

def test_build_snapshot_status_filter_excludes_non_completed(spark):
    accounts = make_accounts(spark, [
        ("ACC001", "CUST001", Decimal("100000")),
    ])
    txns = make_txns(spark, [
        ("TXN1", "ACC001", SNAP_DATE, "DEBIT", Decimal("10000"), "COMPLETED", "MOBILE_APP", SNAP_DATE),
        ("TXN2", "ACC001", SNAP_DATE, "DEBIT", Decimal("20000"), "FAILED", "ATM", SNAP_DATE),
        ("TXN3", "ACC001", SNAP_DATE, "DEBIT", Decimal("15000"), "PENDING", "MOBILE_APP", SNAP_DATE),
    ])

    snapshot, real_txns = build_snapshot(spark, accounts, txns, SNAP_DATE)

    assert real_txns.count() == 1
    row = snapshot.collect()[0]
    assert row.txn_count == 1
    assert Decimal(row.txn_total_amount) == Decimal("10000")


# ── Test: aggregation correctness (debit/credit/total/count/channels) ──

def test_build_snapshot_aggregation_correctness(spark):
    accounts = make_accounts(spark, [
        ("ACC001", "CUST001", Decimal("100000")),
        ("ACC002", "CUST002", Decimal("50000")),
    ])
    txns = make_txns(spark, [
        ("T1", "ACC001", SNAP_DATE, "DEBIT", Decimal("10000"), "COMPLETED", "ATM", SNAP_DATE),
        ("T2", "ACC001", SNAP_DATE, "CREDIT", Decimal("5000"), "COMPLETED", "MOBILE_APP", SNAP_DATE),
        ("T3", "ACC001", SNAP_DATE, "DEBIT", Decimal("3000"), "COMPLETED", "ATM", SNAP_DATE),
        ("T4", "ACC002", SNAP_DATE, "CREDIT", Decimal("20000"), "COMPLETED", "MOBILE_APP", SNAP_DATE),
    ])

    snapshot, _ = build_snapshot(spark, accounts, txns, SNAP_DATE)
    rows = {r.account_id: r for r in snapshot.collect()}

    a1 = rows["ACC001"]
    assert Decimal(a1.txn_total_amount) == Decimal("18000")
    assert a1.txn_count == 3
    assert Decimal(a1.debit_amount) == Decimal("13000")
    assert Decimal(a1.credit_amount) == Decimal("5000")
    assert a1.distinct_channels == 2  # ATM + MOBILE_APP

    a2 = rows["ACC002"]
    assert Decimal(a2.txn_total_amount) == Decimal("20000")
    assert a2.txn_count == 1
    assert Decimal(a2.credit_amount) == Decimal("20000")
    assert Decimal(a2.debit_amount) == Decimal("0")

    # Account with no transactions gets zero-filled
    assert Decimal(a2.debit_amount) == Decimal("0")


# ── Test: account with no transactions gets zero-filled ──

def test_build_snapshot_account_with_no_transactions(spark):
    accounts = make_accounts(spark, [
        ("ACC001", "CUST001", Decimal("100000")),
        ("ACC002", "CUST002", Decimal("50000")),
    ])
    txns = make_txns(spark, [
        ("T1", "ACC001", SNAP_DATE, "DEBIT", Decimal("10000"), "COMPLETED", "ATM", SNAP_DATE),
    ])

    snapshot, _ = build_snapshot(spark, accounts, txns, SNAP_DATE)
    rows = {r.account_id: r for r in snapshot.collect()}

    assert rows["ACC002"].txn_count == 0
    assert Decimal(rows["ACC002"].txn_total_amount) == Decimal("0")


# ── Test: recon_balance passes when balance matches computed_close_balance ──

def test_recon_balance_matches_when_balances_agree(spark):
    accounts = make_accounts(spark, [
        ("ACC001", "CUST001", Decimal("100000")),
    ])
    # recon_balance checks: abs(balance - computed_close_balance) < 1.0
    # computed_close_balance = balance - credit_amount + debit_amount
    # For balance == computed_close_balance: debit_amount must == credit_amount
    txns = make_txns(spark, [
        ("T1", "ACC001", SNAP_DATE, "DEBIT", Decimal("10000"), "COMPLETED", "ATM", SNAP_DATE),
        ("T2", "ACC001", SNAP_DATE, "CREDIT", Decimal("10000"), "COMPLETED", "MOBILE_APP", SNAP_DATE),
    ])

    snapshot, _ = build_snapshot(spark, accounts, txns, SNAP_DATE)

    # Should not raise
    recon_balance(snapshot, _MockConfig())


# ── Test: recon_balance passes when account has no transactions ──

def test_recon_balance_passes_when_no_transactions(spark):
    accounts = make_accounts(spark, [
        ("ACC001", "CUST001", Decimal("100000")),
    ])
    txns = make_txns(spark, [])

    snapshot, _ = build_snapshot(spark, accounts, txns, SNAP_DATE)

    # txn_count == 0 is an automatic pass (see recon_balance logic)
    recon_balance(snapshot, _MockConfig())


# ── Test: recon_balance raises when mismatch > 2% ──

def test_recon_balance_raises_on_large_mismatch(spark):
    accounts = make_accounts(spark, [
        ("ACC001", "CUST001", Decimal("100000")),
        ("ACC002", "CUST002", Decimal("100000")),
        ("ACC003", "CUST003", Decimal("100000")),
    ])
    # Make txn amounts that create large mismatches between balance and computed_close_balance
    # computed_close_balance = balance - credit_amount + debit_amount
    # For a mismatch: balance != computed_close_balance
    # i.e., the transactions don't reconcile with the balance
    txns = make_txns(spark, [
        ("T1", "ACC001", SNAP_DATE, "CREDIT", Decimal("10000"), "COMPLETED", "ATM", SNAP_DATE),
        ("T2", "ACC002", SNAP_DATE, "CREDIT", Decimal("10000"), "COMPLETED", "ATM", SNAP_DATE),
        ("T3", "ACC003", SNAP_DATE, "CREDIT", Decimal("10000"), "COMPLETED", "ATM", SNAP_DATE),
    ])

    snapshot, _ = build_snapshot(spark, accounts, txns, SNAP_DATE)

    # All 3 accounts have mismatch: balance=100000, computed=100000-10000+0=90000, diff=10000 > 1.0
    # mismatch = 3/3 = 100% > 2%
    with pytest.raises(DataQualityError, match="balance_recon"):
        recon_balance(snapshot, _MockConfig())


# ── Test: recon_balance does not raise when DQ_FAIL_HARD is False ──

def test_recon_balance_warn_only_when_not_hard_fail(spark):
    accounts = make_accounts(spark, [
        ("ACC001", "CUST001", Decimal("100000")),
        ("ACC002", "CUST002", Decimal("100000")),
    ])
    txns = make_txns(spark, [
        ("T1", "ACC001", SNAP_DATE, "CREDIT", Decimal("50000"), "COMPLETED", "ATM", SNAP_DATE),
        ("T2", "ACC002", SNAP_DATE, "CREDIT", Decimal("50000"), "COMPLETED", "ATM", SNAP_DATE),
    ])

    snapshot, _ = build_snapshot(spark, accounts, txns, SNAP_DATE)

    class SoftConfig:
        DQ_FAIL_HARD = False

    # Should not raise even though mismatch is 100%
    recon_balance(snapshot, SoftConfig())