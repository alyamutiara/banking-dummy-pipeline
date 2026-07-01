# =====================================================================
# SemestaBank — Q2(a): Monthly Customer Health Scorecard (Lakehouse)
# Spark SQL: MinIO/bronze + MinIO/silver → MinIO/gold
#
# Same SQL logic as the PostgreSQL version, adapted for Spark SQL.
# Reads clean silver data for account balances and bronze for
# transaction-level detail and credit scores.
# =====================================================================

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | GOLD-SC | %(message)s",
)
log = logging.getLogger("gold_scorecard")


class Config:
    S3_ENDPOINT    = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    S3_BRONZE      = os.environ.get("S3_BUCKET_BRONZE", "semestabank-bronze")
    S3_SILVER_BKT  = os.environ.get("S3_BUCKET", "semestabank-silver")
    S3_SILVER_PATH = os.environ.get("S3_PATH", "account_snapshots")
    S3_GOLD        = os.environ.get("S3_BUCKET_GOLD", "semestabank-gold")
    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    @property
    def bronze_accounts(self):     return f"s3a://{self.S3_BRONZE}/accounts/"
    @property
    def bronze_transactions(self): return f"s3a://{self.S3_BRONZE}/transactions/"
    @property
    def bronze_customers(self):    return f"s3a://{self.S3_BRONZE}/customers/"
    @property
    def bronze_credit_scores(self): return f"s3a://{self.S3_BRONZE}/credit_scores/"
    @property
    def silver_snapshots(self):    return f"s3a://{self.S3_SILVER_BKT}/{self.S3_SILVER_PATH}/"
    @property
    def gold_scorecard(self):      return f"s3a://{self.S3_GOLD}/customer_health_scorecard/"


def build_spark(cfg: Config) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("semestabank_gold_scorecard")
        .config("spark.sql.shuffle.partitions", "64")
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


def run(spark, cfg: Config):
    # ── Register bronze + silver tables as temp views ──
    spark.read.parquet(cfg.bronze_accounts).createOrReplaceTempView("bronze_accounts")
    spark.read.parquet(cfg.bronze_transactions).createOrReplaceTempView("bronze_transactions")
    spark.read.parquet(cfg.bronze_customers).createOrReplaceTempView("bronze_customers")
    spark.read.parquet(cfg.bronze_credit_scores).createOrReplaceTempView("bronze_credit_scores")
    spark.read.parquet(cfg.silver_snapshots).createOrReplaceTempView("silver_account_snapshots")

    log.info("Temp views registered. Running scorecard SQL...")

    # ── Spark SQL (same CTE structure as the PostgreSQL version) ──
    sql = """
    WITH
    -- CTE 1: current month balance per customer (from bronze, clean data)
    monthly_balance AS (
        SELECT
            a.customer_id,
            DATE_TRUNC('MONTH', CURRENT_DATE()) AS scorecard_month,
            SUM(a.balance)                                       AS total_balance,
            SUM(CASE WHEN a.account_type = 'CREDIT_CARD'
                      AND a.credit_limit > 0
                     THEN a.balance END)                          AS credit_card_balance,
            SUM(CASE WHEN a.account_type = 'CREDIT_CARD'
                     THEN a.credit_limit END)                     AS total_credit_limit,
            -- MRR components (OJK business rules)
            COUNT(CASE WHEN a.account_type = 'SAVINGS'
                        AND a.balance < 1000000
                       THEN 1 END)                                AS savings_fee_accounts,
            SUM(CASE WHEN a.account_type = 'CREDIT_CARD'
                      THEN COALESCE(a.credit_limit, 0) * 0.0003
                      ELSE 0 END)                                 AS credit_card_monthly_fee,
            SUM(CASE WHEN a.account_type = 'LOAN'
                      THEN a.balance * COALESCE(a.interest_rate, 0) / 12
                      ELSE 0 END)                                 AS loan_interest_income
        FROM bronze_accounts a
        WHERE a.status IN ('ACTIVE', 'DORMANT')
        GROUP BY a.customer_id
    ),

    -- CTE 2: previous month balance from silver snapshots (for MoM)
    -- Take each account's MONTH-END balance (latest snapshot_date in the
    -- prior month) then sum per customer. Summing every daily snapshot
    -- would over-count the balance ~30x and corrupt the MoM change.
    prev_month_balance AS (
        SELECT
            s.customer_id,
            TRUNC(ADD_MONTHS(CURRENT_DATE(), -1), 'MONTH') AS scorecard_month,
            SUM(s.balance) AS total_balance
        FROM (
            SELECT
                customer_id,
                account_id,
                balance,
                ROW_NUMBER() OVER (
                    PARTITION BY account_id ORDER BY snapshot_date DESC
                ) AS rn
            FROM silver_account_snapshots
            WHERE snapshot_date >= TRUNC(ADD_MONTHS(CURRENT_DATE(), -1), 'MONTH')
              AND snapshot_date <  TRUNC(CURRENT_DATE(), 'MONTH')
        ) s
        WHERE s.rn = 1
        GROUP BY s.customer_id
    ),

    -- CTE 3: union current + previous for LAG
    balance_history AS (
        SELECT customer_id, scorecard_month, total_balance, credit_card_balance,
               total_credit_limit, savings_fee_accounts, credit_card_monthly_fee,
               loan_interest_income
        FROM monthly_balance
        UNION ALL
        SELECT customer_id, scorecard_month, total_balance,
               CAST(NULL AS DECIMAL(15,2)) AS credit_card_balance,
               CAST(NULL AS DECIMAL(15,2)) AS total_credit_limit,
               CAST(NULL AS BIGINT)       AS savings_fee_accounts,
               CAST(NULL AS DECIMAL(15,2)) AS credit_card_monthly_fee,
               CAST(NULL AS DECIMAL(15,2)) AS loan_interest_income
        FROM prev_month_balance
    ),

    -- CTE 4: LAG for MoM comparison
    balance_with_lag AS (
        SELECT
            customer_id,
            scorecard_month,
            total_balance,
            credit_card_balance,
            total_credit_limit,
            savings_fee_accounts,
            credit_card_monthly_fee,
            loan_interest_income,
            LAG(total_balance) OVER (
                PARTITION BY customer_id ORDER BY scorecard_month
            ) AS prev_month_balance
        FROM balance_history
    ),

    -- CTE 5: balance with derived MoM and utilization
    balance_final AS (
        SELECT
            customer_id,
            scorecard_month,
            total_balance,
            total_balance - prev_month_balance AS mom_balance_change,
            CASE WHEN prev_month_balance IS NOT NULL AND prev_month_balance != 0
                 THEN ROUND((total_balance - prev_month_balance) / prev_month_balance * 100, 2)
                 END AS mom_balance_change_pct,
            CASE WHEN total_credit_limit > 0
                 THEN ROUND(credit_card_balance / total_credit_limit * 100, 2)
                 END AS credit_utilization_pct,
            -- MRR: Rp 10,000 per savings account under Rp 1M
            COALESCE(savings_fee_accounts, 0) * 10000 AS monthly_savings_fee,
            COALESCE(credit_card_monthly_fee, 0)      AS monthly_credit_card_fee,
            COALESCE(loan_interest_income, 0)         AS monthly_loan_interest,
            COALESCE(savings_fee_accounts, 0) * 10000
                + COALESCE(credit_card_monthly_fee, 0)
                + COALESCE(loan_interest_income, 0)   AS estimated_mrr
        FROM balance_with_lag
        WHERE scorecard_month = DATE_TRUNC('MONTH', CURRENT_DATE())
    ),

    -- CTE 6: monthly transaction aggregates per account
    monthly_txns AS (
        SELECT
            t.account_id,
            DATE_TRUNC('MONTH', t.txn_date) AS txn_month,
            COUNT(CASE WHEN t.txn_type = 'DEBIT'        THEN 1 END) AS debit_count,
            COUNT(CASE WHEN t.txn_type = 'CREDIT'       THEN 1 END) AS credit_count,
            COUNT(CASE WHEN t.txn_type = 'TRANSFER_IN'  THEN 1 END) AS transfer_in_count,
            COUNT(CASE WHEN t.txn_type = 'TRANSFER_OUT' THEN 1 END) AS transfer_out_count,
            COUNT(CASE WHEN t.txn_type = 'PAYMENT'      THEN 1 END) AS payment_count,
            COUNT(CASE WHEN t.txn_type = 'FEE'          THEN 1 END) AS fee_count,
            SUM(CASE WHEN t.txn_type = 'DEBIT'  THEN t.amount ELSE 0 END) AS debit_amount,
            SUM(CASE WHEN t.txn_type = 'CREDIT' THEN t.amount ELSE 0 END) AS credit_amount,
            AVG(CASE WHEN t.channel = 'MOBILE_APP'  THEN t.amount END) AS avg_amt_mobile_app,
            AVG(CASE WHEN t.channel = 'WEB'         THEN t.amount END) AS avg_amt_web,
            AVG(CASE WHEN t.channel = 'QRIS'        THEN t.amount END) AS avg_amt_qris,
            AVG(CASE WHEN t.channel = 'ATM_NETWORK' THEN t.amount END) AS avg_amt_atm,
            AVG(CASE WHEN t.channel = 'AUTO_DEBIT'  THEN t.amount END) AS avg_amt_auto_debit
        FROM bronze_transactions t
        WHERE t.status = 'COMPLETED'
          AND t.txn_date >= DATE_TRUNC('MONTH', CURRENT_DATE())
          AND t.txn_date <  TRUNC(ADD_MONTHS(CURRENT_DATE(), 1), 'MONTH')
        GROUP BY t.account_id, DATE_TRUNC('MONTH', t.txn_date)
    ),

    -- CTE 7: join transactions back to customer
    customer_monthly_txns AS (
        SELECT
            a.customer_id,
            mt.txn_month,
            SUM(mt.debit_count)          AS debit_count,
            SUM(mt.credit_count)         AS credit_count,
            SUM(mt.transfer_in_count)    AS transfer_in_count,
            SUM(mt.transfer_out_count)   AS transfer_out_count,
            SUM(mt.payment_count)        AS payment_count,
            SUM(mt.fee_count)            AS fee_count,
            SUM(mt.debit_amount)         AS debit_amount,
            SUM(mt.credit_amount)        AS credit_amount,
            AVG(mt.avg_amt_mobile_app)   AS avg_amt_mobile_app,
            AVG(mt.avg_amt_web)          AS avg_amt_web,
            AVG(mt.avg_amt_qris)         AS avg_amt_qris,
            AVG(mt.avg_amt_atm)          AS avg_amt_atm,
            AVG(mt.avg_amt_auto_debit)   AS avg_amt_auto_debit
        FROM monthly_txns mt
        JOIN bronze_accounts a ON mt.account_id = a.account_id
        GROUP BY a.customer_id, mt.txn_month
    ),

    -- CTE 8: latest credit score per customer
    latest_credit_score AS (
        SELECT
            customer_id,
            probability_of_default,
            credit_score
        FROM (
            SELECT
                customer_id,
                probability_of_default,
                credit_score,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY score_date DESC) AS rn
            FROM bronze_credit_scores
        ) ranked
        WHERE rn = 1
    )

    -- Final SELECT — assemble the scorecard
    SELECT
        b.customer_id,
        c.full_name,
        c.segment,
        b.scorecard_month,
        b.total_balance,
        b.mom_balance_change,
        b.mom_balance_change_pct,
        b.credit_utilization_pct,
        b.monthly_savings_fee,
        b.monthly_credit_card_fee,
        b.monthly_loan_interest,
        b.estimated_mrr,
        COALESCE(cmt.debit_count, 0)        AS debit_count,
        COALESCE(cmt.credit_count, 0)       AS credit_count,
        COALESCE(cmt.transfer_in_count, 0)  AS transfer_in_count,
        COALESCE(cmt.transfer_out_count, 0) AS transfer_out_count,
        COALESCE(cmt.payment_count, 0)      AS payment_count,
        COALESCE(cmt.fee_count, 0)          AS fee_count,
        COALESCE(cmt.debit_amount, 0)       AS debit_amount,
        COALESCE(cmt.credit_amount, 0)      AS credit_amount,
        cmt.avg_amt_mobile_app,
        cmt.avg_amt_web,
        cmt.avg_amt_qris,
        cmt.avg_amt_atm,
        cmt.avg_amt_auto_debit,
        lcs.credit_score,
        lcs.probability_of_default,
        CASE
            WHEN b.credit_utilization_pct IS NOT NULL AND b.credit_utilization_pct > 80
            THEN TRUE
            WHEN lcs.probability_of_default IS NOT NULL AND lcs.probability_of_default > 0.3
            THEN TRUE
            WHEN b.mom_balance_change_pct IS NOT NULL AND b.mom_balance_change_pct < -30
            THEN TRUE
            ELSE FALSE
        END AS risk_flag
    FROM balance_final b
    JOIN bronze_customers c        ON b.customer_id = c.customer_id
    LEFT JOIN customer_monthly_txns cmt
           ON cmt.customer_id = b.customer_id
          AND cmt.txn_month    = b.scorecard_month
    LEFT JOIN latest_credit_score lcs
           ON lcs.customer_id = b.customer_id
    """

    gold_df = spark.sql(sql)
    return gold_df


def main():
    cfg = Config()
    spark = build_spark(cfg)

    start = datetime.utcnow()
    gold_df = run(spark, cfg)
    gold_df.cache()

    count = gold_df.count()
    log.info("Gold scorecard rows: %d", count)

    output = cfg.gold_scorecard
    log.info("Writing gold.customer_health_scorecard → %s", output)
    gold_df.coalesce(4).write.mode("overwrite").parquet(output)

    elapsed = (datetime.utcnow() - start).total_seconds()
    log.info("SUCCESS | scorecard rows=%d | elapsed=%.1fs", count, elapsed)
    spark.stop()


if __name__ == "__main__":
    main()
