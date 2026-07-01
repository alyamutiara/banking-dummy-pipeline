-- =====================================================================
-- SemestaBank — Q2 Regulatory Views — PRODUCTION variant
-- ---------------------------------------------------------------------
-- Production twin of the LOCAL PostgreSQL views at:
--   Final_Answer/local/q2_sql/sql/01_customer_health_scorecard.sql
--   Final_Answer/local/q2_sql/sql/02_fraud_detection.sql
--
-- Q2(f) optimization answer: the 45-minute nightly materialization is solved
-- by INCREMENTAL refresh. Three portable options are shown; pick by platform.
--
--   PLATFORM            MATERIALIZATION                       REFRESH
--   ------------------  ------------------------------------  -----------------
--   Databricks (Q3)     DLT / Materialized View              auto-incremental
--   Snowflake           DYNAMIC TABLE (TARGET_LAG)            auto-incremental
--   PostgreSQL (local)  MATERIALIZED VIEW + unique index      REFRESH CONCURRENTLY
--   dbt (multi-team)    {{ config(materialized='incremental') }}
--
-- The SELECT bodies are the SAME ANSI SQL as the local views (only the
-- CREATE wrapper and a couple of dialect functions change:
--   jsonb_build_object -> to_json(named_struct(...))   [Spark]
--   STRING_AGG         -> array_join(collect_set(...)) [Spark]
--   DATE_TRUNC         -> date_trunc                     [same]
-- =====================================================================

-- ╔═══════════════════════════════════════════════════════════════════╗
-- ║ OPTION A — DATABRICKS DELTA LIVE TABLES (recommended; Q3 platform) ║
-- ╚═══════════════════════════════════════════════════════════════════╝
-- A DLT pipeline runs this file. `LIVE.` references wire the lineage graph,
-- and Unity Catalog captures column-level lineage automatically (Q3 part i).
-- Only the CURRENT month is recomputed each night (the WHERE predicates make
-- the scan incremental), so the 45-min job drops to a few minutes.

CREATE OR REFRESH MATERIALIZED VIEW semestabank.gold.customer_health_scorecard
  COMMENT 'OJK monthly customer health scorecard — one row per (customer, month).'
  TBLPROPERTIES ('quality' = 'gold', 'pipelines.autoOptimize.zOrderCols' = 'customer_id')
AS
WITH monthly_balance AS (
  SELECT a.customer_id,
         date_trunc('month', current_date())                      AS scorecard_month,
         SUM(a.balance)                                            AS total_balance,
         SUM(CASE WHEN a.account_type = 'CREDIT_CARD' AND a.credit_limit > 0
                  THEN a.balance END)                              AS credit_card_balance,
         SUM(CASE WHEN a.account_type = 'CREDIT_CARD'
                  THEN a.credit_limit END)                         AS total_credit_limit
  FROM semestabank.bronze.accounts a
  WHERE a.status IN ('ACTIVE', 'DORMANT')
  GROUP BY a.customer_id
),
prev_month_balance AS (                       -- Q1→Q2 link: real month-end from silver Delta
  SELECT customer_id,
         date_trunc('month', add_months(current_date(), -1))      AS scorecard_month,
         SUM(balance)                                             AS total_balance,
         CAST(NULL AS DECIMAL(15,2))                              AS credit_card_balance,
         CAST(NULL AS DECIMAL(15,2))                              AS total_credit_limit
  FROM (
    SELECT customer_id, account_id, balance,
           ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY snapshot_date DESC) rn
    FROM semestabank.silver.account_snapshots
    WHERE snapshot_date >= date_trunc('month', add_months(current_date(), -1))
      AND snapshot_date <  date_trunc('month', current_date())
  ) s
  WHERE s.rn = 1
  GROUP BY customer_id
),
monthly_balance_history AS (
  SELECT customer_id, scorecard_month, total_balance, credit_card_balance, total_credit_limit,
         LAG(total_balance) OVER (PARTITION BY customer_id ORDER BY scorecard_month) AS prev_month_balance
  FROM (SELECT * FROM monthly_balance UNION ALL SELECT * FROM prev_month_balance)
),
monthly_txns AS (
  SELECT t.account_id, date_trunc('month', t.txn_date) AS txn_month,
         COUNT(CASE WHEN t.txn_type='DEBIT'        THEN 1 END) AS debit_count,
         COUNT(CASE WHEN t.txn_type='CREDIT'       THEN 1 END) AS credit_count,
         COUNT(CASE WHEN t.txn_type='TRANSFER_IN'  THEN 1 END) AS transfer_in_count,
         COUNT(CASE WHEN t.txn_type='TRANSFER_OUT' THEN 1 END) AS transfer_out_count,
         COUNT(CASE WHEN t.txn_type='PAYMENT'      THEN 1 END) AS payment_count,
         COUNT(CASE WHEN t.txn_type='FEE'          THEN 1 END) AS fee_count,
         SUM(CASE WHEN t.txn_type='DEBIT'  THEN t.amount ELSE 0 END) AS debit_amount,
         SUM(CASE WHEN t.txn_type='CREDIT' THEN t.amount ELSE 0 END) AS credit_amount,
         AVG(CASE WHEN t.channel='MOBILE_APP'  THEN t.amount END) AS avg_amt_mobile_app,
         AVG(CASE WHEN t.channel='WEB'         THEN t.amount END) AS avg_amt_web,
         AVG(CASE WHEN t.channel='QRIS'        THEN t.amount END) AS avg_amt_qris,
         AVG(CASE WHEN t.channel='ATM_NETWORK' THEN t.amount END) AS avg_amt_atm,
         AVG(CASE WHEN t.channel='AUTO_DEBIT'  THEN t.amount END) AS avg_amt_auto_debit
  FROM semestabank.bronze.transactions t
  WHERE t.status='COMPLETED'
    AND t.txn_date >= date_trunc('month', current_date())          -- incremental: current month only
    AND t.txn_date <  add_months(date_trunc('month', current_date()), 1)
  GROUP BY t.account_id, date_trunc('month', t.txn_date)
),
customer_monthly_txns AS (
  SELECT a.customer_id, mt.txn_month,
         SUM(mt.debit_count) debit_count, SUM(mt.credit_count) credit_count,
         SUM(mt.transfer_in_count) transfer_in_count, SUM(mt.transfer_out_count) transfer_out_count,
         SUM(mt.payment_count) payment_count, SUM(mt.fee_count) fee_count,
         SUM(mt.debit_amount) debit_amount, SUM(mt.credit_amount) credit_amount,
         AVG(mt.avg_amt_mobile_app) avg_amt_mobile_app, AVG(mt.avg_amt_web) avg_amt_web,
         AVG(mt.avg_amt_qris) avg_amt_qris, AVG(mt.avg_amt_atm) avg_amt_atm,
         AVG(mt.avg_amt_auto_debit) avg_amt_auto_debit
  FROM monthly_txns mt JOIN semestabank.bronze.accounts a ON mt.account_id = a.account_id
  GROUP BY a.customer_id, mt.txn_month
),
latest_credit_score AS (
  SELECT customer_id, probability_of_default, credit_score FROM (
    SELECT customer_id, probability_of_default, credit_score,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY score_date DESC) rn
    FROM semestabank.bronze.credit_scores) WHERE rn = 1
),
balance_with_lag AS (
  SELECT customer_id, scorecard_month, total_balance,
         total_balance - prev_month_balance AS mom_balance_change,
         CASE WHEN prev_month_balance IS NOT NULL AND prev_month_balance <> 0
              THEN ROUND((total_balance - prev_month_balance)/NULLIF(prev_month_balance,0)*100, 2) END AS mom_balance_change_pct,
         CASE WHEN total_credit_limit > 0
              THEN ROUND(credit_card_balance/total_credit_limit*100, 2) END AS credit_utilization_pct
  FROM monthly_balance_history
  WHERE scorecard_month = date_trunc('month', current_date())
)
SELECT b.customer_id, c.full_name, c.segment, b.scorecard_month, b.total_balance,
       b.mom_balance_change, b.mom_balance_change_pct, b.credit_utilization_pct,
       COALESCE(cmt.debit_count,0) debit_count, COALESCE(cmt.credit_count,0) credit_count,
       COALESCE(cmt.transfer_in_count,0) transfer_in_count, COALESCE(cmt.transfer_out_count,0) transfer_out_count,
       COALESCE(cmt.payment_count,0) payment_count, COALESCE(cmt.fee_count,0) fee_count,
       COALESCE(cmt.debit_amount,0) debit_amount, COALESCE(cmt.credit_amount,0) credit_amount,
       cmt.avg_amt_mobile_app, cmt.avg_amt_web, cmt.avg_amt_qris, cmt.avg_amt_atm, cmt.avg_amt_auto_debit,
       lcs.credit_score, lcs.probability_of_default,
       CASE WHEN b.credit_utilization_pct > 80 THEN TRUE
            WHEN lcs.probability_of_default > 0.3 THEN TRUE
            WHEN b.mom_balance_change_pct < -30 THEN TRUE
            ELSE FALSE END AS risk_flag
FROM balance_with_lag b
JOIN semestabank.bronze.customers c ON b.customer_id = c.customer_id
LEFT JOIN customer_monthly_txns cmt ON cmt.customer_id = b.customer_id AND cmt.txn_month = b.scorecard_month
LEFT JOIN latest_credit_score lcs ON lcs.customer_id = b.customer_id;


CREATE OR REFRESH MATERIALIZED VIEW semestabank.gold.fraud_detection_alerts
  COMMENT 'Fraud alerts — one row per (customer, alert_type, alert_date).'
AS
WITH txn_enriched AS (
  SELECT t.txn_id, t.account_id, a.customer_id, t.txn_date, t.amount, t.channel,
         ml.merchant_city, CAST(t.txn_date AS DATE) AS txn_day
  FROM semestabank.bronze.transactions t
  JOIN semestabank.bronze.accounts a ON t.account_id = a.account_id
  LEFT JOIN semestabank.bronze.merchant_locations ml ON t.reference_id = ml.reference_id
  WHERE t.status = 'COMPLETED'
),
high_frequency AS (
  SELECT customer_id, txn_day AS alert_date, COUNT(*) AS txns_in_max_window
  FROM (
    SELECT customer_id, txn_day,
           COUNT(*) OVER (PARTITION BY customer_id ORDER BY CAST(txn_date AS TIMESTAMP)
                          RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS rolling_1h_count
    FROM txn_enriched)
  WHERE rolling_1h_count >= 5
  GROUP BY customer_id, txn_day
),
multi_city AS (
  SELECT customer_id, txn_day AS alert_date,
         COUNT(DISTINCT merchant_city) AS distinct_cities,
         array_join(collect_set(merchant_city), ', ') AS city_list
  FROM txn_enriched WHERE merchant_city IS NOT NULL
  GROUP BY customer_id, txn_day HAVING COUNT(DISTINCT merchant_city) >= 3
),
amount_anomaly AS (
  SELECT customer_id, txn_day AS alert_date, txn_id, amount, rolling_30d_avg
  FROM (
    SELECT customer_id, txn_id, txn_day, amount,
           AVG(amount) OVER (PARTITION BY customer_id ORDER BY txn_date
                             ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS rolling_30d_avg
    FROM txn_enriched)
  WHERE rolling_30d_avg IS NOT NULL AND rolling_30d_avg > 0
    AND amount > 3 * rolling_30d_avg
    AND amount > 1000000   -- absolute floor: prevents micro-baseline ratio explosion
)
SELECT customer_id, 'HIGH_FREQUENCY' AS alert_type, alert_date,
       to_json(named_struct('txns_in_max_window', txns_in_max_window,
                            'description', '5+ transactions within a 1-hour window')) AS details_json
FROM high_frequency
UNION ALL
SELECT customer_id, 'MULTI_CITY', alert_date,
       to_json(named_struct('distinct_cities', distinct_cities, 'city_list', city_list,
                            'description', 'Transactions in 3+ different cities on the same day'))
FROM multi_city
UNION ALL
SELECT customer_id, 'AMOUNT_ANOMALY', alert_date,
       to_json(named_struct('txn_id', txn_id, 'amount', amount,
                            'rolling_30d_avg', ROUND(rolling_30d_avg,2),
                            'multiple_of_avg', ROUND(amount/rolling_30d_avg,2),
                            'description', 'Single transaction exceeds 3x rolling 30-transaction average'))
FROM amount_anomaly;


-- ╔═══════════════════════════════════════════════════════════════════╗
-- ║ OPTION B — SNOWFLAKE DYNAMIC TABLE (if BI standardizes on Snowflake)║
-- ╚═══════════════════════════════════════════════════════════════════╝
-- Same SELECT body; Snowflake computes the incremental delta automatically
-- and keeps the table at most TARGET_LAG behind the base tables.
--
-- CREATE OR REPLACE DYNAMIC TABLE gold.customer_health_scorecard
--   TARGET_LAG = '1 hour'
--   WAREHOUSE  = wh_regulatory
-- AS <same SELECT as above, with Snowflake JSON: OBJECT_CONSTRUCT(...) >;

-- ╔═══════════════════════════════════════════════════════════════════╗
-- ║ OPTION C — POSTGRES MATERIALIZED VIEW (what the LOCAL stack uses)   ║
-- ╚═══════════════════════════════════════════════════════════════════╝
-- See Final_Answer/local/q2_sql/sql/03_optimization_strategy.sql —
-- CREATE MATERIALIZED VIEW + UNIQUE index + REFRESH MATERIALIZED VIEW
-- CONCURRENTLY (non-blocking nightly refresh).
