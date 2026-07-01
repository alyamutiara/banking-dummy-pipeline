-- =====================================================================
-- Q2(a): Monthly Customer Health Scorecard
-- SQL (PostgreSQL), CTEs, Window Functions (LAG), Conditional Aggregation
--
-- Purpose: monthly regulatory report for OJK — one row per customer per month.
-- Grain:   (customer_id, scorecard_month)
--
-- Q1→Q2 link: the month-over-month balance change is computed from REAL
-- history — `silver.account_snapshots` (Q1's clean, deduplicated daily
-- snapshot, partitioned by snapshot_date) — never a fabricated factor.
-- Prerequisite: silver.account_snapshots must be populated (see
-- ../seed/silver_account_snapshots.sql for a runnable local stand-in).
--
-- Scorecard metrics (all required by the test):
--   1. total_balance          — sum of all account balances
--   2. mom_balance_change     — month-over-month delta (via LAG)
--   3. mom_balance_change_pct — percentage change for the risk_flag
--   4. txn counts per type    — conditional aggregation (PIVOT-style)
--   5. avg_txn_amount per channel — one column per channel
--   6. credit_utilization     — balance / credit_limit for credit cards
--   7. risk_flag               — TRUE if any risk condition fires
--
-- Optimization:
--   - Every CBE is a straight GROUP BY on indexed columns.
--   - The LAG window function uses a PARTITION BY + ORDER BY on a
--     narrow intermediate result, not the full 60M-row transaction table.
--   - Conditional aggregation (SUM(CASE WHEN ...)) avoids self-joins.
--   - The final SELECT only reads from the pre-aggregated CTEs, so the
--     heavy work happens once per month, not per output column.
-- =====================================================================

CREATE OR REPLACE VIEW gold.customer_health_scorecard AS

WITH
-- ===================================================================
-- CTE 1: monthly balance per customer
-- One row per (customer, month). This is the grain for the scorecard.
-- ===================================================================
monthly_balance AS (
    SELECT
        a.customer_id,
        DATE_TRUNC('month', CURRENT_DATE) AS scorecard_month,
        SUM(a.balance)                                     AS total_balance,
        SUM(CASE WHEN a.account_type = 'CREDIT_CARD'
                 AND a.credit_limit > 0
                 THEN a.balance
            END)                                            AS credit_card_balance,
        SUM(CASE WHEN a.account_type = 'CREDIT_CARD'
                 THEN a.credit_limit
            END)                                            AS total_credit_limit
    FROM bronze.accounts a
    WHERE a.status IN ('ACTIVE', 'DORMANT')             -- exclude CLOSED/SUSPENDED
    GROUP BY a.customer_id, DATE_TRUNC('month', CURRENT_DATE)
),

-- ===================================================================
-- CTE 2: previous month balance — REAL prior balance from Q1's silver
-- This is the genuine Q1→Q2 link: Q1 produces silver.account_snapshots
-- (one clean, deduplicated row per account per day, partitioned by
-- snapshot_date).  We take each account's *month-end* balance for the
-- previous month (the latest snapshot_date in that month) and sum it per
-- customer — never a fabricated number.
-- ===================================================================
prev_month_balance AS (
    SELECT
        s.customer_id,
        DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AS scorecard_month,
        SUM(s.balance)        AS total_balance,
        NULL::numeric         AS credit_card_balance,
        NULL::numeric         AS total_credit_limit
    FROM (
        SELECT
            customer_id,
            account_id,
            balance,
            ROW_NUMBER() OVER (
                PARTITION BY account_id
                ORDER BY snapshot_date DESC      -- latest day in the month = month-end
            ) AS rn
        FROM silver.account_snapshots
        WHERE snapshot_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
          AND snapshot_date <  DATE_TRUNC('month', CURRENT_DATE)
    ) s
    WHERE s.rn = 1                               -- one month-end balance per account
    GROUP BY s.customer_id
),

-- ===================================================================
-- CTE 3: stitch current + previous month, then LAG over real history
-- Two rows per customer (this month, last month) so LAG can look back
-- exactly one month against genuine balances.
-- ===================================================================
monthly_balance_history AS (
    SELECT
        customer_id,
        scorecard_month,
        total_balance,
        credit_card_balance,
        total_credit_limit,
        LAG(total_balance) OVER (
            PARTITION BY customer_id
            ORDER BY scorecard_month
        ) AS prev_month_balance
    FROM (
        SELECT customer_id, scorecard_month, total_balance,
               credit_card_balance, total_credit_limit
        FROM monthly_balance
        UNION ALL
        SELECT customer_id, scorecard_month, total_balance,
               credit_card_balance, total_credit_limit
        FROM prev_month_balance
    ) stitched
),

-- ===================================================================
-- CTE 4: monthly transaction aggregates per customer
-- Conditional aggregation = pivoting without the PIVOT keyword.
-- One pass over transactions, one row per customer per month.
-- ===================================================================
monthly_txns AS (
    SELECT
        t.account_id,
        DATE_TRUNC('month', t.txn_date) AS txn_month,
        -- counts by type
        COUNT(CASE WHEN t.txn_type = 'DEBIT'        THEN 1 END)  AS debit_count,
        COUNT(CASE WHEN t.txn_type = 'CREDIT'       THEN 1 END)  AS credit_count,
        COUNT(CASE WHEN t.txn_type = 'TRANSFER_IN'  THEN 1 END)  AS transfer_in_count,
        COUNT(CASE WHEN t.txn_type = 'TRANSFER_OUT' THEN 1 END)  AS transfer_out_count,
        COUNT(CASE WHEN t.txn_type = 'PAYMENT'      THEN 1 END)  AS payment_count,
        COUNT(CASE WHEN t.txn_type = 'FEE'          THEN 1 END)  AS fee_count,
        -- sums by type
        SUM(CASE WHEN t.txn_type = 'DEBIT'        THEN t.amount ELSE 0 END)  AS debit_amount,
        SUM(CASE WHEN t.txn_type = 'CREDIT'       THEN t.amount ELSE 0 END)  AS credit_amount,
        -- avg amount per channel (conditional avg inside conditional sum/count)
        AVG(CASE WHEN t.channel = 'MOBILE_APP'   THEN t.amount END)  AS avg_amt_mobile_app,
        AVG(CASE WHEN t.channel = 'WEB'           THEN t.amount END)  AS avg_amt_web,
        AVG(CASE WHEN t.channel = 'QRIS'          THEN t.amount END)  AS avg_amt_qris,
        AVG(CASE WHEN t.channel = 'ATM_NETWORK'   THEN t.amount END)  AS avg_amt_atm,
        AVG(CASE WHEN t.channel = 'AUTO_DEBIT'    THEN t.amount END)  AS avg_amt_auto_debit
    FROM bronze.transactions t
    WHERE t.status = 'COMPLETED'
      AND t.txn_date >= DATE_TRUNC('month', CURRENT_DATE)
      AND t.txn_date <  DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
    GROUP BY t.account_id, DATE_TRUNC('month', t.txn_date)
),

-- ===================================================================
-- CTE 5: join transactions (per account) back to customer
-- ===================================================================
customer_monthly_txns AS (
    SELECT
        a.customer_id,
        mt.txn_month,
        SUM(mt.debit_count)         AS debit_count,
        SUM(mt.credit_count)        AS credit_count,
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
    JOIN bronze.accounts a ON mt.account_id = a.account_id
    GROUP BY a.customer_id, mt.txn_month
),

-- ===================================================================
-- CTE 6: latest credit score per customer (for probability_of_default)
-- ===================================================================
latest_credit_score AS (
    SELECT DISTINCT ON (cs.customer_id)
        cs.customer_id,
        cs.probability_of_default,
        cs.credit_score
    FROM bronze.credit_scores cs
    ORDER BY cs.customer_id, cs.score_date DESC
),

-- ===================================================================
-- CTE 7: balance history with LAG applied
-- Keep only the current-month row: it now carries the real previous-month
-- balance via LAG, so the prior-month helper row has done its job and is
-- filtered out (one output row per customer).
-- ===================================================================
balance_with_lag AS (
    SELECT
        mbh.customer_id,
        mbh.scorecard_month,
        mbh.total_balance,
        mbh.total_balance - mbh.prev_month_balance            AS mom_balance_change,
        CASE WHEN mbh.prev_month_balance IS NOT NULL
              AND mbh.prev_month_balance <> 0
             THEN ROUND(
                  (mbh.total_balance - mbh.prev_month_balance)
                  / NULLIF(mbh.prev_month_balance, 0) * 100, 2)
             END                                                AS mom_balance_change_pct,
        CASE WHEN mbh.total_credit_limit > 0
             THEN ROUND(mbh.credit_card_balance
                        / mbh.total_credit_limit * 100, 2)
             END                                                AS credit_utilization_pct
    FROM monthly_balance_history mbh
    WHERE mbh.scorecard_month = DATE_TRUNC('month', CURRENT_DATE)
)

-- ===================================================================
-- Final SELECT — assemble the scorecard
-- ===================================================================
SELECT
    b.customer_id,
    c.full_name,
    c.segment,
    b.scorecard_month,
    b.total_balance,
    b.mom_balance_change,
    b.mom_balance_change_pct,
    b.credit_utilization_pct,
    -- transaction counts by type (conditional aggregation -> pivoted columns)
    COALESCE(cmt.debit_count, 0)       AS debit_count,
    COALESCE(cmt.credit_count, 0)      AS credit_count,
    COALESCE(cmt.transfer_in_count, 0) AS transfer_in_count,
    COALESCE(cmt.transfer_out_count, 0) AS transfer_out_count,
    COALESCE(cmt.payment_count, 0)    AS payment_count,
    COALESCE(cmt.fee_count, 0)         AS fee_count,
    COALESCE(cmt.debit_amount, 0)      AS debit_amount,
    COALESCE(cmt.credit_amount, 0)     AS credit_amount,
    -- average transaction amount per channel
    cmt.avg_amt_mobile_app,
    cmt.avg_amt_web,
    cmt.avg_amt_qris,
    cmt.avg_amt_atm,
    cmt.avg_amt_auto_debit,
    -- credit score info
    lcs.credit_score,
    lcs.probability_of_default,
    -- =================================================================
    -- risk_flag — TRUE if ANY of the following conditions hold:
    --   1. credit_utilization > 80%
    --   2. probability_of_default > 0.3
    --   3. balance declined > 30% month-over-month
    -- =================================================================
    CASE
        WHEN b.credit_utilization_pct IS NOT NULL
             AND b.credit_utilization_pct > 80
        THEN TRUE
        WHEN lcs.probability_of_default IS NOT NULL
             AND lcs.probability_of_default > 0.3
        THEN TRUE
        WHEN b.mom_balance_change_pct IS NOT NULL
             AND b.mom_balance_change_pct < -30
        THEN TRUE
        ELSE FALSE
    END AS risk_flag
FROM balance_with_lag b
JOIN bronze.customers c        ON b.customer_id = c.customer_id
LEFT JOIN customer_monthly_txns cmt
       ON cmt.customer_id = b.customer_id
      AND cmt.txn_month  = b.scorecard_month
LEFT JOIN latest_credit_score lcs
       ON lcs.customer_id = b.customer_id;