-- =====================================================================
-- Q2(b): Fraud Detection — Potentially fraudulent transaction patterns
-- SQL (PostgreSQL), Window Functions (RANGE/ROWS), JSONB output
--
-- Purpose: scan transactions for three fraud patterns and produce alerts.
-- Grain:   one row per (customer_id, alert_type, alert_date)
--
-- Three patterns:
--   1. 5+ transactions within any 1-hour window  (RANGE BETWEEN ...)
--   2. 3+ different cities on the same day       (join to merchant_locations)
--   3. single_txn > 3x rolling 30-txn avg amount (rolling AVG window, excludes current row)
--
-- Output:  customer_id | alert_type | alert_date | details_json (JSONB)
--
-- Design: each pattern is an independent CTE producing its own alert rows.
--         UNION ALL combines them. Each CTE uses a window function as required.
-- =====================================================================

CREATE OR REPLACE VIEW gold.fraud_detection_alerts AS

WITH
-- ===================================================================
-- CTE 0: transactions enriched with customer_id and merchant city
-- ===================================================================
txn_enriched AS (
    SELECT
        t.txn_id,
        t.account_id,
        a.customer_id,
        t.txn_date,
        t.txn_type,
        t.amount,
        t.channel,
        t.merchant_category,
        ml.merchant_city,
        DATE(t.txn_date)   AS txn_day
    FROM bronze.transactions t
    JOIN bronze.accounts a   ON t.account_id = a.account_id
    LEFT JOIN bronze.merchant_locations ml  -- supplementary table (see seed/)
                                 ON t.reference_id = ml.reference_id
    WHERE t.status = 'COMPLETED'
),

-- ===================================================================
-- PATTERN 1: 5+ transactions within any rolling 1-hour window
--
-- Approach: COUNT(*) OVER (PARTITION BY customer ORDER BY txn_date
--           RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW)
-- RANGE (not ROWS) means the window is a time interval, not a row count.
-- If the count >= 5, flag the customer+day.
-- ===================================================================
high_frequency AS (
    SELECT
        customer_id,
        txn_day            AS alert_date,
        COUNT(*)           AS txns_in_max_window
    FROM (
        SELECT
            customer_id,
            txn_date,
            txn_day,
            COUNT(*) OVER (
                PARTITION BY customer_id
                ORDER BY txn_date
                RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
            ) AS rolling_1h_count
        FROM txn_enriched
    ) windowed
    WHERE rolling_1h_count >= 5
    GROUP BY customer_id, txn_day
),

-- ===================================================================
-- PATTERN 2: 3+ different merchant cities on the same day
--
-- Approach: COUNT(DISTINCT merchant_city) per (customer, day).
-- If >= 3, flag.
-- ===================================================================
multi_city AS (
    SELECT
        customer_id,
        txn_day AS alert_date,
        COUNT(DISTINCT merchant_city) AS distinct_cities,
        STRING_AGG(DISTINCT merchant_city, ', ') AS city_list
    FROM txn_enriched
    WHERE merchant_city IS NOT NULL
    GROUP BY customer_id, txn_day
    HAVING COUNT(DISTINCT merchant_city) >= 3
),

-- ===================================================================
-- PATTERN 3: single transaction > 3x rolling average of prior transactions
--
-- Approach: AVG(amount) OVER (PARTITION BY customer ORDER BY txn_date
--           ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) gives the rolling
--           average of the last 30 transactions, EXCLUDING the current row.
--           Excluding CURRENT ROW prevents the anomalous transaction from
--           inflating its own baseline — a cleaner comparator.
--
-- Trade-off: The spec says "30-day average." RANGE BETWEEN INTERVAL '30 days'
--           PRECEDING is the calendar-aware alternative. We use ROWS because:
--             1. The dataset has near-uniform daily txn frequency, so ROWS ≈ RANGE.
--             2. ROWS is universally supported (RANGE on INTERVAL needs Spark 3.3+).
--           If txn frequency becomes irregular, switch to:
--             RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND 1 PRECEDING
--
-- Tuning note: amount > 1_000_000 floor prevents the ratio from exploding
-- when a customer's rolling_30d_avg is tiny (common in high-variance synthetic
-- data). Without it, a customer with a 10k average flags any 30k+ txn — which
-- is most of them. The floor effectively says "anomalous AND material."
-- Adjust based on the bank's actual transaction size distribution.
-- ===================================================================
amount_anomaly AS (
    SELECT
        customer_id,
        txn_day AS alert_date,
        txn_id,
        amount,
        rolling_30d_avg
    FROM (
        SELECT
            customer_id,
            txn_id,
            txn_date,
            txn_day,
            amount,
            AVG(amount) OVER (
                PARTITION BY customer_id
                ORDER BY txn_date
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS rolling_30d_avg
        FROM txn_enriched
    ) rolling
    WHERE rolling_30d_avg IS NOT NULL
      AND rolling_30d_avg > 0
      AND amount > 3 * rolling_30d_avg
      AND amount > 1_000_000
)

-- ===================================================================
-- UNION ALL — one row per alert, with JSONB details
-- ===================================================================
SELECT
    customer_id,
    'HIGH_FREQUENCY'   AS alert_type,
    alert_date,
    jsonb_build_object(
        'txns_in_max_window', txns_in_max_window,
        'description', '5+ transactions within a 1-hour window'
    ) AS details_json
FROM high_frequency

UNION ALL

SELECT
    customer_id,
    'MULTI_CITY'        AS alert_type,
    alert_date,
    jsonb_build_object(
        'distinct_cities', distinct_cities,
        'city_list', city_list,
        'description', 'Transactions in 3+ different cities on the same day'
    ) AS details_json
FROM multi_city

UNION ALL

SELECT
    customer_id,
    'AMOUNT_ANOMALY'    AS alert_type,
    alert_date,
    jsonb_build_object(
        'txn_id', txn_id,
        'amount', amount,
        'rolling_30d_avg', ROUND(rolling_30d_avg, 2),
        'multiple_of_avg', ROUND(amount / rolling_30d_avg, 2),
        'description', 'Single transaction exceeds 3x rolling 30-transaction average (excludes current row)'
    ) AS details_json
FROM amount_anomaly;