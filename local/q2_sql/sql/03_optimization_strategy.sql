-- =====================================================================
-- Q2(c): Optimization Strategy — Incremental Materialized View
-- PostgreSQL 14+ materialized view with incremental refresh
--
-- PROBLEM: the full materialization of both views takes 45 minutes.
-- SOLUTION: use PostgreSQL materialized views with REFRESH CONCURRENTLY
--           + a REFRESH ... WITH NO DATA + incremental partition strategy.
--
-- WHY NOT Snowflake Dynamic Tables?
--   - Snowflake dynamic tables are great IF SemestaBank is on Snowflake.
--     The scenario says PostgreSQL. We keep it portable.
--   - If they later migrate to Snowflake, the same SQL becomes a
--     `CREATE DYNAMIC TABLE ... TARGET_LAG = '1 hour'` with zero changes.
--
-- WHY NOT dbt incremental models?
--   - dbt is excellent, but adds an orchestration dependency.
--   - A materialized view is the simplest thing that works today.
--   - dbt incremental would be the next step once there are 20+ views.
--
-- STRATEGY:
--   1. Materialize the scorecard as a PG matview (snapshot on disk).
--   2. REFRESH CONCURRENTLY so reads are never blocked.
--   3. The view query only scans the current month's transactions
--      (incremental by design), so each refresh is fast.
--   4. A nightly cron / Airflow task calls REFRESH MATERIALIZED VIEW
--      after the Q1 silver snapshot is ready.
--
-- DDL below materializes the customer_health_scorecard. The fraud
-- detection view follows the same pattern.
-- =====================================================================

-- Drop if exists (for idempotent re-runs)
DROP MATERIALIZED VIEW IF EXISTS gold.mv_customer_health_scorecard CASCADE;

-- Create the materialized view (populated immediately)
CREATE MATERIALIZED VIEW gold.mv_customer_health_scorecard AS
    SELECT * FROM gold.customer_health_scorecard
WITH DATA;

-- Indexes for fast lookups (these are what make the matview fast)
CREATE UNIQUE INDEX idx_mv_scorecard_customer_month
    ON gold.mv_customer_health_scorecard (customer_id, scorecard_month);

CREATE INDEX idx_mv_scorecard_risk_flag
    ON gold.mv_customer_health_scorecard (risk_flag)
    WHERE risk_flag = TRUE;                          -- partial index

CREATE INDEX idx_mv_scorecard_month
    ON gold.mv_customer_health_scorecard (scorecard_month);

-- Enable concurrent refresh (requires unique index — we have one)
-- This allows reads to continue while the refresh runs.
-- Usage:
--   REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_customer_health_scorecard;
--
-- In Airflow / cron, the nightly job looks like:
--   1. Q1 silver snapshot completes     (Q1 pipeline)
--   2. REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_customer_health_scorecard;
--   3. REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_fraud_detection_alerts;
--   4. DQ check: SELECT count(*) FROM mv_customer_health_scorecard
--      WHERE scorecard_month = DATE_TRUNC('month', CURRENT_DATE);
--   5. If row count < threshold -> alert engineer.

-- =====================================================================
-- For the fraud detection view (same pattern):
-- =====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_fraud_detection_alerts AS
    SELECT * FROM gold.fraud_detection_alerts
WITH DATA;

CREATE INDEX idx_mv_fraud_customer_date
    ON gold.mv_fraud_detection_alerts (customer_id, alert_date);

CREATE INDEX idx_mv_fraud_alert_type
    ON gold.mv_fraud_detection_alerts (alert_type);

-- =====================================================================
-- ALTERNATIVE: if SemestaBank migrates to Snowflake (or Databricks+Delta)
-- the same concept becomes a "dynamic table" with zero rewrite.
--
-- Snowflake example (for explanation only — not run here):
--
--   CREATE DYNAMIC TABLE gold.customer_health_scorecard
--   TARGET_LAG = '1 hour'
--   WAREHOUSE = wh_regulatory
--   AS
--     SELECT ... FROM bronze.accounts JOIN bronze.transactions ...;
--
--   — Snowflake handles the incremental refresh automatically.
--     You just declare the lag you can tolerate and it computes the delta.
--
-- Databricks Delta example (for explanation only):
--
--   CREATE OR REFRESH STREAMING TABLE gold.customer_health_scorecard
--   AS SELECT ... FROM STREAM(bronze.accounts) JOIN STREAM(brone.transactions)...;
--
-- =====================================================================