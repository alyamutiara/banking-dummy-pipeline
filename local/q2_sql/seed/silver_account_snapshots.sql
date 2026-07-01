-- =====================================================================
-- silver.account_snapshots — runnable local stand-in for Q1's output
--
-- In the real system this table is produced by Q1
-- (Q1_Pipeline_Optimization/spark/jobs/account_snapshot.py) as a clean,
-- deduplicated daily snapshot in MinIO/silver, partitioned by snapshot_date.
-- The PostgreSQL reference view 01_customer_health_scorecard.sql needs that
-- prior-month history to compute a REAL month-over-month balance change.
--
-- This seed reconstructs each account's previous-month-END balance from
-- GENUINE transaction history — never a fabricated factor — so the
-- scorecard's MoM numbers vary per customer and the "balance declined
-- > 30% MoM" risk branch can actually fire:
--
--     prior_month_end_balance
--         = current_balance
--           − (net signed effect of THIS month's completed transactions)
--
-- where credits/transfers-in increase balance and debits/transfers-out/
-- payments/fees decrease it. Roll today's balance back over this month's
-- real activity and you land on the genuine month-end balance.
--
-- Run AFTER Q1's seed/init.sql has loaded the bronze schema. Idempotent.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.account_snapshots;

CREATE TABLE silver.account_snapshots (
    snapshot_date  DATE           NOT NULL,
    account_id     VARCHAR(20)    NOT NULL,
    customer_id    VARCHAR(20)    NOT NULL,
    balance        DECIMAL(15,2)  NOT NULL
);

-- One month-end row per account for the PREVIOUS month, dated to the last
-- day of that month (the reference view picks the latest snapshot_date in
-- the prior month, so a single month-end row is sufficient and exact).
INSERT INTO silver.account_snapshots (snapshot_date, account_id, customer_id, balance)
WITH this_month_net AS (
    -- Signed net of this month's completed transactions, per account.
    SELECT
        t.account_id,
        SUM(
            CASE
                WHEN t.txn_type IN ('CREDIT', 'TRANSFER_IN')                      THEN  t.amount
                WHEN t.txn_type IN ('DEBIT', 'TRANSFER_OUT', 'PAYMENT', 'FEE')    THEN -t.amount
                ELSE 0
            END
        ) AS net_change
    FROM bronze.transactions t
    WHERE t.status = 'COMPLETED'
      AND t.txn_date >= DATE_TRUNC('month', CURRENT_DATE)
      AND t.txn_date <  DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
    GROUP BY t.account_id
)
SELECT
    (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day')::date  AS snapshot_date,
    a.account_id,
    a.customer_id,
    -- Roll the current balance back over this month's real activity.
    a.balance - COALESCE(tm.net_change, 0)                        AS balance
FROM bronze.accounts a
LEFT JOIN this_month_net tm ON tm.account_id = a.account_id
WHERE a.status IN ('ACTIVE', 'DORMANT');

CREATE INDEX idx_silver_snap_date ON silver.account_snapshots (snapshot_date);
CREATE INDEX idx_silver_snap_acct ON silver.account_snapshots (account_id);

-- Sanity check (printed to the psql client running this seed)
DO $$
DECLARE
    n_rows  bigint;
    n_accts bigint;
BEGIN
    SELECT COUNT(*), COUNT(DISTINCT account_id) INTO n_rows, n_accts
    FROM silver.account_snapshots;
    RAISE NOTICE 'silver.account_snapshots seeded: % rows across % accounts', n_rows, n_accts;
END $$;
