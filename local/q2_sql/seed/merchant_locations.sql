-- =====================================================================
-- Supplementary table: bronze.merchant_locations
--
-- The semestabank_dataset does NOT include merchant location data. The
-- transactions table has a `reference_id` column (values like REF2693675,
-- REF5365702, etc.) and a `merchant_category` column, but there is no
-- city/geolocation column attached to individual transactions.
--
-- This is a realistic gap: in production, SemestaBank would populate a
-- merchant directory table from the payment network (QRIS merchant registry,
-- Visa acquiring data, Mastercard MATCH, or an MCC-to-city lookup from the
-- card network). The Q2(b) fraud detection view LEFT JOINs to this table
-- to detect the "3+ different cities on the same day" pattern.
--
-- Strategy for the demo:
--   - We hash every distinct reference_id from bronze.transactions to one
--     of 10 Indonesian cities, so the MULTI_CITY fraud pattern has real
--     coverage. A 20-row hand seed matches <0.001% of 942,703 distinct
--     reference_ids, making MULTI_CITY structurally dead. This hash-based
--     assignment gives ~10% per city — enough for 3-city same-day patterns
--     to fire naturally.
--   - In production, this table is the full payment-network merchant registry
--     (QRIS / Visa acquiring / Mastercard MATCH); the hash is a demo stand-in.
-- =====================================================================

CREATE TABLE IF NOT EXISTS bronze.merchant_locations (
    reference_id    VARCHAR(30)   PRIMARY KEY,
    merchant_name   VARCHAR(100),
    merchant_city   VARCHAR(30),
    merchant_lat    DECIMAL(10, 6),
    merchant_lon    DECIMAL(10, 6),
    mcc_category    VARCHAR(30)
);

-- Deterministic hash-based city assignment: every distinct reference_id in
-- bronze.transactions maps to one of 10 Indonesian cities. PostgreSQL's
-- internal hashtext() is stable and repeatable — modulo 10 distributes
-- evenly across the 10-city enum.
INSERT INTO bronze.merchant_locations (
    reference_id, merchant_name, merchant_city,
    merchant_lat, merchant_lon, mcc_category
)
SELECT
    reference_id,
    'Merchant ' || reference_id AS merchant_name,
    CASE MOD(ABS(hashtext(reference_id)), 10)
        WHEN 0 THEN 'Jakarta'
        WHEN 1 THEN 'Surabaya'
        WHEN 2 THEN 'Bandung'
        WHEN 3 THEN 'Medan'
        WHEN 4 THEN 'Makassar'
        WHEN 5 THEN 'Semarang'
        WHEN 6 THEN 'Palembang'
        WHEN 7 THEN 'Denpasar'
        WHEN 8 THEN 'Balikpapan'
        WHEN 9 THEN 'Yogyakarta'
    END AS merchant_city,
    CASE MOD(ABS(hashtext(reference_id)), 10)
        WHEN 0 THEN -6.2088   WHEN 1 THEN -7.2575
        WHEN 2 THEN -6.9175   WHEN 3 THEN  3.5952
        WHEN 4 THEN -5.1477   WHEN 5 THEN -6.9932
        WHEN 6 THEN -2.9911   WHEN 7 THEN -8.6705
        WHEN 8 THEN -1.2379   WHEN 9 THEN -7.7956
    END AS merchant_lat,
    CASE MOD(ABS(hashtext(reference_id)), 10)
        WHEN 0 THEN 106.8456  WHEN 1 THEN 112.7521
        WHEN 2 THEN 107.6191  WHEN 3 THEN  98.6722
        WHEN 4 THEN 119.4327  WHEN 5 THEN 110.4193
        WHEN 6 THEN 104.7570  WHEN 7 THEN 115.2126
        WHEN 8 THEN 116.8612  WHEN 9 THEN 110.3740
    END AS merchant_lon,
    'General' AS mcc_category
FROM (
    SELECT DISTINCT reference_id
    FROM bronze.transactions
    WHERE reference_id IS NOT NULL
) refs
ON CONFLICT (reference_id) DO NOTHING;
