-- =====================================================================
-- Gold schema setup
--
-- Q1's seed/init.sql already creates the full bronze schema and loads all
-- 7 datasets from semestabank_dataset/CSVs via COPY:
--   bronze.customers            (5,000 rows)
--   bronze.accounts             (7,954 rows)
--   bronze.transactions         (1,991,349 rows)
--   bronze.credit_scores        (7,452 rows)
--   bronze.app_events           (500,000 rows)
--   bronze.support_tickets      (16,953 rows)
--   bronze.acquisition_channels (5,000 rows)
--
-- This file ONLY creates the gold schema. The bronze data is already in
-- PostgreSQL — no custom inserts needed.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;
