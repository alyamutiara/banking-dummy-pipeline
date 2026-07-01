-- =====================================================================
-- SemestaBank - bronze schema seed (local stand-in for Oracle core banking)
-- Loads the full assessment dataset from ../semestabank_dataset/ via COPY.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS bronze;

-- Airflow needs its own DB for metadata
CREATE DATABASE airflow;

-- ----------------------------------------------------------------- customers
CREATE TABLE bronze.customers (
    customer_id        VARCHAR(20) PRIMARY KEY,
    full_name          VARCHAR(100),
    nik                VARCHAR(16),
    phone              VARCHAR(15),
    email              VARCHAR(80),
    city               VARCHAR(30),
    province           VARCHAR(30),
    registration_date  DATE,
    kyc_status         VARCHAR(15),
    risk_score         DECIMAL(5,2),
    segment            VARCHAR(20)
);

-- ----------------------------------------------------------------- accounts
CREATE TABLE bronze.accounts (
    account_id    VARCHAR(20) PRIMARY KEY,
    customer_id   VARCHAR(20) REFERENCES bronze.customers(customer_id),
    account_type  VARCHAR(20) NOT NULL,
    product_name  VARCHAR(50),
    opened_date   DATE,
    status        VARCHAR(15) NOT NULL,
    balance       DECIMAL(15,2),
    credit_limit  DECIMAL(15,2),
    interest_rate DECIMAL(5,4)
);

-- ----------------------------------------------------------------- transactions
CREATE TABLE bronze.transactions (
    txn_id             VARCHAR(25),
    account_id         VARCHAR(20) REFERENCES bronze.accounts(account_id),
    txn_date           TIMESTAMP NOT NULL,
    txn_type           VARCHAR(20) NOT NULL,
    amount             DECIMAL(15,2) NOT NULL,
    merchant_category  VARCHAR(30),
    channel            VARCHAR(20),
    reference_id       VARCHAR(30),
    status             VARCHAR(15) NOT NULL
);
CREATE INDEX idx_txn_id ON bronze.transactions (txn_id);
CREATE INDEX idx_txn_date ON bronze.transactions (txn_date);
CREATE INDEX idx_txn_acct ON bronze.transactions (account_id);

-- ----------------------------------------------------------------- credit_scores
CREATE TABLE bronze.credit_scores (
    score_id              VARCHAR(20) PRIMARY KEY,
    customer_id           VARCHAR(20) REFERENCES bronze.customers(customer_id),
    score_date            DATE NOT NULL,
    model_version         VARCHAR(10),
    credit_score          INTEGER,
    probability_of_default DECIMAL(5,4),
    features_used         TEXT
);

-- ----------------------------------------------------------------- app_events
CREATE TABLE bronze.app_events (
    event_id         VARCHAR(25) PRIMARY KEY,
    customer_id      VARCHAR(20),
    event_timestamp  TIMESTAMP NOT NULL,
    event_type       VARCHAR(30),
    screen_name      VARCHAR(30),
    session_id       VARCHAR(30),
    device_type      VARCHAR(10),
    app_version      VARCHAR(10)
);

-- ----------------------------------------------------------------- support_tickets
CREATE TABLE bronze.support_tickets (
    ticket_id          VARCHAR(15) PRIMARY KEY,
    customer_id        VARCHAR(20),
    created_at         TIMESTAMP NOT NULL,
    resolved_at        TIMESTAMP,
    category           VARCHAR(20),
    priority           VARCHAR(10),
    satisfaction_score DECIMAL(3,1),
    channel            VARCHAR(20)
);

-- ----------------------------------------------------------------- acquisition_channels
CREATE TABLE bronze.acquisition_channels (
    customer_id      VARCHAR(20) PRIMARY KEY,
    channel          VARCHAR(20),
    acquisition_date DATE,
    campaign_id      VARCHAR(30)
);

-- =====================================================================
-- Load data from CSV files (order matters for FK constraints)
-- =====================================================================

COPY bronze.customers       FROM '/dataset/customers.csv'           WITH (FORMAT CSV, HEADER true);
COPY bronze.accounts        FROM '/dataset/accounts.csv'            WITH (FORMAT CSV, HEADER true);
COPY bronze.transactions    FROM '/dataset/transactions.csv'        WITH (FORMAT CSV, HEADER true);
COPY bronze.credit_scores   FROM '/dataset/credit_scores.csv'       WITH (FORMAT CSV, HEADER true);
COPY bronze.app_events      FROM '/dataset/app_events.csv'          WITH (FORMAT CSV, HEADER true);
COPY bronze.support_tickets FROM '/dataset/support_tickets.csv'     WITH (FORMAT CSV, HEADER true);
COPY bronze.acquisition_channels FROM '/dataset/acquisition_channels.csv' WITH (FORMAT CSV, HEADER true);
