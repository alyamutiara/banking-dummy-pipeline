# 01 · Scenario & Dataset

## The company — SemestaBank

SemestaBank is an Indonesian **digital-first bank** — 4.2M customers, no physical branches,
everything through a mobile app and web. Products: savings, personal loans, credit cards,
QRIS payments, and investments (mutual funds + government bonds).

Data infrastructure named in the brief:


| Source              | Role                       | Scale                  |
| ------------------- | -------------------------- | ---------------------- |
| Oracle core banking | Accounts + transactions    | 2M+ transactions/day   |
| Kafka (mobile app)  | Clickstream events         | 50K events/sec at peak |
| Databricks          | Credit-scoring ML pipeline | —                      |
| Zendesk             | Support tickets            | 15K tickets/month      |
| Braze + Google Ads  | Marketing/campaigns        | —                      |


**The regulatory driver:** OJK (Indonesia's financial regulator) has ordered SemestaBank to
demonstrate **data lineage and auditability for all customer-facing metrics within 6 months**.
A cross-functional team (data engineers, BI analysts, data governance analysts) shares one
platform. This single requirement shapes all three answers — it's why the pipeline keeps
history (time travel), why the SQL reads from real snapshots, and why the architecture treats
lineage as a first-class citizen.

The three questions are deliberately connected:

```
Q1  fix/rewrite the nightly account-snapshot ELT  ─┐
                                                   ├─▶ Q1's silver snapshots feed Q2's MoM balance
Q2  build regulatory SQL views on top of it       ─┤
                                                   └─▶ Q3 scales Q1+Q2 onto a governed platform
Q3  design the end-to-end platform
```

## The dataset

The sample dataset (`semestabank_dataset/` at the repo root) covers **Aug 2025 – Feb 2026**:


| File                       | Rows          | Notes                                                                             |
| -------------------------- | ------------- | --------------------------------------------------------------------------------- |
| `customers.csv`            | 5,000         | KYC status, risk_score, segment; NIK/phone are PII (must be masked)               |
| `accounts.csv`             | 7,954         | SAVINGS / LOAN / CREDIT_CARD / INVESTMENT; balance, credit_limit                  |
| `transactions.csv`         | **1,991,349** | the big one; DEBIT/CREDIT/TRANSFER/PAYMENT/FEE; COMPLETED/PENDING/FAILED/REVERSED |
| `credit_scores.csv`        | 7,452         | credit_score 300–850, probability_of_default, model_version                       |
| `app_events.csv`           | 500,000       | clickstream sample (device, screen, session)                                      |
| `support_tickets.csv`      | 16,953        | Zendesk; category, priority, CSAT                                                 |
| `acquisition_channels.csv` | 5,000         | attribution (ORGANIC / REFERRAL / PAID_*)                                         |


### Shared bronze schema (key columns used by the answers)

- `**bronze.accounts`** — `account_id`, `customer_id`, `account_type`, `balance`,
`credit_limit`, `interest_rate`, `status`
- `**bronze.transactions`** — `txn_id`, `account_id`, `txn_date`, `txn_type`, `amount`,
`channel`, `merchant_category`, `reference_id`, `status`  *(txn_id is intentionally
non-unique in the seed so the pipeline must deduplicate)*
- `**bronze.customers`** — `customer_id`, `full_name`, `nik`, `phone`, `kyc_status`,
`risk_score`, `segment`
- `**bronze.credit_scores*`* — `customer_id`, `score_date`, `credit_score`,
`probability_of_default`, `model_version`

### OJK business rules baked into the answers

These rules (from the dataset brief) appear as **data-quality gates** in Q1 and as **risk
logic** in Q2:

- All `ACTIVE` accounts must have `kyc_status = 'VERIFIED'`.
- KYC `PENDING` > 30 days requires escalation.
- No transactions allowed for `kyc_status = 'REJECTED'`.
- Credit scores must not jump > 100 points between assessments.
- `risk_score` and `probability_of_default` should be correlated.
- Balance reconciliation: `CREDIT − DEBIT = account balance change`.
- NIK (16-digit national ID) and phone must be **masked** (last 4 digits only).
- Credit utilization = `balance / credit_limit` (credit cards).

> Where these show up: Q1's `account_snapshot.py` enforces KYC compliance, credit-score
> stability, risk/PD correlation, and balance reconciliation as DQ assertions; `bronze_ingest.py`
> masks NIK/phone. Q2's scorecard turns utilization + PD + MoM-decline into a `risk_flag`.

