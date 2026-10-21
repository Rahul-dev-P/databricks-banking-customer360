-- 1. Date Dimension
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_date
COMMENT 'Standard banking calendar dimension with fiscal and weekend flags'
AS 
WITH date_range AS (
  SELECT EXPLODE(SEQUENCE(DATE'2024-01-01', DATE'2026-12-31', INTERVAL 1 DAY)) AS calendar_date
)
SELECT 
  CAST(DATE_FORMAT(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
  calendar_date,
  YEAR(calendar_date) AS year,
  QUARTER(calendar_date) AS quarter,
  MONTH(calendar_date) AS month,
  DATE_FORMAT(calendar_date, 'MMMM') AS month_name,
  WEEKOFYEAR(calendar_date) AS week_of_year,
  DAY(calendar_date) AS day_of_month,
  DAYOFWEEK(calendar_date) AS day_of_week,
  DATE_FORMAT(calendar_date, 'EEEE') AS day_name,
  CASE WHEN DAYOFWEEK(calendar_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
  CASE WHEN MONTH(calendar_date) IN (1, 2, 3) THEN 'Q1'
       WHEN MONTH(calendar_date) IN (4, 5, 6) THEN 'Q2'
       WHEN MONTH(calendar_date) IN (7, 8, 9) THEN 'Q3'
       ELSE 'Q4' END AS fiscal_quarter
FROM date_range;

-- 2. Customer Dimension
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_customer
COMMENT 'Active customer dimension slice reflecting current profile state'
AS 
SELECT 
  customer_id,
  first_name,
  last_name,
  CONCAT(first_name, ' ', last_name) AS full_name,
  email,
  phone_digits AS phone,
  address_street,
  city,
  state,
  postal_code,
  country,
  customer_segment,
  risk_score,
  CASE 
    WHEN risk_score >= 750 THEN 'Excellent'
    WHEN risk_score >= 670 THEN 'Good'
    WHEN risk_score >= 580 THEN 'Fair'
    ELSE 'Poor'
  END AS credit_tier,
  kyc_status,
  created_at AS customer_since,
  __START_AT AS current_version_effective_date
FROM silver.customers_history
WHERE __END_AT IS NULL;

-- 3. Account Dimension
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_account
COMMENT 'Current operational snapshot of banking products and accounts'
AS 
SELECT 
  account_id,
  customer_id,
  account_type,
  currency,
  status AS account_status,
  opened_at,
  last_updated AS account_last_modified
FROM silver.accounts_current;

-- 4. Fact Transactions (Direct 1-Hop Star Schema Fact)
CREATE OR REFRESH MATERIALIZED VIEW gold.fact_transactions
COMMENT 'Core fact table linking transactions to customer, account, and date keys'
AS 
SELECT 
  t.transaction_id,
  CAST(DATE_FORMAT(t.transaction_timestamp, 'yyyyMMdd') AS INT) AS date_key,
  t.account_id,
  a.customer_id,
  t.amount,
  t.transaction_type,
  t.channel,
  t.merchant_category,
  t.transaction_timestamp,
  t._source_file
FROM silver.transactions_clean t
INNER JOIN silver.accounts_current a ON t.account_id = a.account_id;

-- =============================================================================
-- 5. RFM & Behavioral Segments (Materialized View - Fast Full Recompute ~2s)
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.rfm_segments
COMMENT 'Customer behavioral metrics and RFM quintiles pre-aggregated at write time'
AS 
WITH customer_tx_summary AS (
  SELECT 
    a.customer_id,
    COUNT(t.transaction_id) AS lifetime_transactions,
    COALESCE(SUM(t.amount), 0.0) AS lifetime_spend_volume,
    ROUND(COALESCE(AVG(t.amount), 0.0), 2) AS avg_transaction_value,
    MAX(t.transaction_timestamp) AS last_transaction_timestamp,
    MAX_BY(t.channel, t.transaction_timestamp) AS preferred_channel,
    MAX_BY(t.merchant_category, t.transaction_timestamp) AS preferred_category,
    DATEDIFF(CURRENT_DATE(), MAX(CAST(t.transaction_timestamp AS DATE))) AS recency_days
  FROM silver.accounts_current a
  JOIN silver.transactions_clean t ON a.account_id = t.account_id
  GROUP BY a.customer_id
),
scored AS (
  SELECT 
    c.customer_id,
    COALESCE(tx.recency_days, 999) AS recency_days,
    COALESCE(tx.lifetime_transactions, 0) AS lifetime_transactions,
    COALESCE(tx.lifetime_spend_volume, 0.0) AS lifetime_spend_volume,
    COALESCE(tx.avg_transaction_value, 0.0) AS avg_transaction_value,
    tx.last_transaction_timestamp,
    tx.preferred_channel,
    tx.preferred_category,
    NTILE(5) OVER (ORDER BY COALESCE(tx.recency_days, 999) DESC) AS r_score,
    NTILE(5) OVER (ORDER BY COALESCE(tx.lifetime_transactions, 0) ASC) AS f_score,
    NTILE(5) OVER (ORDER BY COALESCE(tx.lifetime_spend_volume, 0.0) ASC) AS m_score
  FROM silver.customers_history c
  LEFT JOIN customer_tx_summary tx ON c.customer_id = tx.customer_id
  WHERE c.__END_AT IS NULL
)
SELECT 
  customer_id,
  recency_days,
  lifetime_transactions,
  lifetime_spend_volume,
  avg_transaction_value,
  last_transaction_timestamp,
  preferred_channel,
  preferred_category,
  r_score,
  f_score,
  m_score,
  (r_score + f_score + m_score) AS rfm_composite_score,
  CASE 
    WHEN (r_score >= 4 AND f_score >= 4 AND m_score >= 4) THEN 'Champions'
    WHEN (r_score >= 3 AND f_score >= 3) THEN 'Loyal Customers'
    WHEN (r_score >= 4 AND f_score <= 2) THEN 'Recent High Potentials'
    WHEN (r_score <= 2 AND f_score >= 3) THEN 'At Risk / Churn Alert'
    WHEN (r_score <= 2 AND f_score <= 2) THEN 'Hibernating / Inactive'
    ELSE 'Steady Retail'
  END AS rfm_segment_label
FROM scored;

-- 6. Monthly Customer Summary
CREATE OR REFRESH MATERIALIZED VIEW gold.monthly_customer_summary
COMMENT 'Monthly aggregate financial activity per customer for longitudinal analytics'
AS 
SELECT 
  a.customer_id,
  DATE_FORMAT(t.transaction_timestamp, 'yyyy-MM') AS activity_month,
  COUNT(t.transaction_id) AS monthly_tx_count,
  SUM(t.amount) AS monthly_total_volume,
  ROUND(AVG(t.amount), 2) AS monthly_avg_ticket_size,
  SUM(CASE WHEN t.transaction_type IN ('Deposit', 'Transfer') THEN t.amount ELSE 0.0 END) AS monthly_inflow_volume,
  SUM(CASE WHEN t.transaction_type IN ('Withdrawal', 'POS Purchase', 'Online Payment', 'Fee') THEN t.amount ELSE 0.0 END) AS monthly_outflow_volume
FROM silver.accounts_current a
JOIN silver.transactions_clean t ON a.account_id = t.account_id
GROUP BY a.customer_id, DATE_FORMAT(t.transaction_timestamp, 'yyyy-MM');

-- =============================================================================
-- 7. Complete Customer 360 View (Standard View - Instant Sub-Second Lookup)
-- =============================================================================
CREATE VIEW gold.customer_360
COMMENT 'Unified Customer 360 profile uniting demographics, accounts, spending, RFM, and risk'
AS 
WITH account_summary AS (
  SELECT 
    customer_id,
    COUNT(account_id) AS total_accounts,
    SUM(CASE WHEN account_type = 'Checking' THEN 1 ELSE 0 END) AS checking_accounts_count,
    SUM(CASE WHEN account_type = 'Savings' THEN 1 ELSE 0 END) AS savings_accounts_count,
    SUM(CASE WHEN account_type = 'Credit Card' THEN 1 ELSE 0 END) AS credit_cards_count,
    SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_accounts_count,
    SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) AS closed_accounts_count
  FROM silver.accounts_current
  GROUP BY customer_id
)
SELECT 
  -- Demographics & Master State
  c.customer_id,
  c.full_name,
  c.email,
  c.phone,
  c.city,
  c.state,
  c.country,
  c.customer_segment,
  c.risk_score,
  c.credit_tier,
  c.kyc_status,
  c.customer_since,
  
  -- Account Portfolio Metrics
  COALESCE(acc.total_accounts, 0) AS total_accounts_held,
  COALESCE(acc.active_accounts_count, 0) AS active_accounts,
  COALESCE(acc.closed_accounts_count, 0) AS closed_accounts,
  COALESCE(acc.checking_accounts_count, 0) AS checking_accounts,
  COALESCE(acc.savings_accounts_count, 0) AS savings_accounts,
  COALESCE(acc.credit_cards_count, 0) AS credit_cards,
  
  -- Behavioral & Spend Profile (Read directly from pre-computed RFM mart)
  COALESCE(rfm.lifetime_transactions, 0) AS lifetime_transactions,
  COALESCE(rfm.lifetime_spend_volume, 0.0) AS lifetime_spend_volume,
  COALESCE(rfm.avg_transaction_value, 0.0) AS avg_transaction_value,
  rfm.last_transaction_timestamp,
  rfm.preferred_channel,
  rfm.preferred_category,
  
  -- RFM Segment Intelligence
  rfm.recency_days,
  rfm.rfm_composite_score,
  rfm.rfm_segment_label,
  
  -- Risk & Compliance Flags
  CASE WHEN c.risk_score < 550 THEN TRUE ELSE FALSE END AS flag_subprime_risk,
  CASE WHEN c.kyc_status != 'Verified' THEN TRUE ELSE FALSE END AS flag_kyc_incomplete,
  CASE WHEN rfm.lifetime_transactions > 80 AND c.risk_score < 600 THEN TRUE ELSE FALSE END AS flag_high_velocity_monitoring
FROM gold.dim_customer c
LEFT JOIN account_summary acc ON c.customer_id = acc.customer_id
LEFT JOIN gold.rfm_segments rfm ON c.customer_id = rfm.customer_id;