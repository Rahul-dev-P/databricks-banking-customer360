-- =============================================================================
-- 1. Date Dimension
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_date
COMMENT 'Standard banking calendar dimension with fiscal and weekend flags'
AS 
WITH date_range AS (
  SELECT EXPLODE(SEQUENCE(DATE'2020-01-01', DATE_ADD(CURRENT_DATE(), 1825), INTERVAL 1 DAY)) AS calendar_date
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

-- =============================================================================
-- 2. FULL SCD2 DIMENSION: Customer History with Deterministic Surrogate Keys
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_customer_history
COMMENT 'Immutable SCD Type 2 dimension preserving full profile mutation history'
AS 
SELECT 
  -- Deterministic Surrogate Key: Hashing natural key + version start timestamp
  SHA2(CONCAT(CAST(customer_id AS STRING), '|', CAST(__START_AT AS STRING)), 256) AS customer_sk,
  customer_id,
  first_name,
  last_name,
  CONCAT(first_name, ' ', last_name) AS full_name,
  email,
  phone,
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
  __START_AT AS valid_from,
  COALESCE(__END_AT, TIMESTAMP'9999-12-31 23:59:59') AS valid_to,
  CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM silver.customers_history;

-- =============================================================================
-- 3. ACTIVE DIMENSION SLICE: Current Active Customers (Type 1 View of SCD2)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_customer
COMMENT 'Active customer dimensional slice representing current profile state'
AS 
SELECT 
  customer_sk,
  customer_id,
  first_name,
  last_name,
  full_name,
  email,
  phone,
  address_street,
  city,
  state,
  postal_code,
  country,
  customer_segment,
  risk_score,
  credit_tier,
  kyc_status,
  customer_since,
  valid_from AS current_version_effective_date
FROM gold.dim_customer_history
WHERE is_current = TRUE;

-- =============================================================================
-- 4. OPERATIONAL PRODUCT DIMENSION: Current Accounts
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_account
COMMENT 'Current operational view of all bank accounts and balances'
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

-- =============================================================================
-- 5. FACT TABLE: Point-in-Time Core Transactions (Liquid Clustered)
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.fact_transactions
COMMENT 'Core monetary transaction fact table with Point-in-Time SCD2 Surrogate Keys'
AS 
SELECT 
  t.transaction_id,
  CAST(DATE_FORMAT(t.transaction_timestamp, 'yyyyMMdd') AS INT) AS date_key,
  t.account_id,
  
  -- Audit / As-Was Join: Exact customer version active at transaction timestamp
  COALESCE(c.customer_sk, 'UNKNOWN_CUSTOMER_SK') AS customer_sk,
  
  -- As-Is Join Key: Enables current cohort analysis without multi-hop traversal
  COALESCE(a.customer_id, -1) AS customer_id,
  
  t.amount,
  t.signed_amount,
  t.transaction_type,
  t.channel,
  t.merchant_category,
  t.transaction_timestamp,
  t._source_file,
  
  -- Lineage & Orphaned Flags
  CASE WHEN a.account_id IS NULL THEN TRUE ELSE FALSE END AS is_orphaned_account,
  CASE WHEN c.customer_sk IS NULL THEN TRUE ELSE FALSE END AS is_unmatched_customer_history
FROM silver.transactions_clean t
LEFT JOIN silver.accounts_current a 
  ON t.account_id = a.account_id
-- Range join matches transaction time to the exact SCD2 validity interval
LEFT JOIN gold.dim_customer_history c 
  ON a.customer_id = c.customer_id
 AND t.transaction_timestamp >= c.valid_from 
 AND t.transaction_timestamp < c.valid_to;

 -- =============================================================================
-- 6. PERIODIC SNAPSHOT FACT: Monthly Customer Summary
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.monthly_customer_summary
COMMENT 'Monthly longitudinal financial rollup per customer for trend and balance analytics'
AS 
SELECT 
  customer_id,
  DATE_FORMAT(transaction_timestamp, 'yyyy-MM') AS activity_month,
  COUNT(transaction_id) AS monthly_tx_count,
  ROUND(SUM(amount), 2) AS monthly_total_volume,
  ROUND(AVG(amount), 2) AS monthly_avg_ticket_size,
  ROUND(SUM(CASE WHEN signed_amount > 0 THEN signed_amount ELSE 0.0 END), 2) AS monthly_inflow_volume,
  ROUND(SUM(CASE WHEN signed_amount < 0 THEN ABS(signed_amount) ELSE 0.0 END), 2) AS monthly_outflow_volume,
  MAX(transaction_timestamp) AS month_last_transaction_timestamp,
  MAX_BY(channel, transaction_timestamp) AS month_preferred_channel
FROM gold.fact_transactions
GROUP BY customer_id, DATE_FORMAT(transaction_timestamp, 'yyyy-MM');

-- =============================================================================
-- 7. CUMULATIVE ROLLUP: Customer Lifetime Spend & Velocity State
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.customer_spend_summary
COMMENT 'Pre-aggregated lifetime financial metrics derived from monthly summaries'
AS 
SELECT 
  customer_id,
  SUM(monthly_tx_count) AS lifetime_transactions,
  ROUND(SUM(monthly_total_volume), 2) AS lifetime_spend_volume,
  ROUND(SUM(monthly_total_volume) / NULLIF(SUM(monthly_tx_count), 0), 2) AS avg_transaction_value,
  ROUND(SUM(monthly_inflow_volume), 2) AS lifetime_inflow_volume,
  ROUND(SUM(monthly_outflow_volume), 2) AS lifetime_outflow_volume,
  MAX(month_last_transaction_timestamp) AS last_transaction_timestamp,
  MAX_BY(month_preferred_channel, month_last_transaction_timestamp) AS preferred_channel
FROM gold.monthly_customer_summary
GROUP BY customer_id;

-- =============================================================================
-- 8. BEHAVIORAL SEGMENTATION: Decoupled RFM Model (Event-Time Anchored)
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.rfm_segments
COMMENT 'Customer behavioral segmentation (Recency, Frequency, Monetary quintiles)'
AS 
WITH anchor AS (
  -- Anchors recency calculation to the newest event in the dataset rather than CURRENT_DATE()
  -- This prevents recompute cascades when no new data arrives.
  SELECT COALESCE(MAX(transaction_timestamp), CURRENT_TIMESTAMP()) AS max_lakehouse_timestamp
  FROM gold.fact_transactions
),
metrics AS (
  SELECT 
    c.customer_id,
    -- Recency measured from the latest transaction, falling back to profile creation date
    DATEDIFF(
      (SELECT max_lakehouse_timestamp FROM anchor), 
      COALESCE(s.last_transaction_timestamp, c.customer_since)
    ) AS recency_days,
    COALESCE(s.lifetime_transactions, 0) AS frequency_count,
    COALESCE(s.lifetime_spend_volume, 0.0) AS monetary_total
  FROM gold.dim_customer c
  LEFT JOIN gold.customer_spend_summary s ON c.customer_id = s.customer_id
),
scored AS (
  SELECT 
    customer_id,
    recency_days,
    frequency_count,
    monetary_total,
    -- Invert recency rank: Lowest recency days gets highest score (5)
    NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
    NTILE(5) OVER (ORDER BY frequency_count ASC) AS f_score,
    NTILE(5) OVER (ORDER BY monetary_total ASC) AS m_score
  FROM metrics
)
SELECT 
  customer_id,
  recency_days,
  frequency_count,
  monetary_total,
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

-- =============================================================================
-- 9. VIRTUAL SERVING MART: Customer 360 View (Zero-Scan Runtime)
-- =============================================================================
CREATE VIEW gold.customer_360
COMMENT 'Enterprise unified Customer 360 profile. Queries execute in milliseconds with ZERO transaction scans.'
AS 
WITH account_summary AS (
  SELECT 
    customer_id,
    COUNT(account_id) AS total_accounts,
    SUM(CASE WHEN account_type = 'Checking' THEN 1 ELSE 0 END) AS checking_accounts_count,
    SUM(CASE WHEN account_type = 'Savings' THEN 1 ELSE 0 END) AS savings_accounts_count,
    SUM(CASE WHEN account_type = 'Credit Card' THEN 1 ELSE 0 END) AS credit_cards_count,
    SUM(CASE WHEN account_status = 'Active' THEN 1 ELSE 0 END) AS active_accounts_count,
    SUM(CASE WHEN account_status = 'Closed' THEN 1 ELSE 0 END) AS closed_accounts_count
  FROM gold.dim_account
  GROUP BY customer_id
)
SELECT 
  -- Demographics & Master State
  c.customer_id,
  c.customer_sk,
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
  c.current_version_effective_date,
  
  -- Account Portfolio Metrics
  COALESCE(acc.total_accounts, 0) AS total_accounts_held,
  COALESCE(acc.active_accounts_count, 0) AS active_accounts,
  COALESCE(acc.closed_accounts_count, 0) AS closed_accounts,
  COALESCE(acc.checking_accounts_count, 0) AS checking_accounts,
  COALESCE(acc.savings_accounts_count, 0) AS savings_accounts,
  COALESCE(acc.credit_cards_count, 0) AS credit_cards,
  
  -- Pre-aggregated Spend & Behavioral Metrics (Read directly from pre-computed summary)
  COALESCE(sp.lifetime_transactions, 0) AS lifetime_transactions,
  COALESCE(sp.lifetime_spend_volume, 0.0) AS lifetime_spend_volume,
  COALESCE(sp.avg_transaction_value, 0.0) AS avg_transaction_value,
  COALESCE(sp.lifetime_inflow_volume, 0.0) AS lifetime_inflow_volume,
  COALESCE(sp.lifetime_outflow_volume, 0.0) AS lifetime_outflow_volume,
  sp.last_transaction_timestamp,
  sp.preferred_channel,
  
  -- RFM Behavioral Intelligence
  rfm.recency_days,
  rfm.rfm_composite_score,
  rfm.rfm_segment_label,
  
  -- Governance & Compliance Risk Flags
  CASE WHEN c.risk_score < 550 THEN TRUE ELSE FALSE END AS flag_subprime_risk,
  CASE WHEN c.kyc_status != 'Verified' THEN TRUE ELSE FALSE END AS flag_kyc_incomplete,
  CASE WHEN sp.lifetime_transactions > 80 AND c.risk_score < 600 THEN TRUE ELSE FALSE END AS flag_high_velocity_monitoring
FROM gold.dim_customer c
LEFT JOIN account_summary acc ON c.customer_id = acc.customer_id
LEFT JOIN gold.customer_spend_summary sp ON c.customer_id = sp.customer_id
LEFT JOIN gold.rfm_segments rfm ON c.customer_id = rfm.customer_id;
