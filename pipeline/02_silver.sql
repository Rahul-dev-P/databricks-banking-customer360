-- -----------------------------------------------------------------------------
-- 1. Silver Transactions (Expectations & Cleaning)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver.transactions_clean (
  CONSTRAINT valid_txn_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_account_id EXPECT (account_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount EXPECT (amount > 0.0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp EXPECT (transaction_timestamp <= CURRENT_TIMESTAMP()) ON VIOLATION DROP ROW
)
COMMENT 'Sanitized and validated financial transaction event stream'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  CAST(transaction_id AS STRING) AS transaction_id,
  CAST(account_id AS STRING) AS account_id,
  CAST(amount AS DECIMAL(18, 4)) AS amount,
  UPPER(TRIM(transaction_type)) AS transaction_type,
  TRIM(channel) AS channel,
  TRIM(merchant_category) AS merchant_category,
  CAST(transaction_timestamp AS TIMESTAMP) AS transaction_timestamp,
  _source_file,
  CURRENT_TIMESTAMP() AS _cleaned_at
FROM STREAM(bronze.transactions_raw);

-- -----------------------------------------------------------------------------
-- 2. Silver Customers History (SCD Type 2 via AUTO CDC)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver.customers_history (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_email_format EXPECT (
    email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
  ) ON VIOLATION DROP ROW,
  CONSTRAINT valid_phone_format EXPECT (
    LENGTH(phone_digits) BETWEEN 10 AND 15
  ) ON VIOLATION DROP ROW,
  CONSTRAINT valid_risk_score EXPECT (risk_score BETWEEN 300 AND 850) ON VIOLATION DROP ROW,
  CONSTRAINT valid_segment EXPECT (customer_segment IN ('Retail', 'Affluent', 'SME', 'Corporate')) ON VIOLATION DROP ROW
)
COMMENT 'Customer master SCD Type 2 dimension preserving full profile mutation history'
TBLPROPERTIES ('quality' = 'silver');

CREATE TEMPORARY STREAMING TABLE customers_cleaned_stream
AS SELECT
  customer_id,
  TRIM(first_name) AS first_name,
  TRIM(last_name) AS last_name,
  LOWER(TRIM(email)) AS email,
  TRIM(phone) AS phone_raw,
  REGEXP_REPLACE(phone, '[^0-9]', '') AS phone_digits,
  TRIM(address_street) AS address_street,
  TRIM(city) AS city,
  UPPER(TRIM(state)) AS state,
  TRIM(postal_code) AS postal_code,
  UPPER(TRIM(country)) AS country,
  customer_segment,
  risk_score,
  kyc_status,
  created_at,
  last_updated,
  is_deleted
FROM STREAM(bronze.customers_raw);

CREATE FLOW silver_customers_scd2_flow AS
AUTO CDC INTO silver.customers_history
FROM STREAM(customers_cleaned_stream)
KEYS (customer_id)
APPLY AS DELETE WHEN is_deleted = 1
SEQUENCE BY last_updated
COLUMNS * EXCEPT (is_deleted, phone_raw)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (last_updated);

-- -----------------------------------------------------------------------------
-- 3. Silver Accounts Current (SCD Type 1 via AUTO CDC)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver.accounts_current (
  CONSTRAINT valid_acc_id EXPECT (account_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_cust_ref EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (status IN ('Active', 'Dormant', 'Frozen', 'Closed')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_type EXPECT (account_type IN ('Checking', 'Savings', 'Money Market', 'Credit Card')) ON VIOLATION DROP ROW
)
COMMENT 'Current operational snapshot of accounts (SCD Type 1 in-place update)'
TBLPROPERTIES ('quality' = 'silver');

CREATE TEMPORARY STREAMING TABLE accounts_cleaned_stream
AS SELECT
  TRIM(account_id) AS account_id,
  customer_id,
  TRIM(account_type) AS account_type,
  UPPER(TRIM(currency)) AS currency,
  TRIM(status) AS status,
  opened_at,
  last_updated,
  is_deleted
FROM STREAM(bronze.accounts_raw);

CREATE FLOW silver_accounts_scd1_flow AS
AUTO CDC INTO silver.accounts_current
FROM STREAM(accounts_cleaned_stream)
KEYS (account_id)
APPLY AS DELETE WHEN is_deleted = 1
SEQUENCE BY last_updated
COLUMNS * EXCEPT (is_deleted)
STORED AS SCD TYPE 1;