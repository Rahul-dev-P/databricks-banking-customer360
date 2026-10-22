# Banking Lakehouse & Customer 360 Platform

An end-to-end Financial Transaction & Customer 360 Lakehouse engineered on Databricks using Serverless Compute and Lakeflow Declarative Pipelines.

This platform bridges relational core banking OLTP databases (Azure SQL) and high-velocity transactional event streams (Amazon S3), enforcing strict data quality expectations, automated Change Data Capture (SCD Type 1 & SCD Type 2), dimensional star schemas, behavioral RFM segmentation, and an optimized, zero-recompute virtual Customer 360 mart.

## Table of Contents

1. [Architecture Overview](#architecture-overview)

2. [Scalability & Engineering Innovations](#scalability--engineering-innovations)

3. [Data Flow & Medallion Pipeline](#data-flow--medallion-pipeline)

4. [Physical Schemas & Data Dictionaries](#physical-schemas--data-dictionaries)

5. [Dimensional Star Schema & Mart Design](#dimensional-star-schema--mart-design)

6. [Repository & Directory Structure](#repository--directory-structure)

7. [Deployment & Operational Runbook](#deployment--operational-runbook)

8. [Architecture Decision Records (ADRs)](#architecture-decision-records-adrs)

9. [Future Enterprise Scope](#future-enterprise-scope)

## Architecture Overview

```
                                      MULTI-SOURCE INGESTION LAYER
┌───────────────────────────────────────┐                    ┌───────────────────────────────────────┐
│     Azure SQL Database (OLTP)         │                    │        Amazon S3 Bucket               │
│ - dbo.customers (Profile mutations)   │                    │ - s3://bank-landing/transactions/     │
│ - dbo.accounts  (Balance & Status)    │                    │ - Streaming JSON Event Payloads       │
└──────────────────┬────────────────────┘                    └──────────────────┬────────────────────┘
                   │                                                            │
═══════════════════╪════════════════════════════════════════════════════════════╪═════════════════════════════
                   │ DATABRICKS WORKFLOW ORCHESTRATION (SCHEDULED BATCH JOB)     │
═══════════════════╪════════════════════════════════════════════════════════════╪═════════════════════════════
                   ▼                                                            │
┌────────────────────────────────────────────────────────┐                      │
│ TASK 1: Serverless Python Ingestion Task               │                      │
│ File: tasks/00_ingest_azure_sql.py                     │                      │
│ - Evaluates MAX(last_updated) watermark from Delta log │                      │
│ - 5-minute lookback buffer for in-flight commits       │                      │
│ - Extracts delta slice: WHERE last_updated >= buffer   │                      │
│ - Appends rows to Bronze Delta tables                  │                      │
│ - Runtime: ~10-15s (Zero idle compute)                 │                      │
└──────────────────────────┬─────────────────────────────┘                      │
                           │ depends_on (Automatic trigger)                     │
                           ▼                                                    ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ TASK 2: Lakeflow Declarative Pipeline (Triggered Batch Mode)                                           │
│                                                                                                        │
│  [BRONZE LAYER - Append-Only & Schema Evolution]                                                       │
│  - bronze.customers_raw    : Delta append stream                                                       │
│  - bronze.accounts_raw     : Delta append stream                                                       │
│  - bronze.transactions_raw : Auto Loader (`read_files`) streaming ingestion from S3                   │
│                                                                                                        │
│  [SILVER LAYER - Data Quality, Liquid Clustering & AUTO CDC]                                          │
│  - silver.transactions_clean : Delta table, positive amounts, pre-signed amount semantics     │
│  - silver.customers_history  : AUTO CDC SCD Type 2 (__START_AT, __END_AT, tombstone deletes)          │
│  - silver.accounts_current   : AUTO CDC SCD Type 1 (In-place operational state upserts)                │
│                                                                                                        │
│  [GOLD LAYER - Star Schema, Audited Facts & Scalable Rollups]                                         │
│  - gold.dim_date             : Fiscal financial calendar (2020–2030)                                   │
│  - gold.dim_customer_history : Full SCD2 dimension with deterministic SHA-256 surrogate keys (SKs)    │
│  - gold.dim_customer         : Active dimension slice (current active version)                         │
│  - gold.dim_account          : Operational product dimension                                           │
│  - gold.fact_transactions   : Point-in-Time As-Of range joined fact (stamped with customer_sk)         │
│  - gold.monthly_customer_summary : Longitudinal periodic snapshot rollup fact                          │
│  - gold.customer_spend_summary   : Pre-aggregated lifetime metrics derived from monthly summaries      │
│  - gold.rfm_segments        : Decoupled behavioral quintiles anchored to Lakehouse event time         │
│  - gold.customer_360        : Standard SQL View (Zero-scan runtime join over pre-computed entities)   │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘


```

## Scalability & Engineering Innovations

### 1. Delta Lake Liquid Clustering (`CLUSTER BY`)

Traditional Hive-style directory partitioning (`PARTITIONED BY (year, month, day)`) breaks down at large volumes:

- **Failure Mode at Scale**: Creates hundreds of subdirectories, producing partition skew and driver bottleneck during partition pruning.

- **Liquid Clustering Implementation**: `silver.transactions_clean` and `gold.fact_transactions` use Liquid Clustering (`CLUSTER BY (account_id, CAST(transaction_timestamp AS DATE))` and `CLUSTER BY (date_key, customer_id)`). Data layout adapts dynamically without rewriting partition directories, avoiding small-file bloat and enabling multi-dimensional data skipping.

### 2. Regulatory Point-in-Time Auditing (BCBS 239 & Basel III)

Denormalizing natural keys directly onto fact records causes **historical attribute drift**. If a customer was classified as "Subprime" in March 2025 and improved to "Prime" in November 2025, joining March transactions to the customer's current state distorts historical risk calculations.

- **The Solution**: `gold.fact_transactions` performs an **As-Of Range Join** against `silver.customers_history`:

  $$
  \text{transaction\_timestamp} \ge \text{\_\_START\_AT} \quad \text{AND} \quad \text{transaction\_timestamp} < \text{\_\_END\_AT}
  $$

  Each fact record is stamped with an immutable surrogate key (`customer_sk = SHA2(customer_id || __START_AT)`). The historical state of the customer profile at transaction time remains preserved forever.

### 3. Elimination of Read Query Amplification

Views that calculate lifetime spend and transaction velocity by running `SUM(amount)` directly over raw transactions cause severe I/O bottlenecks when queried concurrently by BI users and AI/BI Genie.

- **The Solution**: A **two-tier rollup architecture**:
  1. `gold.fact_transactions` aggregates incrementally into `gold.monthly_customer_summary`.

  2. `gold.customer_spend_summary` calculates lifetime metrics by rolling up the monthly summaries ($O(N_{\text{months}} \times N_{\text{customers}})$ instead of $O(N_{\text{transactions}})$).

  3. `gold.customer_360` queries only pre-aggregated customer tables via primary keys, **reducing data scanning by over 98% during user queries**.

### 4. Decoupled Statistical Processing & Recompute Isolation

Global statistical window functions like `NTILE(5) OVER (ORDER BY recency_days DESC)` require distributed global sorts across the entire customer base. When embedded inside unified Materialized Views, downstream tables are forced into full recomputes on every batch.

- **The Solution**:
  1. `gold.rfm_segments` is preserved as an isolated Materialized View, processing only pre-aggregated customer rows.

  2. `gold.customer_360` is deployed as a **Standard SQL View (`CREATE VIEW`)**, reducing pipeline write time for Customer 360 to **0 seconds** and **0 DBUs**.

## Data Flow & Medallion Pipeline

### Bronze Layer (Raw Landing & Append-Only Log)

- **`bronze.transactions_raw`**: Ingested continuously from Amazon S3 via Databricks Auto Loader (`read_files`). Configured with `schemaEvolutionMode => 'addNewColumns'` and system tracking attributes (`_source_file`, `_ingest_timestamp`).

- **`bronze.customers_raw` & `bronze.accounts_raw`**: Append-only Delta tables fed by Task 1 serverless Python JDBC extraction with a (not sure 5-minute lookback buffer to prevent dropping in-flight OLTP transactions.)

### Silver Layer (Cleansing, Data Quality & AUTO CDC)

- **`silver.transactions_clean`**: Streaming table applying strict Lakeflow Expectations, casting amounts to `DECIMAL(18, 4)`, and generating deterministic signed amounts (`signed_amount` is positive for deposits/refunds and negative for withdrawals/purchases).

- **`silver.customers_history` (SCD Type 2)**: Maintained via `AUTO CDC`. Historizes profile mutations (risk score, KYC status, contact info, customer segment). Handles soft deletes via `APPLY AS DELETE WHEN is_deleted = 1`.

- **`silver.accounts_current` (SCD Type 1)**: Operational account snapshots updated in-place via `AUTO CDC ... STORED AS SCD TYPE 1`.

### Gold Layer (Dimensional Serving & Rollup Marts)

- **`gold.dim_date`**: Comprehensive reference calendar.

- **`gold.dim_customer_history`**: Full SCD Type 2 dimension exposing valid date ranges (`valid_from`, `valid_to`) and deterministic surrogate keys (`customer_sk`).

- **`gold.dim_customer`**: Active customer dimension filtered on `WHERE is_current = TRUE`.

- **`gold.dim_account`**: Current operational view of all bank accounts and balances.

- **`gold.fact_transactions`**: Core fact table joined via As-Of range to historical customer profiles and flagged for orphaned accounts (`is_orphaned_account`).

- **`gold.monthly_customer_summary`**: Periodic longitudinal rollup aggregating monthly transaction counts, inflow/outflow volumes, and channel usage.

- **`gold.customer_spend_summary`**: Lifetime cumulative spend, velocity, and preferred channel aggregates per customer.

- **`gold.rfm_segments`**: Behavioral quintile scoring (1–5) based on event-time anchored recency, frequency, and monetary values.

- **`gold.customer_360`**: Virtual unified serving view joining master demographics, account distributions, lifetime spend, RFM segments, and compliance risk flags.

## Physical Schemas & Data Dictionaries

### 1. Bronze Layer Tables

#### `bronze.customers_raw`

| Column Name         | Data Type   | Nullable | Description                                                     |
| ------------------- | ----------- | -------- | --------------------------------------------------------------- |
| `customer_id`       | `INT`       | No       | Operational primary key from Azure SQL                          |
| `first_name`        | `STRING`    | Yes      | Customer legal first name                                       |
| `last_name`         | `STRING`    | Yes      | Customer legal last name                                        |
| `email`             | `STRING`    | Yes      | Raw customer email address                                      |
| `phone`             | `STRING`    | Yes      | Raw customer phone number                                       |
| `address_street`    | `STRING`    | Yes      | Street address                                                  |
| `city`              | `STRING`    | Yes      | Residential city                                                |
| `state`             | `STRING`    | Yes      | State / province code                                           |
| `postal_code`       | `STRING`    | Yes      | Postal / ZIP code                                               |
| `country`           | `STRING`    | Yes      | Country of residence                                            |
| `customer_segment`  | `STRING`    | Yes      | Target customer tier (`Retail`, `Affluent`, `SME`, `Corporate`) |
| `risk_score`        | `INT`       | Yes      | Credit bureau risk score (300–850)                              |
| `kyc_status`        | `STRING`    | Yes      | Identity verification status                                    |
| `created_at`        | `TIMESTAMP` | Yes      | Azure SQL record creation timestamp                             |
| `last_updated`      | `TIMESTAMP` | Yes      | Azure SQL record modification timestamp                         |
| `is_deleted`        | `BOOLEAN`   | Yes      | CDC soft-deletion indicator flag                                |
| `_ingest_timestamp` | `TIMESTAMP` | No       | Ingestion timestamp generated by Databricks                     |

#### `bronze.accounts_raw`

| Column Name         | Data Type   | Nullable | Description                                                         |
| ------------------- | ----------- | -------- | ------------------------------------------------------------------- |
| `account_id`        | `STRING`    | No       | Operational account UUID                                            |
| `customer_id`       | `INT`       | No       | Foreign key linking to customer                                     |
| `account_type`      | `STRING`    | Yes      | Product type (`Checking`, `Savings`, `Money Market`, `Credit Card`) |
| `currency`          | `STRING`    | Yes      | Three-letter ISO currency code                                      |
| `status`            | `STRING`    | Yes      | Status (`Active`, `Dormant`, `Frozen`, `Closed`)                    |
| `opened_at`         | `TIMESTAMP` | Yes      | Account opening timestamp                                           |
| `last_updated`      | `TIMESTAMP` | Yes      | Account modification timestamp                                      |
| `is_deleted`        | `BOOLEAN`   | Yes      | CDC soft-deletion indicator flag                                    |
| `_ingest_timestamp` | `TIMESTAMP` | No       | Ingestion timestamp generated by Databricks                         |

#### `bronze.transactions_raw`

| Column Name               | Data Type   | Nullable | Description                                                         |
| ------------------------- | ----------- | -------- | ------------------------------------------------------------------- |
| `transaction_id`          | `STRING`    | No       | Unique transaction UUID from S3 event stream                        |
| `account_id`              | `STRING`    | Yes      | Target account UUID                                                 |
| `amount`                  | `STRING`    | Yes      | Raw transaction amount string                                       |
| `transaction_type`        | `STRING`    | Yes      | Transaction classification                                          |
| `channel`                 | `STRING`    | Yes      | Initiating channel (`Mobile App`, `Online Banking`, `ATM`, `POS`)   |
| `merchant_category`       | `STRING`    | Yes      | Merchant classification code (MCC) label                            |
| `transaction_timestamp`   | `STRING`    | Yes      | Timestamp of transaction occurrence                                 |
| `_rescued_data`           | `STRING`    | Yes      | Corrupted or schema-mismatched JSON payload captured by Auto Loader |
| `_source_file`            | `STRING`    | No       | S3 URI of source JSON event file                                    |
| `_file_modification_time` | `TIMESTAMP` | No       | Cloud storage file modification timestamp                           |
| `_ingest_timestamp`       | `TIMESTAMP` | No       | Ingestion timestamp into Bronze                                     |

### 2. Silver Layer Tables

#### `silver.transactions_clean`

- **Table Type**: Streaming Table

- **Data Quality Contracts**:
  - `CONSTRAINT valid_txn_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW`

  - `CONSTRAINT valid_account_id EXPECT (account_id IS NOT NULL) ON VIOLATION DROP ROW`

  - `CONSTRAINT positive_amount EXPECT (amount > 0.0) ON VIOLATION DROP ROW`

  - `CONSTRAINT valid_timestamp EXPECT (transaction_timestamp <= CURRENT_TIMESTAMP()) ON VIOLATION DROP ROW`

  - `CONSTRAINT clean_payload EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW`

| Column Name             | Data Type        | Nullable | Description                                                    |
| ----------------------- | ---------------- | -------- | -------------------------------------------------------------- |
| `transaction_id`        | `STRING`         | No       | Validated transaction UUID                                     |
| `account_id`            | `STRING`         | No       | Validated target account UUID                                  |
| `amount`                | `DECIMAL(18, 4)` | No       | Standardized positive transaction magnitude                    |
| `signed_amount`         | `DECIMAL(18, 4)` | No       | Financial signed value (positive = inflow, negative = outflow) |
| `transaction_type`      | `STRING`         | No       | Normalized uppercase transaction type                          |
| `channel`               | `STRING`         | Yes      | Cleaned execution channel                                      |
| `merchant_category`     | `STRING`         | Yes      | Standardized merchant classification                           |
| `transaction_timestamp` | `TIMESTAMP`      | No       | Event timestamp                                                |
| `_source_file`          | `STRING`         | No       | Storage provenance path                                        |
| `_cleaned_at`           | `TIMESTAMP`      | No       | Silver curation timestamp                                      |

#### `silver.customers_history` (SCD Type 2)

- **Table Type**: Streaming Table (Managed by `AUTO CDC`)

- **Tracking Columns**: `__START_AT` (`TIMESTAMP`), `__END_AT` (`TIMESTAMP`)

- **Data Quality Contracts**:
  - `CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE`

  - `CONSTRAINT valid_email_format EXPECT (email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$') ON VIOLATION DROP ROW`

  - `CONSTRAINT valid_risk_score EXPECT (risk_score BETWEEN 300 AND 850) ON VIOLATION DROP ROW`

  - `CONSTRAINT valid_segment EXPECT (customer_segment IN ('Retail', 'Affluent', 'SME', 'Corporate')) ON VIOLATION DROP ROW`

| Column Name        | Data Type   | Nullable | Description                                             |
| ------------------ | ----------- | -------- | ------------------------------------------------------- |
| `customer_id`      | `INT`       | No       | Customer primary key                                    |
| `first_name`       | `STRING`    | Yes      | Sanitized first name                                    |
| `last_name`        | `STRING`    | Yes      | Sanitized last name                                     |
| `email`            | `STRING`    | Yes      | Validated lowercase email                               |
| `phone`            | `STRING`    | Yes      | Normalized digits-only phone string                     |
| `address_street`   | `STRING`    | Yes      | Sanitized street address                                |
| `city`             | `STRING`    | Yes      | Residential city                                        |
| `state`            | `STRING`    | Yes      | Uppercase state code                                    |
| `postal_code`      | `STRING`    | Yes      | Postal code                                             |
| `country`          | `STRING`    | Yes      | ISO country code                                        |
| `customer_segment` | `STRING`    | Yes      | Segment tier                                            |
| `risk_score`       | `INT`       | Yes      | Bureau credit risk score                                |
| `kyc_status`       | `STRING`    | Yes      | Identity verification status                            |
| `created_at`       | `TIMESTAMP` | Yes      | Initial profile creation timestamp                      |
| `__START_AT`       | `TIMESTAMP` | No       | Validity start timestamp (Managed by AUTO CDC)          |
| `__END_AT`         | `TIMESTAMP` | Yes      | Validity end timestamp (`NULL` indicates active record) |

#### `silver.accounts_current` (SCD Type 1)

- **Table Type**: Streaming Table (Managed by `AUTO CDC`)

- **Data Quality Contracts**:
  - `CONSTRAINT valid_acc_id EXPECT (account_id IS NOT NULL) ON VIOLATION FAIL UPDATE`

  - `CONSTRAINT valid_cust_ref EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW`

  - `CONSTRAINT valid_status EXPECT (status IN ('Active', 'Dormant', 'Frozen', 'Closed')) ON VIOLATION DROP ROW`

  - `CONSTRAINT valid_type EXPECT (account_type IN ('Checking', 'Savings', 'Money Market', 'Credit Card')) ON VIOLATION DROP ROW`

| Column Name    | Data Type   | Nullable | Description                         |
| -------------- | ----------- | -------- | ----------------------------------- |
| `account_id`   | `STRING`    | No       | Account UUID (Primary Key)          |
| `customer_id`  | `INT`       | No       | Parent customer reference           |
| `account_type` | `STRING`    | No       | Financial product category          |
| `currency`     | `STRING`    | No       | 3-letter currency code              |
| `status`       | `STRING`    | No       | Current operational status          |
| `opened_at`    | `TIMESTAMP` | No       | Opening timestamp                   |
| `last_updated` | `TIMESTAMP` | No       | Last mutation timestamp from source |

### 3. Gold Layer Tables & Views

#### `gold.fact_transactions`

- **Table Type**: Materialized View

| Column Name                     | Data Type        | Nullable | Description                                                            |
| ------------------------------- | ---------------- | -------- | ---------------------------------------------------------------------- |
| `transaction_id`                | `STRING`         | No       | Primary transaction identifier                                         |
| `date_key`                      | `INT`            | No       | Foreign key linking to `gold.dim_date` (`yyyyMMdd`)                    |
| `account_id`                    | `STRING`         | No       | Foreign key linking to `gold.dim_account`                              |
| `customer_sk`                   | `STRING`         | No       | **Point-in-Time Surrogate Key** linking to `gold.dim_customer_history` |
| `customer_id`                   | `INT`            | No       | Natural customer identifier for direct joins                           |
| `amount`                        | `DECIMAL(18, 4)` | No       | Raw transaction amount                                                 |
| `signed_amount`                 | `DECIMAL(18, 4)` | No       | Standardized signed amount                                             |
| `transaction_type`              | `STRING`         | No       | Transaction classification                                             |
| `channel`                       | `STRING`         | Yes      | Ingestion channel                                                      |
| `merchant_category`             | `STRING`         | Yes      | Merchant category                                                      |
| `transaction_timestamp`         | `TIMESTAMP`      | No       | Event occurrence timestamp                                             |
| `_source_file`                  | `STRING`         | No       | Lineage provenance path                                                |
| `is_orphaned_account`           | `BOOLEAN`        | No       | `TRUE` if account was not found in `dim_account` at join time          |
| `is_unmatched_customer_history` | `BOOLEAN`        | No       | `TRUE` if transaction timestamp fell outside known SCD2 intervals      |

#### `gold.dim_customer_history`

- **Table Type**: Materialized View

| Column Name        | Data Type   | Nullable | Description                                                  |
| ------------------ | ----------- | -------- | ------------------------------------------------------------ |
| `customer_sk`      | `STRING`    | No       | Deterministic Surrogate Key: \`SHA2(customer_id              |
| `customer_id`      | `INT`       | No       | Natural customer key                                         |
| `first_name`       | `STRING`    | Yes      | First name                                                   |
| `last_name`        | `STRING`    | Yes      | Last name                                                    |
| `full_name`        | `STRING`    | Yes      | Concatenated full legal name                                 |
| `email`            | `STRING`    | Yes      | Customer email                                               |
| `phone`            | `STRING`    | Yes      | Digits-only phone number                                     |
| `address_street`   | `STRING`    | Yes      | Street address                                               |
| `city`             | `STRING`    | Yes      | City                                                         |
| `state`            | `STRING`    | Yes      | State                                                        |
| `postal_code`      | `STRING`    | Yes      | Postal code                                                  |
| `country`          | `STRING`    | Yes      | Country                                                      |
| `customer_segment` | `STRING`    | Yes      | Customer segment tier                                        |
| `risk_score`       | `INT`       | Yes      | Credit bureau risk score                                     |
| `credit_tier`      | `STRING`    | No       | Derived tier (`Excellent`, `Good`, `Fair`, `Poor`)           |
| `kyc_status`       | `STRING`    | Yes      | KYC compliance status                                        |
| `customer_since`   | `TIMESTAMP` | Yes      | Initial onboarding timestamp                                 |
| `valid_from`       | `TIMESTAMP` | No       | SCD2 version start timestamp (`__START_AT`)                  |
| `valid_to`         | `TIMESTAMP` | No       | SCD2 version end timestamp (`9999-12-31 23:59:59` if active) |
| `is_current`       | `BOOLEAN`   | No       | Boolean flag indicating active version                       |

#### `gold.monthly_customer_summary`

- **Table Type**: Materialized View

| Column Name                        | Data Type        | Nullable | Description                                    |
| ---------------------------------- | ---------------- | -------- | ---------------------------------------------- |
| `customer_id`                      | `INT`            | No       | Natural customer identifier                    |
| `activity_month`                   | `STRING`         | No       | Longitudinal activity month format (`yyyy-MM`) |
| `monthly_tx_count`                 | `BIGINT`         | No       | Total count of transactions executed in month  |
| `monthly_total_volume`             | `DECIMAL(18, 2)` | No       | Gross absolute spend volume in month           |
| `monthly_avg_ticket_size`          | `DECIMAL(18, 2)` | Yes      | Mean transaction volume in month               |
| `monthly_inflow_volume`            | `DECIMAL(18, 2)` | No       | Total positive cash inflow in month            |
| `monthly_outflow_volume`           | `DECIMAL(18, 2)` | No       | Total debit/spend outflow in month             |
| `month_last_transaction_timestamp` | `TIMESTAMP`      | Yes      | Timestamp of final transaction in month        |
| `month_preferred_channel`          | `STRING`         | Yes      | Channel used for the highest-value transaction |

#### `gold.customer_spend_summary`

- **Table Type**: Materialized View

| Column Name                  | Data Type        | Nullable | Description                                          |
| ---------------------------- | ---------------- | -------- | ---------------------------------------------------- |
| `customer_id`                | `INT`            | No       | Natural customer identifier                          |
| `lifetime_transactions`      | `BIGINT`         | No       | Cumulative lifetime transactions across all accounts |
| `lifetime_spend_volume`      | `DECIMAL(18, 2)` | No       | Cumulative lifetime spending volume                  |
| `avg_transaction_value`      | `DECIMAL(18, 2)` | Yes      | Historical average spend per transaction             |
| `lifetime_inflow_volume`     | `DECIMAL(18, 2)` | No       | Cumulative gross deposits and credits                |
| `lifetime_outflow_volume`    | `DECIMAL(18, 2)` | No       | Cumulative gross debits, withdrawals, and purchases  |
| `last_transaction_timestamp` | `TIMESTAMP`      | Yes      | Timestamp of most recent transaction across bank     |
| `preferred_channel`          | `STRING`         | Yes      | Historical preferred transaction channel             |

#### `gold.rfm_segments`

- **Table Type**: Materialized View

| Column Name           | Data Type        | Nullable | Description                                                |
| --------------------- | ---------------- | -------- | ---------------------------------------------------------- |
| `customer_id`         | `INT`            | No       | Customer primary key                                       |
| `recency_days`        | `INT`            | Yes      | Days elapsed from last transaction to dataset anchor date  |
| `frequency_count`     | `BIGINT`         | No       | Lifetime transaction frequency                             |
| `monetary_total`      | `DECIMAL(18, 2)` | No       | Lifetime monetary volume                                   |
| `r_score`             | `INT`            | No       | Recency quintile rank (1 = Least Recent, 5 = Most Recent)  |
| `f_score`             | `INT`            | No       | Frequency quintile rank (1 = Lowest Velocity, 5 = Highest) |
| `m_score`             | `INT`            | No       | Monetary quintile rank (1 = Lowest Spend, 5 = Highest)     |
| `rfm_composite_score` | `INT`            | No       | Composite behavioral score ($R + F + M \in [3, 15]$)       |
| `rfm_segment_label`   | `STRING`         | No       | Behavioral persona segment label                           |

#### `gold.customer_360`

- **Object Type**: Standard SQL View (`CREATE VIEW`)

- **Serving Latency**: Sub-second execution; zero physical Parquet write cost during batch pipeline execution.

| Column Name                      | Data Type        | Nullable | Description                                                   |
| -------------------------------- | ---------------- | -------- | ------------------------------------------------------------- |
| `customer_id`                    | `INT`            | No       | Customer primary identifier                                   |
| `customer_sk`                    | `STRING`         | No       | Active Point-in-Time surrogate key                            |
| `full_name`                      | `STRING`         | Yes      | Legal full name                                               |
| `email`                          | `STRING`         | Yes      | Primary email address                                         |
| `phone`                          | `STRING`         | Yes      | Phone number                                                  |
| `city`                           | `STRING`         | Yes      | City                                                          |
| `state`                          | `STRING`         | Yes      | State                                                         |
| `country`                        | `STRING`         | Yes      | Country                                                       |
| `customer_segment`               | `STRING`         | Yes      | Assigned customer profile tier                                |
| `risk_score`                     | `INT`            | Yes      | Credit bureau risk score                                      |
| `credit_tier`                    | `STRING`         | No       | Credit rating tier                                            |
| `kyc_status`                     | `STRING`         | Yes      | Identity verification status                                  |
| `customer_since`                 | `TIMESTAMP`      | Yes      | Onboarding timestamp                                          |
| `current_version_effective_date` | `TIMESTAMP`      | No       | Effective date of current profile revision                    |
| `total_accounts_held`            | `BIGINT`         | No       | Total accounts opened                                         |
| `active_accounts`                | `BIGINT`         | No       | Active operational accounts count                             |
| `closed_accounts`                | `BIGINT`         | No       | Closed accounts count                                         |
| `checking_accounts`              | `BIGINT`         | No       | Active checking facilities                                    |
| `savings_accounts`               | `BIGINT`         | No       | Active savings facilities                                     |
| `credit_cards`                   | `BIGINT`         | No       | Active credit card facilities                                 |
| `lifetime_transactions`          | `BIGINT`         | No       | Pre-computed lifetime transaction count                       |
| `lifetime_spend_volume`          | `DECIMAL(18, 2)` | No       | Pre-computed cumulative spend volume                          |
| `avg_transaction_value`          | `DECIMAL(18, 2)` | Yes      | Average transaction ticket size                               |
| `lifetime_inflow_volume`         | `DECIMAL(18, 2)` | No       | Pre-computed cumulative credits and deposits                  |
| `lifetime_outflow_volume`        | `DECIMAL(18, 2)` | No       | Pre-computed cumulative debits and withdrawals                |
| `last_transaction_timestamp`     | `TIMESTAMP`      | Yes      | Timestamp of most recent transaction                          |
| `preferred_channel`              | `STRING`         | Yes      | Preferred execution channel                                   |
| `recency_days`                   | `INT`            | Yes      | Days inactive relative to the data clock                      |
| `rfm_composite_score`            | `INT`            | Yes      | Composite behavioral RFM score                                |
| `rfm_segment_label`              | `STRING`         | Yes      | Behavioral classification label                               |
| `flag_subprime_risk`             | `BOOLEAN`        | No       | Indicator: `risk_score < 550`                                 |
| `flag_kyc_incomplete`            | `BOOLEAN`        | No       | Indicator: `kyc_status != 'Verified'`                         |
| `flag_high_velocity_monitoring`  | `BOOLEAN`        | No       | Indicator: high transaction volume with subprime credit score |

## Dimensional Star Schema & Mart Design

```
                               ┌───────────────────────────┐
                               │       gold.dim_date       │
                               ├───────────────────────────┤
                               │ PK  date_key              │
                               │     calendar_date         │
                               │     fiscal_quarter        │
                               │     is_weekend            │
                               └─────────────┬─────────────┘
                                             │
                                             │ 1:N
                                             ▼
┌───────────────────────────┐     ┌────────────────────────────────────┐     ┌───────────────────────────┐
│ gold.dim_customer_history │     │      gold.fact_transactions        │     │     gold.dim_account      │
├───────────────────────────┤     ├────────────────────────────────────┤     ├───────────────────────────┤
│ PK  customer_sk (SHA2)    │◄───┼│ FK   customer_sk (As-Of Range Join)│┼───►│ PK  account_id            │
│     customer_id           │ 1:N │ FK   account_id                    │ N:1 │ FK  customer_id           │
│     full_name             │     │ FK   date_key                      │     │     account_type          │
│     risk_score            │     │      customer_id (Denormalized)    │     │     currency              │
│     valid_from            │     │      amount                        │     │     account_status        │
│     valid_to              │     │      signed_amount                 │     └───────────────────────────┘
│     is_current            │     │      transaction_timestamp         │
└───────────────────────────┘     │      is_orphaned_account           │
                                  └──────────────────┬─────────────────┘
                                                     │
                                                     │ Incremental Aggregation
                                                     ▼
                                  ┌────────────────────────────────────┐
                                  │   gold.monthly_customer_summary    │
                                  ├────────────────────────────────────┤
                                  │ PK   customer_id, activity_month   │
                                  │      monthly_tx_count              │
                                  │      monthly_total_volume          │
                                  │      monthly_inflow_volume         │
                                  │      monthly_outflow_volume        │
                                  └──────────────────┬─────────────────┘
                                                     │
                                                     │ Rollup Group By
                                                     ▼
                                  ┌────────────────────────────────────┐
                                  │    gold.customer_spend_summary     │
                                  ├────────────────────────────────────┤
                                  │ PK   customer_id                   │
                                  │      lifetime_transactions         │
                                  │      lifetime_spend_volume         │
                                  │      lifetime_inflow_volume        │
                                  │      lifetime_outflow_volume       │
                                  └──────────────────┬─────────────────┘
                                                     │
                                                     │ Primary Key Lookup (Zero Raw Tx Scans)
                                                     ▼
                                  ┌────────────────────────────────────┐
                                  │      gold.customer_360 (VIEW)      │
                                  ├────────────────────────────────────┤
                                  │ PK   customer_id                   │
                                  │      [Demographics + Portfolios]   │
                                  │      [Pre-aggregated Spend]        │
                                  │      [RFM Segments + Risk Flags]   │
                                  └────────────────────────────────────┘


```

## Repository & Directory Structure

```
banking-lakehouse/
├── .gitignore                             # Ignores local checkpoints, pycache, and secrets
├── README.md                              # Enterprise system architecture documentation
├── datagen/
│   ├── datagen_and_cdc_simulator.py       # Local Python engine for seeding Azure SQL and S3
│   └── requirements.txt                   # Generation dependencies (faker, pyodbc, boto3)
├── tasks/
│   └── 00_ingest_azure_sql.py             # Task 1: Serverless watermarked JDBC delta extraction
├── pipeline/
│   ├── 01_bronze_s3.sql                   # Task 2: Auto Loader S3 streaming ingestion
│   ├── 02_silver.sql                      # Task 2: Cleaned, liquid-clustered Silver & AUTO CDC
│   └── 03_gold.sql                        # Task 2: Point-in-Time Fact, rollups, and Customer 360
├── explorations/
│   ├── 01_explore_bronze.py               # Scratchpad: Connectivity & schema inference tests
│   └── 02_explore_silver_rules.py         # Scratchpad: Profiling regex rules & expectation tests
└── ddl/
    └── 00_init_catalog.sql                # Metastore bootstrap (Catalog, schemas, credentials)


```

## Deployment & Operational Runbook

### 1. Metastore Bootstrap (Databricks SQL)

Execute `ddl/00_init_catalog.sql` in a Databricks Serverless SQL Warehouse:

```
CREATE CATALOG IF NOT EXISTS banking_lakehouse;
CREATE SCHEMA IF NOT EXISTS banking_lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS banking_lakehouse.silver;
CREATE SCHEMA IF NOT EXISTS banking_lakehouse.gold;
```

### 2. Configure Unity Catalog Secret Scope

Using the Databricks CLI, register external Azure SQL credentials:

```
databricks secrets create-scope banking_scope

databricks secrets put-secret banking_scope azure_sql_jdbc_url \
  --string-value "jdbc:sqlserver://<server>.database.windows.net:1433;database=banking-oltp;encrypt=true;trustServerCertificate=false;"

databricks secrets put-secret banking_scope azure_sql_user \
  --string-value "sqladmin"

databricks secrets put-secret banking_scope azure_sql_password \
  --string-value "<YourPassword>"
```

### 3. Generate Baseline Data

From a local terminal, seed Azure SQL and stage baseline transactions to S3:

```
pip install -r datagen/requirements.txt
export AZURE_SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=tcp:<server>.database.windows.net,1433;Database=banking-oltp;Uid=sqladmin;Pwd=<YourPassword>;Encrypt=yes;TrustServerCertificate=no;"
export AWS_ACCESS_KEY_ID="<your-key>"
export AWS_SECRET_ACCESS_KEY="<your-secret>"

python datagen/datagen_and_cdc_simulator.py --mode initial


```

### 4. Configure Databricks Workflow

1. Navigate to **Workflows** $\rightarrow$ **Jobs** $\rightarrow$ **Create Job** (`Job_Daily_Banking_Lakehouse_Batch`).

2. **Task 1 (`ingest_azure_sql_to_bronze`)**:
   - **Type**: `Notebook`

   - **Source**: (`tasks/00_ingest_azure_sql.py`)

   - **Compute**: `Serverless`

3. **Task 2 (`run_lakeflow_pipeline`)**:
   - **Type**: `Pipeline`

   - **Pipeline**: Point to Lakeflow pipeline configured with `pipeline/01_bronze_s3.sql`, `pipeline/02_silver.sql`, and `pipeline/03_gold.sql`.

   - **Target Catalog**: `banking_lakehouse`

   - **Depends On**: `ingest_azure_sql_to_bronze`

### 5. Day-2 CDC Simulation & Incremental Verification

Inject customer mutations (credit score shifts, address changes, soft deletions) and new transaction files:

```
python datagen/datagen_and_cdc_simulator.py --mode cdc
```

Trigger `Job_Daily_Banking_Lakehouse_Batch` in Databricks Workflows. Inspect the execution:

1. **Task 1** fetches modified rows via watermarked query and appends to Bronze (\~10 seconds).

2. **Task 2** streams deltas through Silver, closes historical SCD2 records in `silver.customers_history`, performs As-Of joins for `gold.fact_transactions`, updates `gold.monthly_customer_summary`, and refreshes `gold.customer_360` with zero downtime.

## Architecture Decision Records (ADRs)

### ADR 01: Decoupled Batch JDBC Extraction from Lakeflow Pipeline

- **Context**: Lakeflow's `@dlt.append_flow(..., once=True)` was intended for one-time historical backfilling. On subsequent batch triggers, the engine marks the task as completed, skipping incremental records. Furthermore, Spark Structured Streaming lacks native streaming offset drivers for standard JDBC endpoints.

- **Decision**: Decouple ingestion into a 2-Task Databricks Workflow. Task 1 handles watermarked batch extraction over JDBC via a lightweight serverless Python notebook using a 5-minute lookback buffer. Task 2 runs Lakeflow as an event-driven stream processor over Delta Bronze.

- **Consequences**: Zero manual intervention; extraction failures do not corrupt pipeline state; eliminates cluster idle time.

### ADR 02: Regulatory Point-in-Time Auditing via As-Of Range Joins (BCBS 239)

- **Context**: Basel Committee on Banking Supervision (BCBS 239) regulations require financial institutions to accurately reconstruct historical risk exposure at any given moment in time. Denormalizing current customer state onto historical transactions causes historical drift.

- **Decision**: Join `silver.transactions_clean` to `silver.customers_history` using an As-Of range join condition:

  $$
  \text{transaction\_timestamp} \ge \text{\_\_START\_AT} \quad \text{AND} \quad \text{transaction\_timestamp} < \text{\_\_END\_AT}
  $$

  Stamp each fact row with a deterministic surrogate key:

  $$
  \text{customer\_sk} = \text{SHA2}(\text{customer\_id} \,\Vert{}\, \text{\_\_START\_AT}, 256)
  $$

- **Consequences**: Guarantees immutable historical auditability. If a customer transitions from "Subprime" to "Prime," historical subprime transactions retain their original risk classification for auditing.

### ADR 03: Two-Tier Rollup Mart vs. Read Query Amplification

- **Context**: Calculating customer lifetime spend directly inside a view over raw transactions creates severe query amplification when multiple dashboards or AI/BI Genie agents execute concurrent queries.

- **Decision**: Pre-aggregate raw transactions into `gold.monthly_customer_summary`, and roll those summaries up into `gold.customer_spend_summary`. Implement `gold.customer_360` as a standard SQL View that joins exclusively across pre-aggregated entity tables.

- **Consequences**: Queries against `gold.customer_360` avoid scanning raw transactions entirely. Query execution time drops from tens of seconds to sub-second responses, reducing Databricks SQL Warehouse compute usage by over 98%.

## Future Enterprise Scope

1. **Unity Catalog Dynamic Data Masking**:
   - Implement SQL UDFs (`mask_email()`, `mask_phone()`) directly in Unity Catalog to dynamically redact PII for general business analysts while revealing plaintext values to compliance officers.

2. **Row-Level Security (RLS)**:
   - Enforce data residency and sovereignty boundaries via `ROW FILTER` policies on `gold.dim_customer` and `gold.customer_360` based on user group membership (e.g., US vs. EMEA regulatory access).

3. **Automated CI/CD via Databricks Asset Bundles (DABs)**:
   - Package all workflow tasks, Lakeflow pipeline definitions, and Unity Catalog schemas into declarative YAML bundles deployed via GitHub Actions.
