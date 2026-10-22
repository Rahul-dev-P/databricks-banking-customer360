-- Create Customers Master Table
CREATE TABLE dbo.customers (
    customer_id INT IDENTITY(10001, 1) PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50) NOT NULL,
    email NVARCHAR(100) NOT NULL,
    phone NVARCHAR(25) NULL,
    address_street NVARCHAR(150) NOT NULL,
    city NVARCHAR(60) NOT NULL,
    state NVARCHAR(50) NOT NULL,
    postal_code NVARCHAR(20) NOT NULL,
    country NVARCHAR(50) NOT NULL DEFAULT 'USA',
    customer_segment NVARCHAR(20) NOT NULL CHECK (customer_segment IN ('Retail', 'Affluent', 'SME', 'Corporate')),
    risk_score INT NOT NULL CHECK (risk_score BETWEEN 300 AND 850),
    kyc_status NVARCHAR(20) NOT NULL CHECK (kyc_status IN ('Verified', 'Pending', 'Action Required', 'Rejected')),
    created_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    last_updated DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    is_deleted BIT NOT NULL DEFAULT 0
);


-- Create Accounts Master Table
CREATE TABLE dbo.accounts (
    account_id VARCHAR(36) PRIMARY KEY,
    customer_id INT NOT NULL,
    account_type VARCHAR(20) NOT NULL CHECK (account_type IN ('Checking', 'Savings', 'Money Market', 'Credit Card')),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    status VARCHAR(20) NOT NULL CHECK (status IN ('Active', 'Dormant', 'Frozen', 'Closed')),
    opened_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    last_updated DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    is_deleted BIT NOT NULL DEFAULT 0,
    CONSTRAINT FK_accounts_customers FOREIGN KEY (customer_id) REFERENCES dbo.customers (customer_id)
);

