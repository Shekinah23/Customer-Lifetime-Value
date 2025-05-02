-- Enable foreign key constraints
PRAGMA foreign_keys = ON;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS data_quality_issues;
DROP TABLE IF EXISTS retention_actions;
DROP TABLE IF EXISTS retention_action_types;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS clients;

-- Clients table schema (must be created first for foreign key constraints)
CREATE TABLE clients (
    ACNTS_CLIENT_NUM INTEGER PRIMARY KEY,
    ACNTS_AC_NAME1 TEXT,
    ACNTS_AC_NAME2 TEXT,
    ACNTS_OPENING_DATE TEXT,
    ACNTS_LAST_TRAN_DATE TEXT,
    ACNTS_PROD_CODE INTEGER,
    ACNTS_ATM_OPERN INTEGER,
    ACNTS_INET_OPERN INTEGER,
    ACNTS_SMS_OPERN INTEGER,
    ACNTS_SALARY_ACNT INTEGER,
    ACNTS_CR_CARDS_ALLOWED INTEGER,
    ACNTS_DORMANT_ACNT INTEGER,
    ACNTS_INOP_ACNT INTEGER
);

-- Data quality issues tracking table
CREATE TABLE data_quality_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    issue_type TEXT NOT NULL,
    issue_details TEXT,
    detected_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    resolved_at TEXT,
    resolution_notes TEXT,
    FOREIGN KEY (client_id) REFERENCES clients(ACNTS_CLIENT_NUM)
);

-- Create index for data quality issues
CREATE INDEX IF NOT EXISTS idx_dq_client_id ON data_quality_issues(client_id);
CREATE INDEX IF NOT EXISTS idx_dq_status ON data_quality_issues(status);
CREATE INDEX IF NOT EXISTS idx_dq_type ON data_quality_issues(issue_type);

-- Retention actions table
CREATE TABLE IF NOT EXISTS retention_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    type TEXT NOT NULL,
    description TEXT NOT NULL,
    action_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(ACNTS_CLIENT_NUM)
);

-- Create indexes for retention actions
CREATE INDEX IF NOT EXISTS idx_retention_client_id ON retention_actions(client_id);
CREATE INDEX IF NOT EXISTS idx_retention_status ON retention_actions(status);
CREATE INDEX IF NOT EXISTS idx_retention_date ON retention_actions(action_date);
CREATE INDEX IF NOT EXISTS idx_retention_priority ON retention_actions(priority);

-- Predefined retention action types
CREATE TABLE IF NOT EXISTS retention_action_types (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL UNIQUE,
    description_template TEXT NOT NULL,
    conditions TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0
);

-- Insert default retention action types
INSERT OR IGNORE INTO retention_action_types (type, description_template, conditions, priority) VALUES 
('Account Review', 'Schedule account review to discuss financial goals and product options', '{"days_since_last_transaction": 60, "product_count": "<2"}', 1),
('Premium Upgrade', 'Offer premium account benefits based on transaction history', '{"current_clv": ">5000", "product_type": "standard"}', 2),
('Digital Activation', 'Encourage digital banking adoption with guided walkthrough', '{"digital_usage_score": "<30"}', 1),
('Product Bundle', 'Recommend complementary products based on usage patterns', '{"product_count": "1", "activity_score": ">70"}', 2),
('Loyalty Boost', 'Special points promotion for increased engagement', '{"days_since_last_transaction": ">30", "days_since_last_transaction": "<90"}', 1),
('Reactivation', 'Personalized offer to restore account activity', '{"days_since_last_transaction": ">90"}', 3),
('Credit Card Offer', 'Pre-approved credit card with special terms', '{"salary_account": "true", "credit_card": "false", "activity_score": ">60"}', 2);

-- Products table
CREATE TABLE IF NOT EXISTS products (
    PRODUCT_CODE INTEGER PRIMARY KEY,
    PRODUCT_NAME TEXT,
    PRODUCT_CONC_NAME TEXT,
    PRODUCT_ALPHA_ID TEXT,
    PRODUCT_GROUP_CODE TEXT,
    PRODUCT_CLASS TEXT,
    PRODUCT_FOR_DEPOSITS INTEGER,
    PRODUCT_FOR_LOANS INTEGER,
    PRODUCT_FOR_RUN_ACS INTEGER,
    PRODUCT_OD_FACILITY INTEGER,
    PRODUCT_REVOLVING_FACILITY INTEGER,
    PRODUCT_FOR_CALL_DEP INTEGER,
    PRODUCT_CONTRACT_ALLOWED INTEGER,
    PRODUCT_CONTRACT_NUM_GEN TEXT,
    PRODUCT_EXEMPT_FROM_NPA INTEGER,
    PRODUCT_REQ_APPLN_ENTRY INTEGER,
    PRODUCT_FOR_RFC INTEGER,
    PRODUCT_FOR_FCLS INTEGER,
    PRODUCT_FOR_FCNR INTEGER,
    PRODUCT_FOR_EEFC INTEGER,
    PRODUCT_FOR_TRADE_FINANCE INTEGER,
    PRODUCT_FOR_LOCKERS INTEGER,
    PRODUCT_INDIRECT_EXP_REQD INTEGER,
    PRODUCT_BUSDIVN_CODE TEXT,
    PRODUCT_GLACC_CODE TEXT,
    PRODUCT_REVOKED_ON TEXT,
    PRODUCT_ENTD_BY TEXT,
    PRODUCT_ENTD_ON TEXT,
    PRODUCT_LAST_MOD_BY TEXT,
    PRODUCT_LAST_MOD_ON TEXT,
    PRODUCT_AUTH_BY TEXT,
    PRODUCT_AUTH_ON TEXT,
    TBA_MAIN_KEY TEXT,
    PRODUCT_FOR_FIXED_ASSETS INTEGER,
    PRODUCT_FOR_SAFE_CUS INTEGER
);

-- Create indexes for products
CREATE INDEX IF NOT EXISTS idx_product_code ON products(PRODUCT_CODE);
CREATE INDEX IF NOT EXISTS idx_product_name ON products(PRODUCT_NAME);
CREATE INDEX IF NOT EXISTS idx_product_group ON products(PRODUCT_GROUP_CODE);
CREATE INDEX IF NOT EXISTS idx_product_class ON products(PRODUCT_CLASS);

-- Transactions table for pattern analysis
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    transaction_date TEXT NOT NULL,
    amount REAL NOT NULL,
    transaction_type TEXT NOT NULL,
    channel TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'completed',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(ACNTS_CLIENT_NUM)
);

-- Create indexes for transactions
CREATE INDEX IF NOT EXISTS idx_transaction_client ON transactions(client_id);
CREATE INDEX IF NOT EXISTS idx_transaction_date ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transaction_type ON transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_transaction_channel ON transactions(channel);

-- Loan tables
CREATE TABLE IF NOT EXISTS loan_info (
    proj_id TEXT NOT NULL,
    loan_type TEXT NOT NULL,
    original_amount REAL NOT NULL,
    interest_rate REAL NOT NULL,
    start_date TEXT NOT NULL,
    maturity_date TEXT NOT NULL,
    monthly_payment REAL NOT NULL,
    ACC_NUM TEXT NOT NULL,
    PRIMARY KEY (proj_id, loan_type, start_date)
);

CREATE TABLE IF NOT EXISTS loan_balance (
    proj_id TEXT NOT NULL,
    outstanding_balance REAL NOT NULL,
    days_past_due INTEGER NOT NULL DEFAULT 0,
    last_payment_date TEXT,
    next_payment_date TEXT,
    PRIMARY KEY (proj_id),
    FOREIGN KEY (proj_id) REFERENCES loan_info(proj_id)
);

CREATE TABLE IF NOT EXISTS loan_payments (
    payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    proj_id TEXT NOT NULL,
    payment_date TEXT NOT NULL,
    amount REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    FOREIGN KEY (proj_id) REFERENCES loan_info(proj_id)
);

-- Create indexes for loan tables
CREATE INDEX IF NOT EXISTS idx_loan_info_acc ON loan_info(ACC_NUM);
CREATE INDEX IF NOT EXISTS idx_loan_type ON loan_info(loan_type);
CREATE INDEX IF NOT EXISTS idx_loan_start ON loan_info(start_date);
CREATE INDEX IF NOT EXISTS idx_loan_maturity ON loan_info(maturity_date);
CREATE INDEX IF NOT EXISTS idx_loan_payment_date ON loan_payments(payment_date);
CREATE INDEX IF NOT EXISTS idx_loan_payment_status ON loan_payments(status);
