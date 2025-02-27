-- Clients table schema (existing)
CREATE TABLE IF NOT EXISTS clients (
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

-- Data quality issues tracking table (new)
CREATE TABLE IF NOT EXISTS data_quality_issues (
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

-- Create index for faster lookups
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
    conditions TEXT NOT NULL,  -- JSON string of conditions that trigger this action
    priority INTEGER NOT NULL DEFAULT 0
);

-- Insert default retention action types
INSERT OR IGNORE INTO retention_action_types (type, description_template, conditions, priority) VALUES
('Account Review', 'Schedule account review to discuss financial goals and product options', 
 '{"days_since_last_transaction": 60, "product_count": "<2"}', 1),
 
('Premium Upgrade', 'Offer premium account benefits based on transaction history',
 '{"current_clv": ">5000", "product_type": "standard"}', 2),
 
('Digital Activation', 'Encourage digital banking adoption with guided walkthrough',
 '{"digital_usage_score": "<30"}', 1),
 
('Product Bundle', 'Recommend complementary products based on usage patterns',
 '{"product_count": "1", "activity_score": ">70"}', 2),
 
('Loyalty Boost', 'Special points promotion for increased engagement',
 '{"days_since_last_transaction": ">30", "days_since_last_transaction": "<90"}', 1),
 
('Reactivation', 'Personalized offer to restore account activity',
 '{"days_since_last_transaction": ">90"}', 3),
 
('Credit Card Offer', 'Pre-approved credit card with special terms',
 '{"salary_account": "true", "credit_card": "false", "activity_score": ">60"}', 2);
