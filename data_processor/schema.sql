-- Add indexes for frequently accessed columns
CREATE INDEX IF NOT EXISTS idx_client_num ON clients(ACNTS_CLIENT_NUM);
CREATE INDEX IF NOT EXISTS idx_opening_date ON clients(ACNTS_OPENING_DATE);
CREATE INDEX IF NOT EXISTS idx_last_tran ON clients(ACNTS_LAST_TRAN_DATE);
CREATE INDEX IF NOT EXISTS idx_prod_code ON clients(ACNTS_PROD_CODE);
CREATE INDEX IF NOT EXISTS idx_dormant ON clients(ACNTS_DORMANT_ACNT);
CREATE INDEX IF NOT EXISTS idx_inop ON clients(ACNTS_INOP_ACNT);
