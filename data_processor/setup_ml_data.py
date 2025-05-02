import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import os

def load_real_data():
    """Load data from CSV files"""
    print("Loading data from CSV files...")
    
    # Get the datasets directory path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    datasets_dir = os.path.join(current_dir, 'datasets')
    
    # Load clients data
    print("Loading clients data...")
    clients_df = pd.read_csv(os.path.join(datasets_dir, 'CLIENTS.csv'), low_memory=False)
    # Drop duplicates keeping the first occurrence
    print("Removing duplicate client records...")
    clients_df = clients_df.drop_duplicates(subset=['ACNTS_CLIENT_NUM'], keep='first')
    # Convert large integers to strings
    for col in clients_df.select_dtypes(include=['int64']).columns:
        clients_df[col] = clients_df[col].astype(str)
    
    # Load transactions data in chunks
    print("Loading transactions data...")
    chunks = []
    for chunk in pd.read_csv(os.path.join(datasets_dir, 'all_active_accounts___transacting_.csv'), 
                            chunksize=10000):  # Process 10,000 rows at a time
        # Convert large integers to strings in each chunk
        for col in chunk.select_dtypes(include=['int64']).columns:
            chunk[col] = chunk[col].astype(str)
        chunks.append(chunk)
    transactions_df = pd.concat(chunks)
    
    # Load products data
    print("Loading products data...")
    products_df = pd.read_csv(os.path.join(datasets_dir, 'products.csv'))
    # Drop duplicates keeping the first occurrence
    print("Removing duplicate product records...")
    products_df = products_df.drop_duplicates(subset=['PRODUCT_CODE'], keep='first')
    # Convert large integers to strings
    for col in products_df.select_dtypes(include=['int64']).columns:
        products_df[col] = products_df[col].astype(str)
    
    # Load charges data
    print("Loading charges data...")
    charges_df = pd.read_csv(os.path.join(datasets_dir, 'charges.csv'))
    # Convert large integers to strings
    for col in charges_df.select_dtypes(include=['int64']).columns:
        charges_df[col] = charges_df[col].astype(str)
    
    return clients_df, transactions_df, products_df, charges_df

def setup_database():
    """Set up database with schema and real data"""
    print("Setting up database...")
    
    # Connect to database
    conn = sqlite3.connect('banking_data.db')
    cursor = conn.cursor()
    
    try:
        # Load real data first
        print("Loading data...")
        clients_df, transactions_df, products_df, charges_df = load_real_data()
        
        # Drop existing tables if they exist
        print("Dropping existing tables...")
        cursor.execute("DROP TABLE IF EXISTS transactions")
        cursor.execute("DROP TABLE IF EXISTS charges")
        cursor.execute("DROP TABLE IF EXISTS products")
        cursor.execute("DROP TABLE IF EXISTS retention_action_types")
        cursor.execute("DROP TABLE IF EXISTS retention_actions")
        cursor.execute("DROP TABLE IF EXISTS data_quality_issues")
        cursor.execute("DROP TABLE IF EXISTS clients")
        
        # Create tables
        print("Creating tables...")
        
        # Clients table with all columns from CSV
        cursor.execute("""
        CREATE TABLE clients (
            ACNTS_ENTITY_NUM TEXT,
            ACNTS_INTERNAL_ACNUM TEXT,
            ACNTS_BRN_CODE TEXT,
            ACNTS_CLIENT_NUM TEXT PRIMARY KEY,
            ACNTS_AC_SEQ_NUM TEXT,
            ACNTS_ACCOUNT_NUMBER TEXT,
            ACNTS_PROD_CODE TEXT,
            ACNTS_AC_TYPE TEXT,
            ACNTS_AC_SUB_TYPE TEXT,
            ACNTS_SCHEME_CODE TEXT,
            ACNTS_OPENING_DATE TEXT,
            ACNTS_AC_NAME1 TEXT,
            ACNTS_AC_NAME2 TEXT,
            ACNTS_SHORT_NAME TEXT,
            ACNTS_AC_ADDR1 TEXT,
            ACNTS_AC_ADDR2 TEXT,
            ACNTS_AC_ADDR3 TEXT,
            ACNTS_AC_ADDR4 TEXT,
            ACNTS_AC_ADDR5 TEXT,
            ACNTS_LOCN_CODE TEXT,
            ACNTS_CURR_CODE TEXT,
            ACNTS_GLACC_CODE TEXT,
            ACNTS_SALARY_ACNT TEXT,
            ACNTS_PASSBK_REQD TEXT,
            ACNTS_DCHANNEL_CODE TEXT,
            ACNTS_MKT_CHANNEL_CODE TEXT,
            ACNTS_MKT_BY_STAFF TEXT,
            ACNTS_MKT_BY_BRN TEXT,
            ACNTS_DSA_CODE TEXT,
            ACNTS_MODE_OF_OPERN TEXT,
            ACNTS_MOPR_ADDN_INFO TEXT,
            ACNTS_REPAYABLE_TO TEXT,
            ACNTS_SPECIAL_ACNT TEXT,
            ACNTS_NOMINATION_REQD TEXT,
            ACNTS_CREDIT_INT_REQD TEXT,
            ACNTS_MINOR_ACNT TEXT,
            ACNTS_POWER_OF_ATTORNEY TEXT,
            ACNTS_CONNP_INV_NUM TEXT,
            ACNTS_NUM_SIG_COMB TEXT,
            ACNTS_TELLER_OPERN TEXT,
            ACNTS_ATM_OPERN TEXT,
            ACNTS_CALL_CENTER_OPERN TEXT,
            ACNTS_INET_OPERN TEXT,
            ACNTS_CR_CARDS_ALLOWED TEXT,
            ACNTS_KIOSK_BANKING TEXT,
            ACNTS_SMS_OPERN TEXT,
            ACNTS_OD_ALLOWED TEXT,
            ACNTS_CHQBK_REQD TEXT,
            ACNTS_ARM_CRM TEXT,
            ACNTS_ARM_ROLE TEXT,
            ACNTS_BUSDIVN_CODE TEXT,
            ACNTS_CREATION_STATUS TEXT,
            ACNTS_BASE_DATE TEXT,
            ACNTS_INOP_ACNT TEXT,
            ACNTS_DORMANT_ACNT TEXT,
            ACNTS_LAST_TRAN_DATE TEXT,
            ACNTS_NONSYS_LAST_DATE TEXT,
            ACNTS_INT_CALC_UPTO TEXT,
            ACNTS_MMB_INT_ACCR_UPTO TEXT,
            ACNTS_INT_ACCR_UPTO TEXT,
            ACNTS_INT_DBCR_UPTO TEXT,
            ACNTS_TRF_TO_OVERDUE TEXT,
            ACNTS_DB_FREEZED TEXT,
            ACNTS_CR_FREEZED TEXT,
            ACNTS_CONTRACT_BASED_FLG TEXT,
            ACNTS_ACST_UPTO_DATE TEXT,
            ACNTS_LAST_STMT_NUM TEXT,
            ACNTS_LAST_CHQBK_ISSUED TEXT,
            ACNTS_CLOSURE_DATE TEXT,
            ACNTS_PREOPEN_ENTD_BY TEXT,
            ACNTS_PREOPEN_ENTD_ON TEXT,
            ACNTS_PREOPEN_LAST_MOD_BY TEXT,
            ACNTS_PREOPEN_LAST_MOD_ON TEXT,
            ACNTS_ENTD_BY TEXT,
            ACNTS_ENTD_ON TEXT,
            ACNTS_LAST_MOD_BY TEXT,
            ACNTS_LAST_MOD_ON TEXT,
            ACNTS_AUTH_BY TEXT,
            ACNTS_AUTH_ON TEXT,
            TBA_MAIN_KEY TEXT,
            ACNTS_TENOR TEXT,
            ACNTS_TENOR_DMY TEXT,
            ACNTS_MATURITY_DATE TEXT,
            ACNTS_SALARY_PAY_AC TEXT,
            ACNTS_APPL_NUM TEXT,
            ACNTS_SLIPS_DIRECT_DB TEXT,
            ACNTS_UDCH_CODE TEXT,
            ACNTS_UDCH_SCHEME TEXT,
            ACNTS_ESCHEAT_ACNT TEXT,
            ACNTS_LAST_DB_INT_RECOVRY_DATE TEXT,
            ACNTS_MBLBNK_OPERN TEXT
        )""")
        
        # Data quality issues table
        cursor.execute("""
        CREATE TABLE data_quality_issues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            issue_type TEXT NOT NULL,
            issue_details TEXT,
            detected_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            resolved_at TEXT,
            resolution_notes TEXT,
            FOREIGN KEY (client_id) REFERENCES clients(ACNTS_CLIENT_NUM)
        )""")
        
        # Retention actions table
        cursor.execute("""
        CREATE TABLE retention_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            type TEXT NOT NULL,
            description TEXT NOT NULL,
            action_date TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            priority INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(ACNTS_CLIENT_NUM)
        )""")
        
        # Retention action types table
        cursor.execute("""
        CREATE TABLE retention_action_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL UNIQUE,
            description_template TEXT NOT NULL,
            conditions TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0
        )""")
        
        # Products table - using actual columns from CSV
        cursor.execute("""
        CREATE TABLE products (
            PRODUCT_CODE TEXT PRIMARY KEY,
            PRODUCT_NAME TEXT,
            PRODUCT_CONC_NAME TEXT,
            PRODUCT_ALPHA_ID TEXT,
            PRODUCT_GROUP_CODE TEXT,
            PRODUCT_CLASS TEXT,
            PRODUCT_FOR_DEPOSITS TEXT,
            PRODUCT_FOR_LOANS TEXT,
            PRODUCT_FOR_RUN_ACS TEXT,
            PRODUCT_OD_FACILITY TEXT,
            PRODUCT_REVOLVING_FACILITY TEXT,
            PRODUCT_FOR_CALL_DEP TEXT,
            PRODUCT_CONTRACT_ALLOWED TEXT,
            PRODUCT_CONTRACT_NUM_GEN TEXT,
            PRODUCT_EXEMPT_FROM_NPA TEXT,
            PRODUCT_REQ_APPLN_ENTRY TEXT,
            PRODUCT_FOR_RFC TEXT,
            PRODUCT_FOR_FCLS TEXT,
            PRODUCT_FOR_FCNR TEXT,
            PRODUCT_FOR_EEFC TEXT,
            PRODUCT_FOR_TRADE_FINANCE TEXT,
            PRODUCT_FOR_LOCKERS TEXT,
            PRODUCT_INDIRECT_EXP_REQD TEXT,
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
            PRODUCT_FOR_FIXED_ASSETS TEXT,
            PRODUCT_FOR_SAFE_CUS TEXT
        )""")
        
        # Transactions table
        cursor.execute("""
        CREATE TABLE transactions (
            transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            transaction_date TEXT NOT NULL,
            amount REAL NOT NULL,
            transaction_type TEXT NOT NULL,
            channel TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'completed',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(ACNTS_CLIENT_NUM)
        )""")
        
        # Insert data
        print("Inserting clients data...")
        clients_df.to_sql('clients', conn, if_exists='append', index=False)
        
        print("Inserting products data...")
        products_df.to_sql('products', conn, if_exists='append', index=False)
        
        print("Inserting transactions data...")
        # Insert transactions in chunks to avoid memory issues
        chunk_size = 10000
        for i in range(0, len(transactions_df), chunk_size):
            chunk = transactions_df.iloc[i:i + chunk_size]
            chunk.to_sql('transactions', conn, if_exists='append', index=False)
            print(f"Inserted {i + len(chunk)} transactions...")
        
        print("Inserting charges data...")
        charges_df.to_sql('charges', conn, if_exists='append', index=False)
        
        # Create indexes after data is loaded
        print("Creating indexes...")
        cursor.execute("CREATE INDEX idx_dq_client_id ON data_quality_issues(client_id)")
        cursor.execute("CREATE INDEX idx_dq_status ON data_quality_issues(status)")
        cursor.execute("CREATE INDEX idx_dq_type ON data_quality_issues(issue_type)")
        
        cursor.execute("CREATE INDEX idx_retention_client_id ON retention_actions(client_id)")
        cursor.execute("CREATE INDEX idx_retention_status ON retention_actions(status)")
        cursor.execute("CREATE INDEX idx_retention_date ON retention_actions(action_date)")
        cursor.execute("CREATE INDEX idx_retention_priority ON retention_actions(priority)")
        
        cursor.execute("CREATE INDEX idx_product_class ON products(PRODUCT_CLASS)")
        cursor.execute("CREATE INDEX idx_product_group ON products(PRODUCT_GROUP_CODE)")
        
        cursor.execute("CREATE INDEX idx_transaction_client ON transactions(client_id)")
        cursor.execute("CREATE INDEX idx_transaction_date ON transactions(transaction_date)")
        cursor.execute("CREATE INDEX idx_transaction_type ON transactions(transaction_type)")
        cursor.execute("CREATE INDEX idx_transaction_channel ON transactions(channel)")
        
        conn.commit()
        
        # Print summary
        cursor.execute("SELECT COUNT(*) FROM transactions")
        transaction_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM products")
        product_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM clients")
        client_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM charges")
        charges_count = cursor.fetchone()[0]
        
        print("\nDatabase Setup Complete:")
        print(f"- {client_count} clients")
        print(f"- {product_count} products")
        print(f"- {transaction_count} transactions")
        print(f"- {charges_count} charges")
        
    except Exception as e:
        print(f"Error setting up database: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    setup_database()
