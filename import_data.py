import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import os

def import_data():
    """Import data from CSV files into SQLite database"""
    print("Starting import process...")
    
    # Connect to database using same path as app.py and check_db.py
    db_path = 'banking_data.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables using schema
    with open('data_processor/schema.sql', 'r') as f:
        schema = f.read()
        conn.executescript(schema)
    
    try:
        # Import clients data
        print("\nImporting clients data...")
        clients_df = pd.read_csv('data_processor/datasets/CLIENTS.csv', 
                            encoding='utf-8', 
                            low_memory=False,
                            on_bad_lines='skip')
        
        # Convert date columns to proper format
        date_columns = ['ACNTS_OPENING_DATE', 'ACNTS_BASE_DATE', 'ACNTS_LAST_TRAN_DATE', 
                       'ACNTS_NONSYS_LAST_DATE', 'ACNTS_CLOSURE_DATE', 'ACNTS_ENTD_ON', 
                       'ACNTS_LAST_MOD_ON', 'ACNTS_AUTH_ON']
        
        for col in date_columns:
            if col in clients_df.columns:
                clients_df[col] = pd.to_datetime(clients_df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Import to database
        clients_df.to_sql('clients', conn, if_exists='replace', index=False)
        print(f"Imported {len(clients_df)} client records")
        
        # Import accounts data if available, otherwise create from clients data
        accounts_csv_path = 'data_processor/datasets/ACCOUNTS.csv'
        if os.path.exists(accounts_csv_path):
            print("\nImporting accounts data...")
            accounts_df = pd.read_csv(accounts_csv_path, 
                                encoding='utf-8', 
                                low_memory=False,
                                on_bad_lines='skip')
            
            # Ensure required columns exist
            required_columns = [
                'ACNTS_INTERNAL_ACNUM', 'ACNTS_BRN_CODE', 'ACNTS_CLIENT_NUM',
                'ACNTS_PROD_CODE', 'ACNTS_AC_NAME1', 'ACNTS_AC_NAME2',
                'ACNTS_SHORT_NAME', 'ACNTS_CURR_CODE', 'ACNTS_INOP_ACNT', 'ACNTS_DORMANT_ACNT'
            ]
            
            for col in required_columns:
                if col not in accounts_df.columns:
                    if col in ['ACNTS_INOP_ACNT', 'ACNTS_DORMANT_ACNT']:
                        accounts_df[col] = 0  # Default to 0 (not inactive/dormant)
                    else:
                        accounts_df[col] = None  # Default to None for other columns
            
            accounts_df.to_sql('accounts', conn, if_exists='replace', index=False)
            print(f"Imported {len(accounts_df)} account records")
        else:
            print("\nAccounts CSV not found. Creating accounts table from clients data...")
            
            # Create accounts table from clients data
            cursor.execute("""
                INSERT INTO accounts (
                    ACNTS_INTERNAL_ACNUM, ACNTS_BRN_CODE, ACNTS_CLIENT_NUM,
                    ACNTS_PROD_CODE, ACNTS_AC_NAME1, ACNTS_AC_NAME2,
                    ACNTS_SHORT_NAME, ACNTS_CURR_CODE, ACNTS_INOP_ACNT, ACNTS_DORMANT_ACNT
                )
                SELECT 
                    ACNTS_CLIENT_NUM as ACNTS_INTERNAL_ACNUM,
                    1 as ACNTS_BRN_CODE,  -- Default branch code
                    ACNTS_CLIENT_NUM,
                    ACNTS_PROD_CODE,
                    ACNTS_AC_NAME1,
                    ACNTS_AC_NAME2,
                    ACNTS_AC_NAME1 as ACNTS_SHORT_NAME,  -- Use first name as short name
                    'USD' as ACNTS_CURR_CODE,  -- Default currency
                    ACNTS_INOP_ACNT,
                    ACNTS_DORMANT_ACNT
                FROM clients
            """)
            
            # Get count of inserted records
            cursor.execute("SELECT COUNT(*) FROM accounts")
            account_count = cursor.fetchone()[0]
            print(f"Created {account_count} account records from clients data")
        
        # Import transactions data if available
        try:
            print("\nImporting transactions data...")
            trans_df = pd.read_csv('data_processor/datasets/Transactions.csv',
                                encoding='utf-8',
                                low_memory=False,
                                on_bad_lines='skip')
            
            # Convert date columns
            date_columns = ['TRAN_DATE_OF_TRAN', 'TRAN_VALUE_DATE', 'TRAN_ENTD_ON', 'TRAN_AUTH_ON', 'TRAN_CHANNEL_DT_TIME']
            for col in date_columns:
                if col in trans_df.columns:
                    trans_df[col] = pd.to_datetime(trans_df[col], errors='coerce').dt.strftime('%Y-%m-%d')
            
            trans_df.to_sql('transactions', conn, if_exists='replace', index=False)
            print(f"Imported {len(trans_df)} transaction records")
        except Exception as e:
            print(f"Warning: Could not import transactions: {str(e)}")
        
        conn.commit()
        print("\nImport completed successfully")
        
    except Exception as e:
        print(f"Error during import: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    import_data()
