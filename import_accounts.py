import sqlite3
import os
import pandas as pd
import datetime
import sys

def get_db_connection():
    """Connect to the SQLite database."""
    try:
        conn = sqlite3.connect('data_processor/banking_data.db')
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def import_accounts_data(file_path):
    """Import data from the ACCOUNTS dataset into the clients table."""
    try:
        # Validate file exists
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} not found.")
            return False
        
        # Connect to the database
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Check if clients table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='clients'")
        if cursor.fetchone() is None:
            print("Creating clients table...")
            # Create table if it doesn't exist
            cursor.execute("""
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
            )
            """)
        else:
            # Drop existing clients table
            print("Dropping existing clients table...")
            cursor.execute("DROP TABLE clients")
            
            # Recreate clients table
            print("Recreating clients table...")
            cursor.execute("""
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
            )
            """)
        
        # Read the ACCOUNTS dataset
        print(f"Reading data from {file_path}...")
        df = pd.read_csv(file_path)
        
        # Process data and insert into clients table
        total_records = len(df)
        inserted_records = 0
        print(f"Found {total_records} records in the dataset.")
        
        # Begin transaction
        conn.execute("BEGIN TRANSACTION")
        
        try:
            for index, row in df.iterrows():
                # Process the ACNTS_INOP_ACNT and ACNTS_DORMANT_ACNT columns
                # Convert them to integer values (0 = No/False, 1 = Yes/True)
                # Handle different data types - could be int, string, or boolean
                inop_acnt = 0
                if pd.notna(row.get('ACNTS_INOP_ACNT')):
                    if isinstance(row.get('ACNTS_INOP_ACNT'), str):
                        inop_acnt = 1 if row.get('ACNTS_INOP_ACNT').lower() == 'yes' else 0
                    else:
                        inop_acnt = int(bool(row.get('ACNTS_INOP_ACNT')))
                        
                dormant_acnt = 0
                if pd.notna(row.get('ACNTS_DORMANT_ACNT')):
                    if isinstance(row.get('ACNTS_DORMANT_ACNT'), str):
                        dormant_acnt = 1 if row.get('ACNTS_DORMANT_ACNT').lower() == 'yes' else 0
                    else:
                        dormant_acnt = int(bool(row.get('ACNTS_DORMANT_ACNT')))
                
                # Insert each record into the clients table
                cursor.execute('''
                INSERT INTO clients (
                    ACNTS_CLIENT_NUM, ACNTS_AC_NAME1, ACNTS_AC_NAME2, 
                    ACNTS_OPENING_DATE, ACNTS_LAST_TRAN_DATE, ACNTS_PROD_CODE,
                    ACNTS_ATM_OPERN, ACNTS_INET_OPERN, ACNTS_SMS_OPERN,
                    ACNTS_SALARY_ACNT, ACNTS_CR_CARDS_ALLOWED,
                    ACNTS_DORMANT_ACNT, ACNTS_INOP_ACNT
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row.get('ACNTS_CLIENT_NUM', None),
                    row.get('ACNTS_AC_NAME1', ''),
                    row.get('ACNTS_AC_NAME2', ''),
                    row.get('ACNTS_OPENING_DATE', None),
                    row.get('ACNTS_LAST_TRAN_DATE', None),
                    row.get('ACNTS_PROD_CODE', 0),
                    row.get('ACNTS_ATM_OPERN', 0),
                    row.get('ACNTS_INET_OPERN', 0),
                    row.get('ACNTS_SMS_OPERN', 0),
                    row.get('ACNTS_SALARY_ACNT', 0),
                    row.get('ACNTS_CR_CARDS_ALLOWED', 0),
                    dormant_acnt,
                    inop_acnt
                ))
                inserted_records += 1
                
                # Progress indicator every 1000 records
                if inserted_records % 1000 == 0:
                    print(f"Processed {inserted_records}/{total_records} records...")
            
            # Commit the transaction
            conn.commit()
            print(f"Successfully imported {inserted_records} records.")
            
            # Create indexes for better performance
            print("Creating indexes...")
            conn.execute('CREATE INDEX IF NOT EXISTS idx_client_num ON clients(ACNTS_CLIENT_NUM)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_opening_date ON clients(ACNTS_OPENING_DATE)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_last_tran ON clients(ACNTS_LAST_TRAN_DATE)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_prod_code ON clients(ACNTS_PROD_CODE)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_dormant ON clients(ACNTS_DORMANT_ACNT)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_inop ON clients(ACNTS_INOP_ACNT)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_ac_name ON clients(ACNTS_AC_NAME1)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON clients(ACNTS_DORMANT_ACNT, ACNTS_INOP_ACNT)')
            conn.commit()
            
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Error during import: {e}")
            return False
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error in import_accounts_data: {e}")
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python import_accounts.py <path_to_accounts_csv>")
        return
        
    file_path = sys.argv[1]
    if import_accounts_data(file_path):
        print("ACCOUNTS data imported successfully!")
    else:
        print("Failed to import ACCOUNTS data.")

if __name__ == "__main__":
    main() 