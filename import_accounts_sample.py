import sqlite3
import os
import pandas as pd
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

def import_sample_accounts(file_path, sample_size=1000):
    """Import a sample of ACCOUNTS data for testing."""
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} not found")
            return False
        
        # Connect to database
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Drop and recreate clients table
        print("Recreating clients table...")
        cursor.execute("DROP TABLE IF EXISTS clients")
        cursor.execute("""
        CREATE TABLE clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ACNTS_CLIENT_NUM INTEGER,
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
        
        # Read sample data
        print(f"Reading sample of {sample_size} records from {file_path}...")
        df = pd.read_csv(file_path, nrows=sample_size)
        total_records = len(df)
        
        # Generate sample data if some columns are missing
        df['ACNTS_CLIENT_NUM'] = range(1001, 1001 + total_records)
        df['ACNTS_AC_NAME1'] = df.get('ACNTS_AC_NAME1', 'Client') + ' ' + df['ACNTS_CLIENT_NUM'].astype(str)
        df['ACNTS_PROD_CODE'] = df.get('ACNTS_PROD_CODE', 1)
        
        # Generate random values for missing columns
        import numpy as np
        
        if 'ACNTS_ATM_OPERN' not in df.columns:
            df['ACNTS_ATM_OPERN'] = np.random.choice([0, 1], size=total_records, p=[0.3, 0.7])
        
        if 'ACNTS_INET_OPERN' not in df.columns:
            df['ACNTS_INET_OPERN'] = np.random.choice([0, 1], size=total_records, p=[0.2, 0.8])
            
        if 'ACNTS_SMS_OPERN' not in df.columns:
            df['ACNTS_SMS_OPERN'] = np.random.choice([0, 1], size=total_records, p=[0.4, 0.6])
            
        if 'ACNTS_SALARY_ACNT' not in df.columns:
            df['ACNTS_SALARY_ACNT'] = np.random.choice([0, 1], size=total_records, p=[0.7, 0.3])
            
        if 'ACNTS_CR_CARDS_ALLOWED' not in df.columns:
            df['ACNTS_CR_CARDS_ALLOWED'] = np.random.choice([0, 1], size=total_records, p=[0.5, 0.5])
        
        # Generate sample data for ACNTS_INOP_ACNT and ACNTS_DORMANT_ACNT if missing
        if 'ACNTS_INOP_ACNT' not in df.columns:
            df['ACNTS_INOP_ACNT'] = np.random.choice([0, 1], size=total_records, p=[0.8, 0.2])
            
        if 'ACNTS_DORMANT_ACNT' not in df.columns:
            # Only dormant if inoperative (business rule)
            df['ACNTS_DORMANT_ACNT'] = 0
            dormant_mask = (df['ACNTS_INOP_ACNT'] == 1)
            dormant_count = dormant_mask.sum()
            if dormant_count > 0:
                # 30% of inoperative accounts become dormant
                df.loc[dormant_mask, 'ACNTS_DORMANT_ACNT'] = np.random.choice(
                    [0, 1], 
                    size=dormant_count, 
                    p=[0.7, 0.3]
                )
        
        # Insert data into table
        print("Inserting data into clients table...")
        data_to_insert = []
        for i, row in df.iterrows():
            # Convert ACNTS_INOP_ACNT and ACNTS_DORMANT_ACNT to integers
            inop_acnt = 0
            if pd.notna(row['ACNTS_INOP_ACNT']):
                if isinstance(row['ACNTS_INOP_ACNT'], str):
                    inop_acnt = 1 if row['ACNTS_INOP_ACNT'].lower() == 'yes' else 0
                else:
                    inop_acnt = int(bool(row['ACNTS_INOP_ACNT']))
            
            dormant_acnt = 0
            if pd.notna(row['ACNTS_DORMANT_ACNT']):
                if isinstance(row['ACNTS_DORMANT_ACNT'], str):
                    dormant_acnt = 1 if row['ACNTS_DORMANT_ACNT'].lower() == 'yes' else 0
                else:
                    dormant_acnt = int(bool(row['ACNTS_DORMANT_ACNT']))
            
            # Add sample dates
            from datetime import datetime, timedelta
            
            opening_date = datetime.now() - timedelta(days=365 * 2)  # 2 years ago
            last_tran_date = None
            
            if inop_acnt == 1:
                # Last transaction was more than 90 days ago
                last_tran_date = datetime.now() - timedelta(days=100 + (i % 100))
            else:
                # Last transaction was recent
                last_tran_date = datetime.now() - timedelta(days=i % 60)
                
            if dormant_acnt == 1:
                # Last transaction was more than 180 days ago (90 inoperative + 90 dormant)
                last_tran_date = datetime.now() - timedelta(days=200 + (i % 100))
            
            # Format dates
            opening_date_str = opening_date.strftime('%Y-%m-%d')
            last_tran_date_str = last_tran_date.strftime('%Y-%m-%d') if last_tran_date else None
            
            data_to_insert.append((
                int(row['ACNTS_CLIENT_NUM']),
                str(row['ACNTS_AC_NAME1']),
                str(row.get('ACNTS_AC_NAME2', '')),
                opening_date_str,
                last_tran_date_str,
                int(row['ACNTS_PROD_CODE']),
                int(row['ACNTS_ATM_OPERN']),
                int(row['ACNTS_INET_OPERN']),
                int(row['ACNTS_SMS_OPERN']),
                int(row['ACNTS_SALARY_ACNT']),
                int(row['ACNTS_CR_CARDS_ALLOWED']),
                dormant_acnt,
                inop_acnt
            ))
        
        # Insert records in a single transaction
        conn.execute("BEGIN TRANSACTION")
        try:
            cursor.executemany("""
            INSERT INTO clients (
                ACNTS_CLIENT_NUM, ACNTS_AC_NAME1, ACNTS_AC_NAME2,
                ACNTS_OPENING_DATE, ACNTS_LAST_TRAN_DATE, ACNTS_PROD_CODE,
                ACNTS_ATM_OPERN, ACNTS_INET_OPERN, ACNTS_SMS_OPERN,
                ACNTS_SALARY_ACNT, ACNTS_CR_CARDS_ALLOWED,
                ACNTS_DORMANT_ACNT, ACNTS_INOP_ACNT
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, data_to_insert)
            
            conn.commit()
            print(f"Successfully imported {len(data_to_insert)} sample records")
            
            # Create indexes
            print("Creating indexes...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_client_num ON clients(ACNTS_CLIENT_NUM)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_dormant ON clients(ACNTS_DORMANT_ACNT)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_inop ON clients(ACNTS_INOP_ACNT)")
            conn.commit()
            
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Error inserting data: {e}")
            return False
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error in import_sample_accounts: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python import_accounts_sample.py <path_to_accounts_csv> [sample_size]")
        sys.exit(1)
        
    file_path = sys.argv[1]
    sample_size = 1000
    
    if len(sys.argv) > 2:
        try:
            sample_size = int(sys.argv[2])
        except ValueError:
            print(f"Invalid sample size: {sys.argv[2]}. Using default: 1000")
    
    if import_sample_accounts(file_path, sample_size):
        print("Sample accounts imported successfully!")
    else:
        print("Failed to import sample accounts") 