import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

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
        
        # Import transactions data if available
        try:
            print("\nImporting transactions data...")
            trans_df = pd.read_csv('data_processor/datasets/all_active_accounts___transacting_.csv',
                                encoding='utf-8',
                                low_memory=False,
                                on_bad_lines='skip')
            
            # Convert date columns
            if 'ACNTS_LAST_TRAN_DATE' in trans_df.columns:
                trans_df['ACNTS_LAST_TRAN_DATE'] = pd.to_datetime(trans_df['ACNTS_LAST_TRAN_DATE'], errors='coerce').dt.strftime('%Y-%m-%d')
            
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
