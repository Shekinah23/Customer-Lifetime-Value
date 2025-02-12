import sqlite3
import os
import csv
from datetime import datetime

def parse_date(date_str):
    if not date_str:
        return None
    try:
        # Try different date formats
        formats = [
            '%m/%d/%Y %I:%M %p',  # e.g., "4/3/2023 3:37 PM"
            '%Y-%m-%d %H:%M:%S'   # fallback format
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
        return None
    except:
        return None

def init_db():
    db_path = os.path.join(os.path.dirname(__file__), 'banking_data.db')
    print(f"Initializing database at: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Drop existing products table
    cursor.execute("DROP TABLE IF EXISTS products")
    
    # Create products table with full schema
    cursor.execute("""
    CREATE TABLE products (
        PRODUCT_CODE INTEGER PRIMARY KEY,
        PRODUCT_NAME TEXT NOT NULL,
        PRODUCT_CONC_NAME TEXT,
        PRODUCT_ALPHA_ID TEXT,
        PRODUCT_GROUP_CODE TEXT NOT NULL,
        PRODUCT_CLASS TEXT NOT NULL,
        PRODUCT_FOR_DEPOSITS INTEGER NOT NULL,
        PRODUCT_FOR_LOANS INTEGER NOT NULL,
        PRODUCT_FOR_RUN_ACS INTEGER NOT NULL,
        PRODUCT_OD_FACILITY INTEGER NOT NULL,
        PRODUCT_REVOLVING_FACILITY INTEGER NOT NULL,
        PRODUCT_FOR_CALL_DEP INTEGER NOT NULL,
        PRODUCT_CONTRACT_ALLOWED INTEGER NOT NULL,
        PRODUCT_CONTRACT_NUM_GEN TEXT,
        PRODUCT_EXEMPT_FROM_NPA INTEGER NOT NULL,
        PRODUCT_REQ_APPLN_ENTRY INTEGER NOT NULL,
        PRODUCT_FOR_RFC INTEGER NOT NULL,
        PRODUCT_FOR_FCLS INTEGER NOT NULL,
        PRODUCT_FOR_FCNR INTEGER NOT NULL,
        PRODUCT_FOR_EEFC INTEGER NOT NULL,
        PRODUCT_FOR_TRADE_FINANCE INTEGER NOT NULL,
        PRODUCT_FOR_LOCKERS INTEGER NOT NULL,
        PRODUCT_INDIRECT_EXP_REQD INTEGER NOT NULL,
        PRODUCT_BUSDIVN_CODE TEXT,
        PRODUCT_GLACC_CODE INTEGER,
        PRODUCT_REVOKED_ON TEXT,
        PRODUCT_ENTD_BY TEXT,
        PRODUCT_ENTD_ON TEXT,
        PRODUCT_LAST_MOD_BY TEXT,
        PRODUCT_LAST_MOD_ON TEXT,
        PRODUCT_AUTH_BY TEXT,
        PRODUCT_AUTH_ON TEXT,
        TBA_MAIN_KEY TEXT,
        PRODUCT_FOR_FIXED_ASSETS INTEGER DEFAULT 0,
        PRODUCT_FOR_SAFE_CUS INTEGER DEFAULT 0
    )
    """)
    
    # Read products from CSV
    csv_path = os.path.join(os.path.dirname(__file__), 'data_processor', 'datasets', 'products.csv')
    print(f"Reading products from: {csv_path}")
    
    with open(csv_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        products = []
        for row in csv_reader:
            # Convert empty strings to None for TEXT fields
            for key in row:
                if row[key] == '':
                    row[key] = None
                elif key in ['PRODUCT_CODE', 'PRODUCT_GLACC_CODE', 'PRODUCT_FOR_DEPOSITS', 'PRODUCT_FOR_LOANS',
                           'PRODUCT_FOR_RUN_ACS', 'PRODUCT_OD_FACILITY', 'PRODUCT_REVOLVING_FACILITY',
                           'PRODUCT_FOR_CALL_DEP', 'PRODUCT_CONTRACT_ALLOWED', 'PRODUCT_EXEMPT_FROM_NPA',
                           'PRODUCT_REQ_APPLN_ENTRY', 'PRODUCT_FOR_RFC', 'PRODUCT_FOR_FCLS', 'PRODUCT_FOR_FCNR',
                           'PRODUCT_FOR_EEFC', 'PRODUCT_FOR_TRADE_FINANCE', 'PRODUCT_FOR_LOCKERS',
                           'PRODUCT_INDIRECT_EXP_REQD', 'PRODUCT_FOR_FIXED_ASSETS', 'PRODUCT_FOR_SAFE_CUS']:
                    row[key] = int(row[key]) if row[key] else 0

            # Parse dates
            row['PRODUCT_ENTD_ON'] = parse_date(row['PRODUCT_ENTD_ON'])
            row['PRODUCT_LAST_MOD_ON'] = parse_date(row['PRODUCT_LAST_MOD_ON'])
            row['PRODUCT_AUTH_ON'] = parse_date(row['PRODUCT_AUTH_ON'])
            
            products.append((
                row['PRODUCT_CODE'], row['PRODUCT_NAME'], row['PRODUCT_CONC_NAME'],
                row['PRODUCT_ALPHA_ID'], row['PRODUCT_GROUP_CODE'], row['PRODUCT_CLASS'],
                row['PRODUCT_FOR_DEPOSITS'], row['PRODUCT_FOR_LOANS'], row['PRODUCT_FOR_RUN_ACS'],
                row['PRODUCT_OD_FACILITY'], row['PRODUCT_REVOLVING_FACILITY'], row['PRODUCT_FOR_CALL_DEP'],
                row['PRODUCT_CONTRACT_ALLOWED'], row['PRODUCT_CONTRACT_NUM_GEN'],
                row['PRODUCT_EXEMPT_FROM_NPA'], row['PRODUCT_REQ_APPLN_ENTRY'],
                row['PRODUCT_FOR_RFC'], row['PRODUCT_FOR_FCLS'], row['PRODUCT_FOR_FCNR'],
                row['PRODUCT_FOR_EEFC'], row['PRODUCT_FOR_TRADE_FINANCE'], row['PRODUCT_FOR_LOCKERS'],
                row['PRODUCT_INDIRECT_EXP_REQD'], row['PRODUCT_BUSDIVN_CODE'],
                row['PRODUCT_GLACC_CODE'], row['PRODUCT_REVOKED_ON'], row['PRODUCT_ENTD_BY'],
                row['PRODUCT_ENTD_ON'], row['PRODUCT_LAST_MOD_BY'], row['PRODUCT_LAST_MOD_ON'],
                row['PRODUCT_AUTH_BY'], row['PRODUCT_AUTH_ON'], row['TBA_MAIN_KEY'],
                row['PRODUCT_FOR_FIXED_ASSETS'], row['PRODUCT_FOR_SAFE_CUS']
            ))
    
    # Insert products
    cursor.executemany("""
    INSERT INTO products (
        PRODUCT_CODE, PRODUCT_NAME, PRODUCT_CONC_NAME, PRODUCT_ALPHA_ID,
        PRODUCT_GROUP_CODE, PRODUCT_CLASS, PRODUCT_FOR_DEPOSITS, PRODUCT_FOR_LOANS,
        PRODUCT_FOR_RUN_ACS, PRODUCT_OD_FACILITY, PRODUCT_REVOLVING_FACILITY,
        PRODUCT_FOR_CALL_DEP, PRODUCT_CONTRACT_ALLOWED, PRODUCT_CONTRACT_NUM_GEN,
        PRODUCT_EXEMPT_FROM_NPA, PRODUCT_REQ_APPLN_ENTRY, PRODUCT_FOR_RFC,
        PRODUCT_FOR_FCLS, PRODUCT_FOR_FCNR, PRODUCT_FOR_EEFC,
        PRODUCT_FOR_TRADE_FINANCE, PRODUCT_FOR_LOCKERS, PRODUCT_INDIRECT_EXP_REQD,
        PRODUCT_BUSDIVN_CODE, PRODUCT_GLACC_CODE, PRODUCT_REVOKED_ON,
        PRODUCT_ENTD_BY, PRODUCT_ENTD_ON, PRODUCT_LAST_MOD_BY,
        PRODUCT_LAST_MOD_ON, PRODUCT_AUTH_BY, PRODUCT_AUTH_ON, TBA_MAIN_KEY,
        PRODUCT_FOR_FIXED_ASSETS, PRODUCT_FOR_SAFE_CUS
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, products)
    
    conn.commit()
    print(f"Database initialized with {len(products)} products")
    conn.close()

if __name__ == '__main__':
    init_db()
