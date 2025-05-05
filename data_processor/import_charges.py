import sqlite3
import csv
import os
import pandas as pd

def import_charges():
    print("Importing charges data...")
    # Path to the database and CSV file
    db_path = 'banking_data.db'
    csv_path = os.path.join('datasets', 'charges.csv')
    
    if not os.path.exists(db_path):
        print(f"Error: Database file {db_path} not found!")
        return False
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found!")
        return False
    
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create charges table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS charges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            CHARGES_ENTITY_NUM INTEGER,
            CHARGES_CHG_CODE TEXT,
            CHARGES_PROD_CODE TEXT,
            CHARGES_AC_TYPE TEXT,
            CHARGES_ACSUB_TYPE TEXT,
            CHARGES_SCHEME_CODE TEXT,
            CHARGES_CHG_TYPE TEXT,
            CHARGES_CHG_CURR TEXT,
            CHARGES_LATEST_EFF_DATE TEXT,
            CHARGES_CHG_AMT_CHOICE INTEGER,
            CHARGES_COMBINATION_CHOICE TEXT,
            CHARGES_CHGS_PERCENTAGE REAL,
            CHARGES_FIXED_AMT REAL,
            CHARGES_VAR_NOOF_SLABS INTEGER,
            CHARGES_SLAB_AMT_CHOICE TEXT,
            CHARGES_CHG_SLAB_CHOICE TEXT,
            CHARGES_OVERALL_MIN_AMT REAL,
            CHARGES_OVERALL_MAX_AMT REAL,
            CHARGES_RNDOFF_CHOICE TEXT,
            CHARGES_RNDOFF_FACTOR REAL
        )
        """)
        
        # Check if table already has data
        cursor.execute("SELECT COUNT(*) FROM charges")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"Charges table already has {count} records. Clearing data before import...")
            cursor.execute("DELETE FROM charges")
            conn.commit()
        
        # Import data using pandas for better handling of data types
        df = pd.read_csv(csv_path)
        
        # Convert empty strings to None/NULL for numeric columns
        numeric_cols = ['CHARGES_ENTITY_NUM', 'CHARGES_CHG_AMT_CHOICE', 'CHARGES_CHGS_PERCENTAGE', 
                        'CHARGES_FIXED_AMT', 'CHARGES_VAR_NOOF_SLABS', 'CHARGES_OVERALL_MIN_AMT',
                        'CHARGES_OVERALL_MAX_AMT', 'CHARGES_RNDOFF_FACTOR']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Write to database
        df.to_sql('charges', conn, if_exists='append', index=False)
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_charges_prod_code ON charges(CHARGES_PROD_CODE)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_charges_chg_code ON charges(CHARGES_CHG_CODE)')
        
        conn.commit()
        
        # Get final record count
        cursor.execute("SELECT COUNT(*) FROM charges")
        final_count = cursor.fetchone()[0]
        print(f"Successfully imported {final_count} charges records into the database.")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error importing charges data: {str(e)}")
        if 'conn' in locals() and conn:
            conn.close()
        return False

if __name__ == "__main__":
    import_charges() 