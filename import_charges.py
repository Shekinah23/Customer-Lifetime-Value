import sqlite3
import csv
import os

def import_charges():
    # Connect to database
    db_path = os.path.join(os.path.dirname(__file__), 'banking_data.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Import charges data
    charges_file = os.path.join(os.path.dirname(__file__), 'data_processor', 'datasets', 'charges.csv')
    with open(charges_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert empty strings to None for REAL/INTEGER fields
            for key in row:
                if row[key] == '':
                    row[key] = None
                elif key in ['CHARGES_ENTITY_NUM', 'CHARGES_PROD_CODE', 'CHARGES_ACSUB_TYPE', 'CHARGES_CHG_AMT_CHOICE']:
                    row[key] = int(row[key]) if row[key] is not None else None
                elif key in ['CHARGES_CHGS_PERCENTAGE', 'CHARGES_FIXED_AMT', 'CHARGES_VAR_NOOF_SLABS', 
                           'CHARGES_OVERALL_MIN_AMT', 'CHARGES_OVERALL_MAX_AMT']:
                    row[key] = float(row[key]) if row[key] is not None else None

            # Insert row into charges table
            cursor.execute('''
                INSERT INTO charges (
                    CHARGES_ENTITY_NUM, CHARGES_CHG_CODE, CHARGES_PROD_CODE, CHARGES_AC_TYPE,
                    CHARGES_ACSUB_TYPE, CHARGES_SCHEME_CODE, CHARGES_CHG_TYPE, CHARGES_CHG_CURR,
                    CHARGES_LATEST_EFF_DATE, CHARGES_CHG_AMT_CHOICE, CHARGES_COMBINATION_CHOICE,
                    CHARGES_CHGS_PERCENTAGE, CHARGES_FIXED_AMT, CHARGES_VAR_NOOF_SLABS,
                    CHARGES_SLAB_AMT_CHOICE, CHARGES_CHG_SLAB_CHOICE, CHARGES_OVERALL_MIN_AMT,
                    CHARGES_OVERALL_MAX_AMT
                ) VALUES (
                    :CHARGES_ENTITY_NUM, :CHARGES_CHG_CODE, :CHARGES_PROD_CODE, :CHARGES_AC_TYPE,
                    :CHARGES_ACSUB_TYPE, :CHARGES_SCHEME_CODE, :CHARGES_CHG_TYPE, :CHARGES_CHG_CURR,
                    :CHARGES_LATEST_EFF_DATE, :CHARGES_CHG_AMT_CHOICE, :CHARGES_COMBINATION_CHOICE,
                    :CHARGES_CHGS_PERCENTAGE, :CHARGES_FIXED_AMT, :CHARGES_VAR_NOOF_SLABS,
                    :CHARGES_SLAB_AMT_CHOICE, :CHARGES_CHG_SLAB_CHOICE, :CHARGES_OVERALL_MIN_AMT,
                    :CHARGES_OVERALL_MAX_AMT
                )
            ''', row)
    
    conn.commit()
    conn.close()
    print("Charges data imported successfully")

if __name__ == '__main__':
    import_charges()
