import sqlite3
import csv
import os

def import_data():
    # Connect to database
    db_path = os.path.join(os.path.dirname(__file__), 'banking_data.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables using schema
    with open('schema.sql', 'r') as f:
        schema = f.read()
        conn.executescript(schema)

    # Import products data
    products_file = os.path.join(os.path.dirname(__file__), 'data_processor', 'datasets', 'products.csv')
    with open(products_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert empty strings to None for REAL/INTEGER fields
            for key in row:
                if row[key] == '':
                    row[key] = None
                elif key == 'PRODUCT_CODE':
                    row[key] = int(row[key])
                elif key in ['PRODUCT_FOR_DEPOSITS', 'PRODUCT_FOR_LOANS', 'PRODUCT_FOR_RUN_ACS',
                           'PRODUCT_OD_FACILITY', 'PRODUCT_REVOLVING_FACILITY', 'PRODUCT_FOR_CALL_DEP',
                           'PRODUCT_CONTRACT_ALLOWED', 'PRODUCT_EXEMPT_FROM_NPA', 'PRODUCT_REQ_APPLN_ENTRY',
                           'PRODUCT_FOR_RFC', 'PRODUCT_FOR_FCLS', 'PRODUCT_FOR_FCNR', 'PRODUCT_FOR_EEFC',
                           'PRODUCT_FOR_TRADE_FINANCE', 'PRODUCT_FOR_LOCKERS', 'PRODUCT_INDIRECT_EXP_REQD']:
                    row[key] = int(row[key])

            # Insert row into products table
            cursor.execute('''
                INSERT INTO products (
                    PRODUCT_CODE, PRODUCT_NAME, PRODUCT_CONC_NAME, PRODUCT_ALPHA_ID,
                    PRODUCT_GROUP_CODE, PRODUCT_CLASS, PRODUCT_FOR_DEPOSITS, PRODUCT_FOR_LOANS,
                    PRODUCT_FOR_RUN_ACS, PRODUCT_OD_FACILITY, PRODUCT_REVOLVING_FACILITY,
                    PRODUCT_FOR_CALL_DEP, PRODUCT_CONTRACT_ALLOWED, PRODUCT_CONTRACT_NUM_GEN,
                    PRODUCT_EXEMPT_FROM_NPA, PRODUCT_REQ_APPLN_ENTRY, PRODUCT_FOR_RFC,
                    PRODUCT_FOR_FCLS, PRODUCT_FOR_FCNR, PRODUCT_FOR_EEFC, PRODUCT_FOR_TRADE_FINANCE,
                    PRODUCT_FOR_LOCKERS, PRODUCT_INDIRECT_EXP_REQD, PRODUCT_BUSDIVN_CODE,
                    PRODUCT_GLACC_CODE, PRODUCT_REVOKED_ON, PRODUCT_ENTD_BY, PRODUCT_ENTD_ON,
                    PRODUCT_LAST_MOD_BY, PRODUCT_LAST_MOD_ON, PRODUCT_AUTH_BY, PRODUCT_AUTH_ON,
                    TBA_MAIN_KEY
                ) VALUES (
                    :PRODUCT_CODE, :PRODUCT_NAME, :PRODUCT_CONC_NAME, :PRODUCT_ALPHA_ID,
                    :PRODUCT_GROUP_CODE, :PRODUCT_CLASS, :PRODUCT_FOR_DEPOSITS, :PRODUCT_FOR_LOANS,
                    :PRODUCT_FOR_RUN_ACS, :PRODUCT_OD_FACILITY, :PRODUCT_REVOLVING_FACILITY,
                    :PRODUCT_FOR_CALL_DEP, :PRODUCT_CONTRACT_ALLOWED, :PRODUCT_CONTRACT_NUM_GEN,
                    :PRODUCT_EXEMPT_FROM_NPA, :PRODUCT_REQ_APPLN_ENTRY, :PRODUCT_FOR_RFC,
                    :PRODUCT_FOR_FCLS, :PRODUCT_FOR_FCNR, :PRODUCT_FOR_EEFC, :PRODUCT_FOR_TRADE_FINANCE,
                    :PRODUCT_FOR_LOCKERS, :PRODUCT_INDIRECT_EXP_REQD, :PRODUCT_BUSDIVN_CODE,
                    :PRODUCT_GLACC_CODE, :PRODUCT_REVOKED_ON, :PRODUCT_ENTD_BY, :PRODUCT_ENTD_ON,
                    :PRODUCT_LAST_MOD_BY, :PRODUCT_LAST_MOD_ON, :PRODUCT_AUTH_BY, :PRODUCT_AUTH_ON,
                    :TBA_MAIN_KEY
                )
            ''', row)
    print("Products data imported successfully")

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
    print("Charges data imported successfully")

    conn.commit()
    conn.close()

if __name__ == '__main__':
    import_data()
