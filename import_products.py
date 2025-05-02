import sqlite3
import csv
import os

def import_data():
    # Connect to database
    db_path = 'banking_data.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables using schema
    with open('schema.sql', 'r') as f:
        schema = f.read()
        conn.executescript(schema)

    # Import products data
    products_file = os.path.join('data_processor', 'datasets', 'products.csv')
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
                           'PRODUCT_FOR_TRADE_FINANCE', 'PRODUCT_FOR_LOCKERS', 'PRODUCT_INDIRECT_EXP_REQD',
                           'PRODUCT_FOR_FIXED_ASSETS', 'PRODUCT_FOR_SAFE_CUS']:
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
                    TBA_MAIN_KEY, PRODUCT_FOR_FIXED_ASSETS, PRODUCT_FOR_SAFE_CUS
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
                    :TBA_MAIN_KEY, :PRODUCT_FOR_FIXED_ASSETS, :PRODUCT_FOR_SAFE_CUS
                )
            ''', row)
    print("Products data imported successfully")

    # Import charges data
    charges_file = os.path.join('data_processor', 'datasets', 'charges.csv')
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

    # Import clients data
    clients_file = os.path.join('data_processor', 'datasets', 'CLIENTS.csv')
    print(f"\nImporting clients from: {os.path.abspath(clients_file)}")
    with open(clients_file, 'r') as f:
        reader = csv.DictReader(f)
        first_row = next(reader, None)
        if first_row:
            print("\nFirst client row:")
            for key, value in first_row.items():
                print(f"{key}: {value}")
            # Reset file pointer and reader
            f.seek(0)
            reader = csv.DictReader(f)
        for row in reader:
            # Convert empty strings to None for REAL/INTEGER fields
            for key in row:
                if row[key] == '':
                    row[key] = None
                elif key in ['ACNTS_ENTITY_NUM', 'ACNTS_INTERNAL_ACNUM', 'ACNTS_BRN_CODE', 'ACNTS_CLIENT_NUM',
                           'ACNTS_AC_SEQ_NUM', 'ACNTS_PROD_CODE', 'ACNTS_AC_TYPE', 'ACNTS_AC_SUB_TYPE',
                           'ACNTS_GLACC_CODE', 'ACNTS_SALARY_ACNT', 'ACNTS_PASSBK_REQD', 'ACNTS_MODE_OF_OPERN',
                           'ACNTS_REPAYABLE_TO', 'ACNTS_SPECIAL_ACNT', 'ACNTS_NOMINATION_REQD', 'ACNTS_CREDIT_INT_REQD',
                           'ACNTS_MINOR_ACNT', 'ACNTS_POWER_OF_ATTORNEY', 'ACNTS_CONNP_INV_NUM', 'ACNTS_TELLER_OPERN',
                           'ACNTS_ATM_OPERN', 'ACNTS_CALL_CENTER_OPERN', 'ACNTS_INET_OPERN', 'ACNTS_CR_CARDS_ALLOWED',
                           'ACNTS_KIOSK_BANKING', 'ACNTS_SMS_OPERN', 'ACNTS_OD_ALLOWED', 'ACNTS_CHQBK_REQD',
                           'ACNTS_INOP_ACNT', 'ACNTS_DORMANT_ACNT', 'ACNTS_DB_FREEZED', 'ACNTS_CR_FREEZED',
                           'ACNTS_ESCHEAT_ACNT']:
                    row[key] = int(row[key]) if row[key] is not None else None
                elif key in ['ACNTS_LOCN_CODE', 'ACNTS_MKT_BY_BRN', 'ACNTS_DSA_CODE', 'ACNTS_NUM_SIG_COMB',
                           'ACNTS_BUSDIVN_CODE', 'ACNTS_MMB_INT_ACCR_UPTO', 'ACNTS_TRF_TO_OVERDUE',
                           'ACNTS_ACST_UPTO_DATE', 'ACNTS_LAST_STMT_NUM', 'ACNTS_LAST_CHQBK_ISSUED',
                           'ACNTS_PREOPEN_ENTD_BY', 'ACNTS_PREOPEN_ENTD_ON', 'ACNTS_PREOPEN_LAST_MOD_BY',
                           'ACNTS_PREOPEN_LAST_MOD_ON', 'TBA_MAIN_KEY', 'ACNTS_TENOR', 'ACNTS_MATURITY_DATE',
                           'ACNTS_SALARY_PAY_AC', 'ACNTS_APPL_NUM', 'ACNTS_SLIPS_DIRECT_DB', 'ACNTS_UDCH_CODE',
                           'ACNTS_UDCH_SCHEME']:
                    row[key] = float(row[key]) if row[key] is not None else None

            # Insert row into clients table
            cursor.execute('''
                INSERT INTO clients (
                    ACNTS_ENTITY_NUM, ACNTS_INTERNAL_ACNUM, ACNTS_BRN_CODE, ACNTS_CLIENT_NUM,
                    ACNTS_AC_SEQ_NUM, ACNTS_ACCOUNT_NUMBER, ACNTS_PROD_CODE, ACNTS_AC_TYPE,
                    ACNTS_AC_SUB_TYPE, ACNTS_SCHEME_CODE, ACNTS_OPENING_DATE, ACNTS_AC_NAME1,
                    ACNTS_AC_NAME2, ACNTS_SHORT_NAME, ACNTS_AC_ADDR1, ACNTS_AC_ADDR2,
                    ACNTS_AC_ADDR3, ACNTS_AC_ADDR4, ACNTS_AC_ADDR5, ACNTS_LOCN_CODE,
                    ACNTS_CURR_CODE, ACNTS_GLACC_CODE, ACNTS_SALARY_ACNT, ACNTS_PASSBK_REQD,
                    ACNTS_DCHANNEL_CODE, ACNTS_MKT_CHANNEL_CODE, ACNTS_MKT_BY_STAFF,
                    ACNTS_MKT_BY_BRN, ACNTS_DSA_CODE, ACNTS_MODE_OF_OPERN, ACNTS_MOPR_ADDN_INFO,
                    ACNTS_REPAYABLE_TO, ACNTS_SPECIAL_ACNT, ACNTS_NOMINATION_REQD,
                    ACNTS_CREDIT_INT_REQD, ACNTS_MINOR_ACNT, ACNTS_POWER_OF_ATTORNEY,
                    ACNTS_CONNP_INV_NUM, ACNTS_NUM_SIG_COMB, ACNTS_TELLER_OPERN,
                    ACNTS_ATM_OPERN, ACNTS_CALL_CENTER_OPERN, ACNTS_INET_OPERN,
                    ACNTS_CR_CARDS_ALLOWED, ACNTS_KIOSK_BANKING, ACNTS_SMS_OPERN,
                    ACNTS_OD_ALLOWED, ACNTS_CHQBK_REQD, ACNTS_ARM_CRM, ACNTS_ARM_ROLE,
                    ACNTS_BUSDIVN_CODE, ACNTS_CREATION_STATUS, ACNTS_BASE_DATE,
                    ACNTS_INOP_ACNT, ACNTS_DORMANT_ACNT, ACNTS_LAST_TRAN_DATE,
                    ACNTS_NONSYS_LAST_DATE, ACNTS_INT_CALC_UPTO, ACNTS_MMB_INT_ACCR_UPTO,
                    ACNTS_INT_ACCR_UPTO, ACNTS_INT_DBCR_UPTO, ACNTS_TRF_TO_OVERDUE,
                    ACNTS_DB_FREEZED, ACNTS_CR_FREEZED, ACNTS_CONTRACT_BASED_FLG,
                    ACNTS_ACST_UPTO_DATE, ACNTS_LAST_STMT_NUM, ACNTS_LAST_CHQBK_ISSUED,
                    ACNTS_CLOSURE_DATE, ACNTS_PREOPEN_ENTD_BY, ACNTS_PREOPEN_ENTD_ON,
                    ACNTS_PREOPEN_LAST_MOD_BY, ACNTS_PREOPEN_LAST_MOD_ON, ACNTS_ENTD_BY,
                    ACNTS_ENTD_ON, ACNTS_LAST_MOD_BY, ACNTS_LAST_MOD_ON, ACNTS_AUTH_BY,
                    ACNTS_AUTH_ON, TBA_MAIN_KEY, ACNTS_TENOR, ACNTS_TENOR_DMY,
                    ACNTS_MATURITY_DATE, ACNTS_SALARY_PAY_AC, ACNTS_APPL_NUM,
                    ACNTS_SLIPS_DIRECT_DB, ACNTS_UDCH_CODE, ACNTS_UDCH_SCHEME,
                    ACNTS_ESCHEAT_ACNT, ACNTS_LAST_DB_INT_RECOVRY_DATE, ACNTS_MBLBNK_OPERN
                ) VALUES (
                    :ACNTS_ENTITY_NUM, :ACNTS_INTERNAL_ACNUM, :ACNTS_BRN_CODE, :ACNTS_CLIENT_NUM,
                    :ACNTS_AC_SEQ_NUM, :ACNTS_ACCOUNT_NUMBER, :ACNTS_PROD_CODE, :ACNTS_AC_TYPE,
                    :ACNTS_AC_SUB_TYPE, :ACNTS_SCHEME_CODE, :ACNTS_OPENING_DATE, :ACNTS_AC_NAME1,
                    :ACNTS_AC_NAME2, :ACNTS_SHORT_NAME, :ACNTS_AC_ADDR1, :ACNTS_AC_ADDR2,
                    :ACNTS_AC_ADDR3, :ACNTS_AC_ADDR4, :ACNTS_AC_ADDR5, :ACNTS_LOCN_CODE,
                    :ACNTS_CURR_CODE, :ACNTS_GLACC_CODE, :ACNTS_SALARY_ACNT, :ACNTS_PASSBK_REQD,
                    :ACNTS_DCHANNEL_CODE, :ACNTS_MKT_CHANNEL_CODE, :ACNTS_MKT_BY_STAFF,
                    :ACNTS_MKT_BY_BRN, :ACNTS_DSA_CODE, :ACNTS_MODE_OF_OPERN, :ACNTS_MOPR_ADDN_INFO,
                    :ACNTS_REPAYABLE_TO, :ACNTS_SPECIAL_ACNT, :ACNTS_NOMINATION_REQD,
                    :ACNTS_CREDIT_INT_REQD, :ACNTS_MINOR_ACNT, :ACNTS_POWER_OF_ATTORNEY,
                    :ACNTS_CONNP_INV_NUM, :ACNTS_NUM_SIG_COMB, :ACNTS_TELLER_OPERN,
                    :ACNTS_ATM_OPERN, :ACNTS_CALL_CENTER_OPERN, :ACNTS_INET_OPERN,
                    :ACNTS_CR_CARDS_ALLOWED, :ACNTS_KIOSK_BANKING, :ACNTS_SMS_OPERN,
                    :ACNTS_OD_ALLOWED, :ACNTS_CHQBK_REQD, :ACNTS_ARM_CRM, :ACNTS_ARM_ROLE,
                    :ACNTS_BUSDIVN_CODE, :ACNTS_CREATION_STATUS, :ACNTS_BASE_DATE,
                    :ACNTS_INOP_ACNT, :ACNTS_DORMANT_ACNT, :ACNTS_LAST_TRAN_DATE,
                    :ACNTS_NONSYS_LAST_DATE, :ACNTS_INT_CALC_UPTO, :ACNTS_MMB_INT_ACCR_UPTO,
                    :ACNTS_INT_ACCR_UPTO, :ACNTS_INT_DBCR_UPTO, :ACNTS_TRF_TO_OVERDUE,
                    :ACNTS_DB_FREEZED, :ACNTS_CR_FREEZED, :ACNTS_CONTRACT_BASED_FLG,
                    :ACNTS_ACST_UPTO_DATE, :ACNTS_LAST_STMT_NUM, :ACNTS_LAST_CHQBK_ISSUED,
                    :ACNTS_CLOSURE_DATE, :ACNTS_PREOPEN_ENTD_BY, :ACNTS_PREOPEN_ENTD_ON,
                    :ACNTS_PREOPEN_LAST_MOD_BY, :ACNTS_PREOPEN_LAST_MOD_ON, :ACNTS_ENTD_BY,
                    :ACNTS_ENTD_ON, :ACNTS_LAST_MOD_BY, :ACNTS_LAST_MOD_ON, :ACNTS_AUTH_BY,
                    :ACNTS_AUTH_ON, :TBA_MAIN_KEY, :ACNTS_TENOR, :ACNTS_TENOR_DMY,
                    :ACNTS_MATURITY_DATE, :ACNTS_SALARY_PAY_AC, :ACNTS_APPL_NUM,
                    :ACNTS_SLIPS_DIRECT_DB, :ACNTS_UDCH_CODE, :ACNTS_UDCH_SCHEME,
                    :ACNTS_ESCHEAT_ACNT, :ACNTS_LAST_DB_INT_RECOVRY_DATE, :ACNTS_MBLBNK_OPERN
                )
            ''', row)
    print("Clients data imported successfully")

    conn.commit()
    conn.close()

if __name__ == '__main__':
    import_data()
