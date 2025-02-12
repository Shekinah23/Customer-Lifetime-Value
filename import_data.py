import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

def import_data():
    """Import data from CSV files into SQLite database"""
    print("Starting import process...")
    
    # Connect to database
    conn = sqlite3.connect('banking_data.db')
    cursor = conn.cursor()
    
    try:
        # Import corporate clients
        print("\nImporting corporate clients...")
        corp_df = pd.read_csv('data_processor/datasets/corpclients.csv', 
                            encoding='utf-8', 
                            low_memory=False,
                            on_bad_lines='skip')
        
        # Generate synthetic clients
        print("\nGenerating synthetic clients...")
        num_clients = 1000  # Generate 1000 synthetic clients
        
        clients = []
        for i in range(num_clients):
            clients.append({
                'ACNTS_ENTITY_NUM': np.random.randint(1000, 9999),
                'ACNTS_INTERNAL_ACNUM': np.random.randint(10000, 99999),
                'ACNTS_BRN_CODE': np.random.randint(1, 100),
                'ACNTS_CLIENT_NUM': i + 1,  # Unique client number
                'ACNTS_AC_SEQ_NUM': np.random.randint(1, 100),
                'ACNTS_ACCOUNT_NUMBER': f"ACC{np.random.randint(10000, 99999)}",
                'ACNTS_PROD_CODE': np.random.randint(1, 4),  # 1: Premium, 2: Standard, 3: Basic
                'ACNTS_AC_TYPE': np.random.randint(1, 4),
                'ACNTS_AC_SUB_TYPE': np.random.randint(1, 4),
                'ACNTS_SCHEME_CODE': 'SCH001',
                'ACNTS_OPENING_DATE': '2024-01-01',
                'ACNTS_AC_NAME1': f"Client {i+1}",
                'ACNTS_AC_ADDR1': f"Address {i+1}",
                'ACNTS_CURR_CODE': 'USD',
                'ACNTS_GLACC_CODE': np.random.randint(1000, 9999),
                'ACNTS_SALARY_ACNT': np.random.choice([0, 1], p=[0.7, 0.3]),
                'ACNTS_PASSBK_REQD': np.random.choice([0, 1]),
                'ACNTS_MODE_OF_OPERN': np.random.randint(1, 4),
                'ACNTS_REPAYABLE_TO': 0,
                'ACNTS_SPECIAL_ACNT': 0,
                'ACNTS_NOMINATION_REQD': 0,
                'ACNTS_CREDIT_INT_REQD': 1,
                'ACNTS_MINOR_ACNT': 0,
                'ACNTS_POWER_OF_ATTORNEY': 0,
                'ACNTS_CONNP_INV_NUM': 0,
                'ACNTS_TELLER_OPERN': 1,
                'ACNTS_ATM_OPERN': np.random.randint(0, 20),
                'ACNTS_CALL_CENTER_OPERN': np.random.choice([0, 1]),
                'ACNTS_INET_OPERN': np.random.randint(0, 30),
                'ACNTS_CR_CARDS_ALLOWED': np.random.choice([0, 1], p=[0.8, 0.2]),
                'ACNTS_KIOSK_BANKING': np.random.choice([0, 1]),
                'ACNTS_SMS_OPERN': np.random.randint(0, 25),
                'ACNTS_OD_ALLOWED': np.random.choice([0, 1]),
                'ACNTS_CHQBK_REQD': np.random.choice([0, 1]),
                'ACNTS_CREATION_STATUS': 'A',
                'ACNTS_BASE_DATE': '2024-01-01',
                'ACNTS_INOP_ACNT': 0,
                'ACNTS_DORMANT_ACNT': np.random.choice([0, 1], p=[0.95, 0.05]),
                'ACNTS_LAST_TRAN_DATE': (datetime.now() - pd.Timedelta(days=np.random.randint(1, 180))).strftime('%Y-%m-%d'),
                'ACNTS_DB_FREEZED': 0,
                'ACNTS_CR_FREEZED': 0,
                'ACNTS_CONTRACT_BASED_FLG': 'N',
                'ACNTS_ENTD_BY': 'SYSTEM',
                'ACNTS_ENTD_ON': '2024-01-01',
                'ACNTS_AUTH_BY': 'SYSTEM',
                'ACNTS_AUTH_ON': '2024-01-01',
                'ACNTS_ESCHEAT_ACNT': 0
            })
        
        # Convert to DataFrame and import
        clients_df = pd.DataFrame(clients)
        clients_df.to_sql('clients', conn, if_exists='replace', index=False)
        print(f"Imported {len(clients_df)} client records")
        
        # Generate synthetic transactions
        print("\nGenerating synthetic transactions...")
        transactions = []
        for client_num in clients_df['ACNTS_CLIENT_NUM']:
            # Generate 10-50 transactions per client
            num_transactions = np.random.randint(10, 50)
            for _ in range(num_transactions):
                transactions.append({
                    'ACNTS_BRN_CODE': np.random.randint(1, 100),
                    'ACCOUNT_NUMBER': f"ACC{np.random.randint(10000, 99999)}",
                    'ACNTS_CURR_CODE': 'USD',
                    'ACNTS_CLIENT_NUM': client_num,
                    'ACCOUNT_NAME': 'Transaction Account',
                    'ACNTS_LAST_TRAN_DATE': (datetime.now() - pd.Timedelta(days=np.random.randint(1, 365))).strftime('%Y-%m-%d'),
                    'INDCLIENT_BIRTH_DATE': None,
                    'AGE': None,
                    'ACCT_AGE': np.random.randint(1, 1000),
                    'CLCONTACT_RES_PHONE': None,
                    'INDCLIENT_TEL_RES': None
                })
        
        # Convert transactions to DataFrame and import
        trans_df = pd.DataFrame(transactions)
        trans_df.to_sql('transactions', conn, if_exists='replace', index=False)
        print(f"Generated {len(trans_df)} synthetic transactions")
        
        conn.commit()
        print("\nImport completed successfully")
        
    except Exception as e:
        print(f"Error during import: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    import_data()
