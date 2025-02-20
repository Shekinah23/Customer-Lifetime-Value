import sqlite3
import os

def check_transactions():
    try:
        db_path = 'banking_data.db'
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if transactions table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='transactions'")
        if not cursor.fetchone():
            print("Transactions table does not exist!")
            return
            
        # Get transaction counts and date range
        cursor.execute("""
            SELECT 
                COUNT(*) as count,
                MIN(ACNTS_LAST_TRAN_DATE) as min_date,
                MAX(ACNTS_LAST_TRAN_DATE) as max_date
            FROM transactions
        """)
        result = cursor.fetchone()
        print(f"Total transactions: {result[0]}")
        print(f"Date range: {result[1]} to {result[2]}")
        
        # Check for client 855555 in transactions
        cursor.execute("""
            SELECT 
                ACNTS_CLIENT_NUM,
                ACNTS_CURR_CODE,
                ACNTS_LAST_TRAN_DATE
            FROM transactions 
            WHERE ACNTS_CLIENT_NUM = '855555'
            LIMIT 1
        """)
        trans_row = cursor.fetchone()
        print("\nTransaction record for 855555:")
        if trans_row:
            print(f"Client num: {trans_row[0]}")
            print(f"Currency: {trans_row[1]}")
            print(f"Last tran date: {trans_row[2]}")
        else:
            print("Not found in transactions")
            
        # Check for client 855555 in clients
        cursor.execute("""
            SELECT 
                ACNTS_CLIENT_NUM,
                ACNTS_PROD_CODE,
                ACNTS_AC_TYPE
            FROM clients 
            WHERE ACNTS_CLIENT_NUM = 855555
            LIMIT 1
        """)
        client_row = cursor.fetchone()
        print("\nClient record for 855555:")
        if client_row:
            print(f"Client num: {client_row[0]}")
            print(f"Product code: {client_row[1]}")
            print(f"Account type: {client_row[2]}")
        else:
            print("Not found in clients")
            
        conn.close()
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    check_transactions()
