import sqlite3
import os

def check_database():
    try:
        current_dir = os.getcwd()
        db_path = os.path.join(current_dir, 'banking_data.db')
        print(f"\nChecking database at: {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print("\nAvailable tables:", [table[0] for table in tables])
        
        # Check products
        try:
            cursor.execute("""
                SELECT PRODUCT_CODE, PRODUCT_NAME, PRODUCT_GROUP_CODE, PRODUCT_CLASS
                FROM products
                LIMIT 5
            """)
            products = cursor.fetchall()
            print("\nSample products:")
            for prod in products:
                print(f"Code: {prod[0]}, Name: {prod[1]}, Group: {prod[2]}, Class: {prod[3]}")
        except Exception as e:
            print(f"\nError fetching products: {str(e)}")
            
        # Check clients table structure
        try:
            cursor.execute("PRAGMA table_info(clients)")
            columns = cursor.fetchall()
            print("\nClients table columns:")
            for col in columns:
                print(f"Column: {col[1]}, Type: {col[2]}")
        except Exception as e:
            print(f"\nError getting clients table structure: {str(e)}")
        
        # Check all data in clients table
        try:
            cursor.execute("SELECT COUNT(*) FROM clients")
            count = cursor.fetchone()[0]
            print(f"\nTotal client records: {count}")
            
            if count > 0:
                cursor.execute("""
                    SELECT ACNTS_CLIENT_NUM, ACNTS_ACCOUNT_NUMBER, ACNTS_AC_NAME1, ACNTS_PROD_CODE
                    FROM clients 
                    LIMIT 5
                """)
                clients = cursor.fetchall()
                print("\nFirst 5 client records:")
                for client in clients:
                    print(f"\nClient: {client[0]}")
                    print(f"Account: {client[1]}")
                    print(f"Name: {client[2]}")
                    print(f"Product Code: {client[3]}")
            else:
                print("\nNo client records found")
                
                # Check if table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='clients'")
                if cursor.fetchone():
                    print("Clients table exists but is empty")
                else:
                    print("Clients table does not exist")
                    
        except Exception as e:
            print(f"\nError checking clients data: {str(e)}")
            
    except Exception as e:
        print(f"\nMain error: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_database()
