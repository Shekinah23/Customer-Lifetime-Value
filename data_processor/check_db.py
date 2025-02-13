import sqlite3
import os

def check_database():
    try:
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'banking_data.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print("\nAvailable tables:", [table[0] for table in tables])
        
        # Check products
        cursor.execute("""
            SELECT PRODUCT_CODE, PRODUCT_NAME, PRODUCT_GROUP_CODE, PRODUCT_CLASS
            FROM products
            LIMIT 5
        """)
        products = cursor.fetchall()
        print("\nSample products:")
        for prod in products:
            print(f"Code: {prod[0]}, Name: {prod[1]}, Group: {prod[2]}, Class: {prod[3]}")
            
        # Check clients table structure
        cursor.execute("PRAGMA table_info(clients)")
        columns = cursor.fetchall()
        print("\nClients table columns:")
        for col in columns:
            print(f"Column: {col[1]}, Type: {col[2]}")
        
        # Check all data in clients table
        cursor.execute("""
            SELECT * FROM clients LIMIT 10
        """)
        clients = cursor.fetchall()
        if clients:
            print("\nFirst 10 client records:")
            for client in clients:
                print("\nClient record:")
                for i, col in enumerate(cursor.description):
                    print(f"{col[0]}: {client[i]}")
        else:
            print("\nNo records found in clients table")
            
        # Check if any data exists
        cursor.execute("SELECT COUNT(*) FROM clients")
        count = cursor.fetchone()[0]
        print(f"\nTotal client records: {count}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_database()
