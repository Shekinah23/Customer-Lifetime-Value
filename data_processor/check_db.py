import sqlite3

def check_database():
    try:
        conn = sqlite3.connect('banking_data.db')
        cursor = conn.cursor()
        
        # Get raw schema information
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print("\nDatabase Tables and Schema:")
        for table in tables:
            table_name = table[0]
            print(f"\n{table_name} table:")
            
            # Get record count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"\nTotal records: {count:,}")
            
            # Get column info
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            print("\nColumns:")
            for col in columns:
                print(f"  Column {col[0]}: {col[1]} ({col[2]}) {'PRIMARY KEY' if col[5] else ''} {'NOT NULL' if col[3] else ''}")
            
            # Get sample data
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
            rows = cursor.fetchall()
            if rows:
                print("\nSample data (first 3 rows):")
                for row in rows:
                    print(row)
            
            # Get some statistics for the clients table
            if table_name == 'clients':
                print("\nClient Statistics:")
                # Count distinct client numbers
                cursor.execute("SELECT COUNT(DISTINCT ACNTS_CLIENT_NUM) FROM clients")
                distinct_clients = cursor.fetchone()[0]
                print(f"Distinct clients: {distinct_clients:,}")
                
                # Count non-empty addresses
                cursor.execute("SELECT COUNT(*) FROM clients WHERE ACNTS_AC_ADDR1 IS NOT NULL AND ACNTS_AC_ADDR1 != ''")
                with_address = cursor.fetchone()[0]
                print(f"Clients with addresses: {with_address:,}")
                
                # Get client name sample
                cursor.execute("SELECT ACNTS_AC_NAME1, ACNTS_CLIENT_NUM FROM clients WHERE ACNTS_AC_NAME1 IS NOT NULL AND ACNTS_AC_NAME1 != '' LIMIT 3")
                name_samples = cursor.fetchall()
                print("\nSample client names:")
                for name, num in name_samples:
                    print(f"  Client {num}: {name}")
            
            print("-" * 80)
            
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    check_database()
