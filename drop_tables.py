import sqlite3

def drop_tables():
    conn = sqlite3.connect('banking_data.db')
    cursor = conn.cursor()
    
    # Drop tables in correct order due to foreign key constraints
    cursor.execute("DROP TABLE IF EXISTS loan_payments")
    cursor.execute("DROP TABLE IF EXISTS loan_balance")
    cursor.execute("DROP TABLE IF EXISTS loan_info")
    
    conn.commit()
    conn.close()
    print("Tables dropped successfully")

if __name__ == "__main__":
    drop_tables()
