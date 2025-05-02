import sqlite3

def check_loan_tables_structure():
    conn = sqlite3.connect('banking_data.db')
    cursor = conn.cursor()
    
    # Check loan_info table structure
    cursor.execute("PRAGMA table_info(loan_info)")
    columns = cursor.fetchall()
    
    print("\nLoan Info Table Structure:")
    print("-" * 50)
    for col in columns:
        print(f"Column: {col[1]}, Type: {col[2]}")
    
    # Check loan_payments table structure
    cursor.execute("PRAGMA table_info(loan_payments)")
    columns = cursor.fetchall()
    
    print("\nLoan Payments Table Structure:")
    print("-" * 50)
    for col in columns:
        print(f"Column: {col[1]}, Type: {col[2]}")
    
    # Get sample data from loan_payments
    cursor.execute("SELECT * FROM loan_payments LIMIT 5")
    rows = cursor.fetchall()
    
    print("\nSample Loan Payments Data:")
    print("-" * 50)
    for row in rows:
        print(f"Project ID: {row[0]}, Loan Type: {row[1]}, Origination Date: {row[2]}, Payment Count: {row[3]}")
    
    # Count records in loan_payments
    cursor.execute("SELECT COUNT(*) FROM loan_payments")
    count = cursor.fetchone()[0]
    print(f"\nTotal records in loan_payments: {count}")
    
    # Check for zero payment counts
    cursor.execute("SELECT COUNT(*) FROM loan_payments WHERE payment_count = 0")
    zero_count = cursor.fetchone()[0]
    print(f"Records with zero payment count: {zero_count}")
    
    # Check non-zero payment counts
    cursor.execute("SELECT proj_id, loan_type, origination_date, payment_count FROM loan_payments WHERE payment_count > 0 ORDER BY payment_count DESC LIMIT 10")
    rows = cursor.fetchall()
    
    print("\nTop 10 Non-Zero Payment Counts:")
    print("-" * 50)
    for row in rows:
        print(f"Project ID: {row[0]}, Loan Type: {row[1]}, Origination Date: {row[2]}, Payment Count: {row[3]}")
    
    # Check payment count distribution
    cursor.execute("SELECT payment_count, COUNT(*) FROM loan_payments GROUP BY payment_count ORDER BY payment_count")
    rows = cursor.fetchall()
    
    print("\nPayment Count Distribution:")
    print("-" * 50)
    for row in rows:
        print(f"Payment Count: {row[0]}, Number of Records: {row[1]}")
    
    conn.close()

if __name__ == "__main__":
    check_loan_tables_structure()
