import sqlite3
import os
from datetime import datetime

def insert_sample_issue():
    try:
        # Get the absolute path to the database
        db_path = os.path.join(os.path.dirname(__file__), 'banking_data.db')
        
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Insert a sample data quality issue
        cursor.execute("""
            INSERT INTO data_quality_issues 
            (client_id, issue_type, issue_details, detected_at, status) 
            VALUES (?, ?, ?, ?, ?)
        """, (
            1,  # client_id
            "missing_data",  # issue_type
            "Missing transaction date",  # issue_details
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # detected_at
            "pending"  # status
        ))
        
        conn.commit()
        conn.close()
        print("Successfully inserted sample data quality issue")
        
    except Exception as e:
        print(f"Error inserting sample issue: {str(e)}")

if __name__ == "__main__":
    insert_sample_issue()
