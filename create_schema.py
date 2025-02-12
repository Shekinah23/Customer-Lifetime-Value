import sqlite3
import os

def create_schema():
    # Read the SQL schema
    with open('schema.sql', 'r') as f:
        schema_sql = f.read()
    
    # Connect to the database
    conn = sqlite3.connect('banking_data.db')
    cursor = conn.cursor()
    
    try:
        # Execute the schema SQL
        cursor.executescript(schema_sql)
        conn.commit()
        print("Schema created successfully")
    except Exception as e:
        print(f"Error creating schema: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    create_schema()
