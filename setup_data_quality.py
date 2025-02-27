import sqlite3
import os

def setup_data_quality():
    try:
        # Get the absolute path to the database
        db_path = os.path.join(os.path.dirname(__file__), 'banking_data.db')
        schema_path = os.path.join(os.path.dirname(__file__), 'data_processor', 'schema.sql')
        
        # Read the schema file
        with open(schema_path, 'r') as f:
            schema = f.read()
            
        # Connect to the database and execute schema
        conn = sqlite3.connect(db_path)
        conn.executescript(schema)
        conn.commit()
        conn.close()
        
        print("Successfully set up data quality tables")
        
    except Exception as e:
        print(f"Error setting up data quality tables: {str(e)}")

if __name__ == "__main__":
    setup_data_quality()
