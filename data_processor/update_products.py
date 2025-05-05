import sqlite3
import csv
import os
import sys

def update_products_table():
    print("Starting products table update...")
    # Path to the database and CSV file
    db_path = 'banking_data.db'
    csv_path = os.path.join('datasets', 'products.csv')
    
    if not os.path.exists(db_path):
        print(f"Error: Database file {db_path} not found!")
        return False
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found!")
        return False
    
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Backup existing data (if any)
        print("Backing up existing products data...")
        try:
            cursor.execute("CREATE TABLE IF NOT EXISTS products_backup AS SELECT * FROM products")
            print("Backup created successfully.")
        except sqlite3.Error as e:
            print(f"Warning: Could not create backup - {e}")
        
        # Get the CSV structure
        with open(csv_path, 'r') as f:
            csv_reader = csv.reader(f)
            headers = next(csv_reader)
            print(f"CSV headers: {headers}")
        
        # Drop existing products table
        print("Dropping existing products table...")
        cursor.execute("DROP TABLE IF EXISTS products")
        
        # Create table based on CSV structure
        columns = ", ".join([f"{header} TEXT" for header in headers])
        create_table_sql = f"CREATE TABLE products ({columns})"
        print(f"Creating new table with SQL: {create_table_sql}")
        cursor.execute(create_table_sql)
        
        # Import data from CSV
        print("Importing data from CSV...")
        with open(csv_path, 'r') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)  # Skip header row
            
            # Create placeholders for INSERT statement
            placeholders = ", ".join(["?" for _ in headers])
            insert_sql = f"INSERT INTO products VALUES ({placeholders})"
            
            # Batch insert for better performance
            batch_size = 100
            batch = []
            
            for row in csv_reader:
                batch.append(row)
                if len(batch) >= batch_size:
                    cursor.executemany(insert_sql, batch)
                    batch = []
            
            # Insert any remaining rows
            if batch:
                cursor.executemany(insert_sql, batch)
        
        # Commit changes and close connection
        conn.commit()
        
        # Verify the import
        cursor.execute("SELECT COUNT(*) FROM products")
        count = cursor.fetchone()[0]
        print(f"Successfully imported {count} products.")
        
        # Show sample data
        cursor.execute("SELECT * FROM products LIMIT 5")
        sample = cursor.fetchall()
        print("Sample data:")
        for row in sample:
            print(row)
        
        conn.close()
        return True
        
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    success = update_products_table()
    if success:
        print("Products table updated successfully!")
    else:
        print("Failed to update products table.")
        sys.exit(1) 