import os
import sys
import subprocess
import sqlite3
import json
from datetime import datetime

def check_database():
    """Check if the database exists and has the clients table"""
    try:
        # Check if database file exists
        if not os.path.exists('data_processor/banking_data.db'):
            print("Database file not found.")
            return False
            
        # Connect to database
        conn = sqlite3.connect('data_processor/banking_data.db')
        cursor = conn.cursor()
        
        # Check if clients table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='clients'")
        if cursor.fetchone() is None:
            print("Clients table not found in database.")
            conn.close()
            return False
            
        # Check if the table has records
        cursor.execute("SELECT COUNT(*) FROM clients")
        count = cursor.fetchone()[0]
        conn.close()
        
        if count == 0:
            print("Clients table exists but is empty.")
            return False
            
        print(f"Database verified. Found {count} client records.")
        return True
        
    except Exception as e:
        print(f"Error checking database: {e}")
        return False

def import_sample_accounts(accounts_file):
    """Import sample ACCOUNTS data for testing"""
    try:
        print(f"Importing sample ACCOUNTS data from {accounts_file}...")
        import_result = subprocess.run(
            [sys.executable, "import_accounts_sample.py", accounts_file, "1000"],
            capture_output=True,
            text=True
        )
        
        if import_result.returncode != 0:
            print(f"Error importing sample ACCOUNTS data: {import_result.stderr}")
            return False
            
        print(import_result.stdout)
        return True
        
    except Exception as e:
        print(f"Error running import_accounts_sample.py: {e}")
        return False

def update_dashboard_stats():
    """Update the dashboard statistics"""
    try:
        print("Updating dashboard statistics...")
        update_result = subprocess.run(
            [sys.executable, "update_dashboard_stats.py"],
            capture_output=True,
            text=True
        )
        
        if update_result.returncode != 0:
            print(f"Error updating dashboard statistics: {update_result.stderr}")
            return False
            
        print(update_result.stdout)
        return True
        
    except Exception as e:
        print(f"Error running update script: {e}")
        return False

def run_application():
    """Run the Flask application"""
    try:
        print("Starting Flask application...")
        os.chdir("data_processor")
        # Start Flask app in a subprocess
        flask_process = subprocess.Popen(
            [sys.executable, "app.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Print the first few lines of output to confirm server startup
        for i in range(10):
            line = flask_process.stdout.readline()
            if not line:
                break
            print(line.strip())
            
            # If we see the server running message, break early
            if "Running on" in line:
                print("\nApplication started successfully!")
                print("Press Ctrl+C to stop the server.")
                break
                
        # Keep the process running
        try:
            flask_process.wait()
        except KeyboardInterrupt:
            print("\nStopping server...")
            flask_process.terminate()
            flask_process.wait()
            print("Server stopped.")
            
    except Exception as e:
        print(f"Error running Flask application: {e}")
        return False

def main():
    # Check for ACCOUNTS file
    accounts_file = None
    
    if len(sys.argv) > 1:
        accounts_file = sys.argv[1]
    else:
        # Try to find ACCOUNTS.csv in the current directory or data_processor/datasets
        candidates = [
            "ACCOUNTS.csv",
            "ACCOUNTS.CSV",
            "data_processor/datasets/ACCOUNTS.csv",
            "data_processor/datasets/ACCOUNTS.CSV"
        ]
        
        for candidate in candidates:
            if os.path.exists(candidate):
                accounts_file = candidate
                break
                
    if not accounts_file:
        print("ACCOUNTS dataset file not found.")
        print("Usage: python run_app_with_accounts.py [path_to_accounts_file]")
        return False
        
    # Check if file exists
    if not os.path.exists(accounts_file):
        print(f"File not found: {accounts_file}")
        return False
        
    # Import ACCOUNTS data (sample for testing)
    if not import_sample_accounts(accounts_file):
        print("Failed to import sample ACCOUNTS data. Exiting.")
        return False
        
    # Update dashboard statistics
    if not update_dashboard_stats():
        print("Failed to update dashboard statistics. Exiting.")
        return False
        
    # Check database
    if not check_database():
        print("Database verification failed. Exiting.")
        return False
        
    # Run the application
    run_application()
    
    return True

if __name__ == "__main__":
    main() 