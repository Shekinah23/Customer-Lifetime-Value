import sqlite3
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(current_dir, 'data_processor', 'banking_data.db')
if not os.path.exists(db_path):
    db_path = os.path.join(current_dir, 'banking_data.db')

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get column names
cursor.execute("PRAGMA table_info(clients)")
columns = cursor.fetchall()
print("\nColumns in clients table:")
for col in columns:
    print(f"{col[1]} ({col[2]})")

conn.close()
