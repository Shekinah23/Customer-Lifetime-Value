import sqlite3
import json
import os
from datetime import datetime, timedelta

def get_db_connection():
    """Connect to the SQLite database."""
    try:
        conn = sqlite3.connect('data_processor/banking_data.db')
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def update_account_statistics():
    """Update account statistics in the performance_metrics table"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Ensure performance_metrics table exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS performance_metrics (
            metric_key TEXT PRIMARY KEY,
            metric_value TEXT,
            segment_distribution TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
        
        # Calculate account statistics based on ACNTS_INOP_ACNT and ACNTS_DORMANT_ACNT
        # As per requirements:
        # - Accounts with ACNTS_INOP_ACNT=1 are inoperative (after 90 days of no activity)
        # - Accounts with ACNTS_DORMANT_ACNT=1 are dormant (after 90 days of being inoperative)
        # - Accounts closed after 1 year of being dormant
        cursor.execute("""
        SELECT 
            COUNT(*) as total_clients,
            SUM(CASE WHEN ACNTS_INOP_ACNT = 0 AND ACNTS_DORMANT_ACNT = 0 THEN 1 ELSE 0 END) as active_clients,
            SUM(CASE WHEN ACNTS_INOP_ACNT = 1 AND ACNTS_DORMANT_ACNT = 0 THEN 1 ELSE 0 END) as inactive_clients,
            SUM(CASE WHEN ACNTS_DORMANT_ACNT = 1 THEN 1 ELSE 0 END) as dormant_clients
        FROM clients
        """)
        
        result = cursor.fetchone()
        
        # Calculate closed clients
        # A client is considered closed if they've been dormant for over a year
        # For this simulation, we'll check if the last transaction date is over a year ago
        # and the account is marked as dormant
        one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        cursor.execute(f"""
        SELECT COUNT(*) as closed_clients
        FROM clients
        WHERE ACNTS_DORMANT_ACNT = 1 
        AND ACNTS_LAST_TRAN_DATE IS NOT NULL
        AND ACNTS_LAST_TRAN_DATE < ?
        """, (one_year_ago,))
        
        closed_result = cursor.fetchone()
        closed_clients = closed_result['closed_clients'] if closed_result else 0
        
        # Create metrics object
        metrics = {
            'total_clients': result['total_clients'],
            'active_clients': result['active_clients'],
            'inactive_clients': result['inactive_clients'],
            'dormant_clients': result['dormant_clients'],
            'closed_clients': closed_clients,
            'contributing_clients': result['total_clients'],  # All clients contribute to statistics
            'total_clients_growth': 0.5,  # Sample growth rate
            'total_clients_growth_abs': 0.5  # Sample absolute growth rate
        }
        
        # Save to performance_metrics table
        cursor.execute("""
        INSERT OR REPLACE INTO performance_metrics 
        (metric_key, metric_value, last_updated)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        """, ('account_metrics', json.dumps(metrics)))
        
        # For dashboard use - revenue_metrics is used in the dashboard template
        cursor.execute("""
        INSERT OR REPLACE INTO performance_metrics 
        (metric_key, metric_value, last_updated)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        """, ('revenue_metrics', json.dumps({
            'active_clients': result['active_clients'],
            'inactive_clients': result['inactive_clients'],
            'closed_clients': closed_clients,
            'contributing_clients': result['total_clients']
        })))
        
        conn.commit()
        conn.close()
        
        print("Account statistics updated successfully!")
        print(f"Total Clients: {metrics['total_clients']}")
        print(f"Active Clients: {metrics['active_clients']}")
        print(f"Inactive Clients: {metrics['inactive_clients']}")
        print(f"Dormant Clients: {metrics['dormant_clients']}")
        print(f"Closed Clients: {metrics['closed_clients']}")
        
        return True
        
    except Exception as e:
        print(f"Error updating account statistics: {e}")
        if conn:
            conn.close()
        return False

if __name__ == "__main__":
    update_account_statistics() 