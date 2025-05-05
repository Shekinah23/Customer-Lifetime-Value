import sqlite3
import json

def fix_dashboard_metrics():
    try:
        # Connect to the database
        conn = sqlite3.connect('data_processor/banking_data.db')
        cursor = conn.cursor()
        
        # Create the performance_metrics table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS performance_metrics (
            metric_key TEXT PRIMARY KEY,
            metric_value TEXT,
            segment_distribution TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Get client statistics
        cursor.execute("""
        SELECT 
            COUNT(*) as total_clients,
            SUM(CASE WHEN ACNTS_INOP_ACNT = 0 AND ACNTS_DORMANT_ACNT = 0 THEN 1 ELSE 0 END) as active_clients,
            SUM(CASE WHEN ACNTS_INOP_ACNT = 1 AND ACNTS_DORMANT_ACNT = 0 THEN 1 ELSE 0 END) as inactive_clients,
            SUM(CASE WHEN ACNTS_DORMANT_ACNT = 1 THEN 1 ELSE 0 END) as dormant_clients
        FROM clients
        """)
        
        result = cursor.fetchone()
        
        # Create metrics dictionary with explicit values to avoid None/NULL
        metrics = {
            'active_clients': result[1] or 0,
            'inactive_clients': result[2] or 0,
            'closed_clients': result[3] or 0,  # Using dormant as closed for display
            'contributing_clients': result[0] or 0
        }
        
        # Convert to JSON
        metrics_json = json.dumps(metrics)
        
        # Insert or replace the metrics
        cursor.execute("""
        INSERT OR REPLACE INTO performance_metrics 
        (metric_key, metric_value, last_updated)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        """, ('revenue_metrics', metrics_json))
        
        conn.commit()
        
        print("Dashboard metrics fixed successfully:")
        print(f"Active clients: {metrics['active_clients']}")
        print(f"Inactive clients: {metrics['inactive_clients']}")
        print(f"Closed clients: {metrics['closed_clients']}")
        print(f"Contributing clients: {metrics['contributing_clients']}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error fixing dashboard metrics: {e}")
        if 'conn' in locals() and conn:
            conn.close()
        return False

if __name__ == "__main__":
    fix_dashboard_metrics() 