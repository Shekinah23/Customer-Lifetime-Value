import pandas as pd
import sqlite3
from pathlib import Path
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, db_path=None):
        """Initialize the data processor with database connection."""
        if db_path is None:
            db_path = str(Path(__file__).parent / "banking_data.db")
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.setup_database()

    def setup_database(self):
        """Create necessary database tables if they don't exist."""
        cursor = self.conn.cursor()

        # Drop existing tables
        cursor.execute("DROP TABLE IF EXISTS charges")
        cursor.execute("DROP TABLE IF EXISTS products")
        cursor.execute("DROP TABLE IF EXISTS transactions")

        # Create tables with correct schema
        cursor.execute('''CREATE TABLE transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            client_number TEXT NOT NULL,
            account_name TEXT NOT NULL,
            account_number TEXT NOT NULL,
            last_transaction_date TEXT NOT NULL,
            account_age INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''')

        cursor.execute('''CREATE TABLE products (
            product_code TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            product_class TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''')

        cursor.execute('''CREATE TABLE charges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code TEXT NOT NULL,
            fixed_amount DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_code) REFERENCES products(product_code)
        );''')

        # Create indices
        cursor.execute('CREATE INDEX idx_client_number ON transactions(client_number)')
        cursor.execute('CREATE INDEX idx_account_number ON transactions(account_number)')
        cursor.execute('CREATE INDEX idx_product_code ON products(product_code)')

        self.conn.commit()

    def fetch_and_process_data(self):
        """Create sample data for testing."""
        try:
            # Sample transactions data
            now = datetime.now()
            transactions_data = []
            
            # Corporate clients (C prefix)
            for i in range(1, 6):
                client_num = f"C{i:03d}"
                transactions_data.append((
                    client_num,
                    f"Corp Client {i}",
                    f"C{i:03d}ACC",
                    (now - timedelta(days=i)).strftime('%Y-%m-%d'),
                    12 * i  # months
                ))
            
            # Individual clients (I prefix)
            for i in range(1, 6):
                client_num = f"I{i:03d}"
                transactions_data.append((
                    client_num,
                    f"Individual Client {i}",
                    f"I{i:03d}ACC",
                    (now - timedelta(days=i*2)).strftime('%Y-%m-%d'),
                    6 * i  # months
                ))
            
            # Insert transactions
            cursor = self.conn.cursor()
            cursor.executemany('''
                INSERT INTO transactions 
                (client_id, client_number, account_name, account_number, last_transaction_date, account_age)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', [(data[0], *data) for data in transactions_data])
            
            # Sample products data
            products_data = [
                ('SAV', 'Savings Account', 'Deposit'),
                ('CHQ', 'Checking Account', 'Deposit'),
                ('LOAN', 'Personal Loan', 'Lending'),
                ('MORT', 'Mortgage', 'Lending'),
                ('CC', 'Credit Card', 'Credit')
            ]
            
            # Insert products
            cursor.executemany('''
                INSERT INTO products (product_code, product_name, product_class)
                VALUES (?, ?, ?)
            ''', products_data)
            
            self.conn.commit()
            
            # Verify data
            cursor.execute('SELECT COUNT(*) FROM transactions')
            transaction_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(*) FROM products')
            product_count = cursor.fetchone()[0]
            
            logger.info(f"Created {transaction_count} transactions and {product_count} products")
            logger.info("Sample data created successfully")
            return True

        except Exception as e:
            logger.error(f"Error creating sample data: {str(e)}")
            return False

    def calculate_metrics(self):
        """Calculate various business metrics."""
        cursor = self.conn.cursor()
        metrics = {}

        try:
            # Count total accounts
            cursor.execute('SELECT COUNT(*) FROM transactions')
            metrics['total_accounts'] = cursor.fetchone()[0]

            # Count active accounts (had transactions in last 3 months)
            cursor.execute('''
                SELECT COUNT(*) 
                FROM transactions 
                WHERE last_transaction_date >= date('now', '-3 months')
            ''')
            metrics['active_accounts'] = cursor.fetchone()[0]

            # Count products by type
            cursor.execute('''
                SELECT product_class, COUNT(*) as count 
                FROM products 
                GROUP BY product_class
            ''')
            metrics['products_by_class'] = dict(cursor.fetchall())

            return metrics

        except Exception as e:
            logger.error(f"Error calculating metrics: {str(e)}")
            return None

    def close(self):
        """Close the database connection."""
        self.conn.close()

def main():
    """Main function to run the data processing pipeline."""
    # Remove existing database if it exists
    db_path = Path(__file__).parent / "banking_data.db"
    if db_path.exists():
        db_path.unlink()
    
    processor = DataProcessor()
    
    logger.info("Starting data processing pipeline...")
    if processor.fetch_and_process_data():
        logger.info("Data processing completed successfully")
        
        # Calculate and display metrics
        metrics = processor.calculate_metrics()
        if metrics:
            logger.info("\nBusiness Metrics:")
            for metric, value in metrics.items():
                logger.info(f"{metric}: {value}")
        else:
            logger.error("Failed to calculate metrics")
    else:
        logger.error("Data processing failed")

    processor.close()

if __name__ == "__main__":
    main()
