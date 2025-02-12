import sqlite3
import pandas as pd
from datetime import datetime

def get_season(date_str):
    date = datetime.strptime(date_str, '%Y-%m-%d')
    month = date.month
    if month in [12, 1, 2]:
        return 'Summer'
    elif month in [3, 4, 5]:
        return 'Autumn'
    elif month in [6, 7, 8]:
        return 'Winter'
    else:
        return 'Spring'


def analyze_transactions():
    conn = sqlite3.connect('data_processor/banking_data.db')
    
    # Get detailed transaction analysis
    queries = {
        'account_summary': """
        SELECT 
            COUNT(*) as total_accounts,
            COUNT(DISTINCT client_id) as unique_clients,
            AVG(account_age) as avg_account_age,
            MIN(account_age) as min_account_age,
            MAX(account_age) as max_account_age
        FROM transactions
        """,
        'client_type_distribution': """
        SELECT 
            CASE 
                WHEN client_number LIKE 'C%' THEN 'Corporate'
                WHEN client_number LIKE 'I%' THEN 'Individual'
                ELSE 'Other'
            END as client_type,
            COUNT(*) as account_count,
            AVG(account_age) as avg_account_age
        FROM transactions
        GROUP BY 
            CASE 
                WHEN client_number LIKE 'C%' THEN 'Corporate'
                WHEN client_number LIKE 'I%' THEN 'Individual'
                ELSE 'Other'
            END
        """,
        'age_distribution': """
        SELECT 
            CASE 
                WHEN account_age <= 12 THEN '0-12 months'
                WHEN account_age <= 24 THEN '13-24 months'
                WHEN account_age <= 36 THEN '25-36 months'
                ELSE 'Over 36 months'
            END as age_range,
            COUNT(*) as account_count
        FROM transactions
        GROUP BY 
            CASE 
                WHEN account_age <= 12 THEN '0-12 months'
                WHEN account_age <= 24 THEN '13-24 months'
                WHEN account_age <= 36 THEN '25-36 months'
                ELSE 'Over 36 months'
            END
        ORDER BY 
            CASE age_range
                WHEN '0-12 months' THEN 1
                WHEN '13-24 months' THEN 2
                WHEN '25-36 months' THEN 3
                ELSE 4
            END
        """,
        'recent_transactions': """
        SELECT 
            client_number,
            account_name,
            account_number,
            last_transaction_date,
            account_age
        FROM transactions
        ORDER BY last_transaction_date DESC
        LIMIT 5
        """
    }
    
    print("\nDetailed Transaction Analysis:")
    print("=" * 50)

    # Account Summary
    print("\n1. Overall Account Summary:")
    print("-" * 30)
    df_summary = pd.read_sql_query(queries['account_summary'], conn)
    row = df_summary.iloc[0]
    print(f"Total Accounts: {row['total_accounts']:,}")
    print(f"Unique Clients: {row['unique_clients']:,}")
    print(f"Average Account Age: {row['avg_account_age']:.1f} months")
    print(f"Account Age Range: {row['min_account_age']} to {row['max_account_age']} months")

    # Client Type Distribution
    print("\n2. Client Type Distribution:")
    print("-" * 30)
    df_type = pd.read_sql_query(queries['client_type_distribution'], conn)
    for _, row in df_type.iterrows():
        print(f"{row['client_type']}:")
        print(f"  Accounts: {row['account_count']:,}")
        print(f"  Average Account Age: {row['avg_account_age']:.1f} months")

    # Account Age Distribution
    print("\n3. Account Age Distribution:")
    print("-" * 30)
    df_age = pd.read_sql_query(queries['age_distribution'], conn)
    for _, row in df_age.iterrows():
        print(f"{row['age_range']}: {row['account_count']:,} accounts")

    # Recent Transactions
    print("\n4. Most Recent Transactions:")
    print("-" * 30)
    df_recent = pd.read_sql_query(queries['recent_transactions'], conn)
    df_recent['last_transaction_date'] = pd.to_datetime(df_recent['last_transaction_date'])
    for _, row in df_recent.iterrows():
        print(f"\nClient: {row['client_number']} - {row['account_name']}")
        print(f"  Account: {row['account_number']}")
        print(f"  Last Transaction: {row['last_transaction_date'].strftime('%Y-%m-%d')}")
        print(f"  Account Age: {row['account_age']} months")
    
    conn.close()

def analyze_seasonal_products():
    try:
        conn = sqlite3.connect('data_processor/banking_data.db')
        
        # First check what tables exist
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print("\nAvailable tables:", [table[0] for table in tables])
        
        # First get the schema of products table
        cursor.execute("PRAGMA table_info(products)")
        product_columns = cursor.fetchall()
        print("\nProducts table columns:", [col[1] for col in product_columns])
        
        # Analyze transaction patterns by client type and account age
        query = """
        SELECT 
            CASE 
                WHEN client_number LIKE 'C%' THEN 'Corporate'
                ELSE 'Individual'
            END as client_type,
            CASE 
                WHEN account_age <= 12 THEN 'New (0-12 months)'
                WHEN account_age <= 24 THEN 'Growing (13-24 months)'
                WHEN account_age <= 36 THEN 'Established (25-36 months)'
                ELSE 'Mature (>36 months)'
            END as account_stage,
            COUNT(*) as transaction_count,
            AVG(account_age) as avg_age,
            last_transaction_date
        FROM transactions
        WHERE last_transaction_date IS NOT NULL
        GROUP BY 
            CASE 
                WHEN client_number LIKE 'C%' THEN 'Corporate'
                ELSE 'Individual'
            END,
            CASE 
                WHEN account_age <= 12 THEN 'New (0-12 months)'
                WHEN account_age <= 24 THEN 'Growing (13-24 months)'
                WHEN account_age <= 36 THEN 'Established (25-36 months)'
                ELSE 'Mature (>36 months)'
            END,
            last_transaction_date
        ORDER BY last_transaction_date DESC
        """
    
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            print("\nNo transaction data found for analysis.")
            return
            
        print("\nTransaction Pattern Analysis:")
        print("=" * 50)
        
        # Group by client type and account stage
        analysis = df.groupby(['client_type', 'account_stage']).agg({
            'transaction_count': 'sum',
            'avg_age': 'mean'
        }).reset_index()
        
        for client_type in ['Corporate', 'Individual']:
            client_data = analysis[analysis['client_type'] == client_type]
            if not client_data.empty:
                print(f"\n{client_type} Client Activity:")
                print("-" * 30)
                for _, row in client_data.iterrows():
                    print(f"{row['account_stage']}:")
                    print(f"  Transactions: {int(row['transaction_count']):,}")
                    print(f"  Average Account Age: {row['avg_age']:.1f} months")
                
        # Get most recent transaction date
        latest_date = pd.to_datetime(df['last_transaction_date'].max())
        print(f"\nAnalysis based on transactions up to: {latest_date.strftime('%Y-%m-%d')}")
        
        print("\nRecommendations:")
        print("=" * 50)
        
        print("\nQ1 (Jan-Mar):")
        print("-" * 30)
        print("Corporate Clients:")
        print("• Target mature accounts (>36 months) for high-value products")
        print("• Review and renew long-term corporate relationships")
        print("Individual Clients:")
        print("• Focus on new account acquisition (0-12 months)")
        print("• Introduce starter products for new individual clients")
        
        print("\nQ2 (Apr-Jun):")
        print("-" * 30)
        print("Corporate Clients:")
        print("• Engage established accounts (25-36 months) for product upgrades")
        print("• Implement corporate loyalty programs")
        print("Individual Clients:")
        print("• Target growing accounts (13-24 months) for product expansion")
        print("• Introduce investment products for maturing relationships")
        
        print("\nQ3 (Jul-Sep):")
        print("-" * 30)
        print("Corporate Clients:")
        print("• Focus on growing accounts (13-24 months) for service expansion")
        print("• Develop corporate referral programs")
        print("Individual Clients:")
        print("• Enhance established account relationships (25-36 months)")
        print("• Promote savings and investment products")
        
        print("\nQ4 (Oct-Dec):")
        print("-" * 30)
        print("Corporate Clients:")
        print("• Year-end review of all account stages")
        print("• Launch new corporate product offerings")
        print("Individual Clients:")
        print("• Focus on account retention and upgrades")
        print("• Introduce year-end savings products")
        
    except sqlite3.Error as e:
        print(f"\nDatabase error occurred: {e}")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    analyze_transactions()
    analyze_seasonal_products()
