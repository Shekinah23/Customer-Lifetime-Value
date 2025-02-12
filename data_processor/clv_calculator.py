import pandas as pd
import numpy as np
import sqlite3
import joblib
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def load_models():
    """Load the trained churn prediction models"""
    try:
        rf_model = joblib.load('data_processor/models/rf_model.joblib')
        xgb_model = joblib.load('data_processor/models/xgb_model.joblib')
        scaler = joblib.load('data_processor/models/scaler.joblib')
        return rf_model, xgb_model, scaler
    except Exception as e:
        print(f"Error loading models: {e}")
        return None, None, None

def generate_transaction_amount(prod_code, base_amount):
    """Generate realistic transaction amounts with variation"""
    if prod_code == 1:  # High-value accounts
        mean = 1000 + base_amount
        std = 300
    elif prod_code == 2:  # Medium-value accounts
        mean = 500 + base_amount
        std = 150
    else:  # Standard accounts
        mean = 200 + base_amount
        std = 75
    
    # Generate amount with normal distribution and ensure it's positive
    amount = np.random.normal(mean, std)
    return max(50, amount)  # Minimum transaction amount of 50

def get_customer_data():
    """Load customer data with transactions"""
    conn = sqlite3.connect('banking_data.db')
    
    # First get client data
    client_query = """
    SELECT 
        ACNTS_CLIENT_NUM,
        ACNTS_OPENING_DATE,
        ACNTS_LAST_TRAN_DATE,
        ACNTS_DORMANT_ACNT,
        ACNTS_AC_TYPE,
        ACNTS_PROD_CODE,
        ACNTS_SALARY_ACNT,
        ACNTS_ATM_OPERN,
        ACNTS_INET_OPERN,
        ACNTS_SMS_OPERN,
        ACNTS_CR_CARDS_ALLOWED,
        ACNTS_AC_NAME1,
        ACNTS_AC_ADDR1
    FROM clients
    WHERE ACNTS_CLIENT_NUM != ''
    AND ACNTS_CLIENT_NUM IS NOT NULL
    AND ACNTS_OPENING_DATE IS NOT NULL
    """
    
    df = pd.read_sql_query(client_query, conn)
    conn.close()
    
    # Generate synthetic transaction data
    np.random.seed(42)  # For reproducibility
    
    # Assign product codes with realistic distribution
    df['ACNTS_PROD_CODE'] = np.random.choice(
        [1, 2, 3],  # Product types
        size=len(df),
        p=[0.2, 0.3, 0.5]  # 20% premium, 30% standard, 50% basic
    )
    
    # Generate number of accounts based on product type
    df['num_accounts'] = df.apply(lambda row: np.random.choice(
        [1, 2, 3],
        p=[0.5, 0.3, 0.2] if row['ACNTS_PROD_CODE'] == 1 else  # Premium customers more likely to have multiple accounts
        [0.7, 0.2, 0.1] if row['ACNTS_PROD_CODE'] == 2 else    # Standard customers
        [0.9, 0.08, 0.02]                                       # Basic customers mostly have one account
    ), axis=1)
    
    # Generate transaction counts based on account type and activity
    df['transaction_count'] = df.apply(lambda row: 
        np.random.randint(
            5 if row['ACNTS_DORMANT_ACNT'] else 20,   # Min transactions
            20 if row['ACNTS_DORMANT_ACNT'] else      # Max transactions for dormant
            150 if row['ACNTS_PROD_CODE'] == 1 else   # Premium accounts
            100 if row['ACNTS_PROD_CODE'] == 2 else   # Standard accounts
            50                                         # Basic accounts
        ) * row['num_accounts'],  # More accounts = more transactions
        axis=1
    )
    
    # Generate average transaction amounts
    df['avg_transaction_amount'] = df.apply(lambda row:
        generate_transaction_amount(
            row['ACNTS_PROD_CODE'],
            row['ACNTS_SALARY_ACNT'] * 200 +  # Salary accounts have higher amounts
            row['ACNTS_CR_CARDS_ALLOWED'] * 100  # Credit card holders spend more
        ),
        axis=1
    )
    
    # Calculate total transaction amount
    df['total_transaction_amount'] = df['avg_transaction_amount'] * df['transaction_count']
    
    return df

def prepare_features(df):
    """Prepare features for churn prediction"""
    # Calculate account age in days
    now = pd.Timestamp.now()
    df['account_age_days'] = (now - pd.to_datetime(df['ACNTS_OPENING_DATE'])).dt.total_seconds() / (24 * 60 * 60)
    
    # Calculate days since last transaction
    df['days_since_last_transaction'] = (now - pd.to_datetime(df['ACNTS_LAST_TRAN_DATE'])).dt.total_seconds() / (24 * 60 * 60)
    df['days_since_last_transaction'] = df['days_since_last_transaction'].fillna(df['account_age_days'])
    
    # Create digital engagement score
    df['digital_engagement'] = (
        df['ACNTS_ATM_OPERN'] / df['ACNTS_ATM_OPERN'].max() +
        df['ACNTS_INET_OPERN'] / df['ACNTS_INET_OPERN'].max() * 2 +
        df['ACNTS_SMS_OPERN'] / df['ACNTS_SMS_OPERN'].max() * 1.5
    )
    
    # Create account complexity score
    df['account_complexity'] = (
        df['ACNTS_SALARY_ACNT'] * 2 +
        df['ACNTS_CR_CARDS_ALLOWED'] * 1.5 +
        df['digital_engagement']
    )
    
    # Create feature matrix
    feature_columns = [
        'account_age_days',
        'days_since_last_transaction',
        'ACNTS_DORMANT_ACNT',
        'ACNTS_AC_TYPE',
        'ACNTS_PROD_CODE',
        'ACNTS_SALARY_ACNT',
        'digital_engagement',
        'ACNTS_CR_CARDS_ALLOWED',
        'has_address',
        'account_complexity'
    ]
    
    # Add has_address feature
    df['has_address'] = df['ACNTS_AC_ADDR1'].notna().astype(int)
    
    return df[feature_columns]

def predict_churn_probability(df, rf_model, xgb_model, scaler):
    """Predict churn probability using ensemble model"""
    # Prepare features
    X = prepare_features(df)
    
    # Scale features
    X_scaled = scaler.transform(X)
    
    # Get predictions from both models
    rf_pred = rf_model.predict_proba(X_scaled)[:, 1]
    xgb_pred = xgb_model.predict_proba(X_scaled)[:, 1]
    
    # Ensemble prediction
    base_prob = (rf_pred + xgb_pred) / 2
    
    # Adjust probability based on product type and engagement
    df['adjusted_prob'] = base_prob.copy()
    
    # Premium accounts are less likely to churn
    df.loc[df['ACNTS_PROD_CODE'] == 1, 'adjusted_prob'] *= 0.7
    
    # Multiple accounts reduce churn probability
    df.loc[df['num_accounts'] > 1, 'adjusted_prob'] *= 0.8
    
    # High transaction counts indicate loyalty
    median_trans = df['transaction_count'].median()
    df.loc[df['transaction_count'] > median_trans, 'adjusted_prob'] *= 0.9
    
    # Ensure probabilities are between 0 and 1
    return df['adjusted_prob'].clip(0.1, 0.9)  # Cap at 90% to avoid infinite lifespans

def calculate_monthly_value(row):
    """Calculate average monthly value for a customer"""
    months = max(1, row['account_age_days'] / 30)  # Convert days to months
    
    # Base monthly value from transactions
    monthly_value = row['total_transaction_amount'] / months
    
    # Product-based revenue
    if row['ACNTS_SALARY_ACNT']:
        monthly_value += 5  # Monthly fee for salary account
    if row['ACNTS_CR_CARDS_ALLOWED']:
        monthly_value += 10  # Average monthly revenue from credit card
    
    # Service-based revenue (fees)
    monthly_value += row['ACNTS_ATM_OPERN'] * 0.5  # ATM fees
    monthly_value += row['ACNTS_INET_OPERN'] * 0.2  # Internet banking fees
    monthly_value += row['ACNTS_SMS_OPERN'] * 0.1  # SMS banking fees
    
    # Account type based revenue
    if row['ACNTS_PROD_CODE'] == 1:  # High-value accounts
        monthly_value += 20  # Premium account fee
    elif row['ACNTS_PROD_CODE'] == 2:  # Medium-value accounts
        monthly_value += 10  # Standard account fee
    else:
        monthly_value += 5  # Basic account fee
    
    # Multiple accounts bonus
    if row['num_accounts'] > 1:
        monthly_value *= 1.2  # 20% bonus for multiple accounts
    
    # Add random variation (±5%)
    variation = np.random.uniform(0.95, 1.05)
    monthly_value *= variation
    
    return monthly_value

def assign_clv_segment(clv_value, boundaries):
    """Assign CLV segment based on value boundaries"""
    if clv_value <= boundaries[0]:
        return 'Very Low'
    elif clv_value <= boundaries[1]:
        return 'Low'
    elif clv_value <= boundaries[2]:
        return 'Medium'
    elif clv_value <= boundaries[3]:
        return 'High'
    else:
        return 'Very High'

def calculate_clv(df, rf_model, xgb_model, scaler):
    """Calculate Customer Lifetime Value"""
    # Predict churn probability
    print("Predicting churn probabilities...")
    df['churn_probability'] = predict_churn_probability(df, rf_model, xgb_model, scaler)
    
    # Calculate monthly value
    print("Calculating monthly values...")
    df['monthly_value'] = df.apply(calculate_monthly_value, axis=1)
    
    # Calculate expected lifespan (in months)
    print("Calculating customer lifespans...")
    df['expected_lifespan'] = 12 * (1 / df['churn_probability'])  # Convert to months
    df['expected_lifespan'] = df['expected_lifespan'].clip(1, 120)  # Cap at 10 years
    
    # Calculate CLV with time value of money
    print("Calculating CLV...")
    discount_rate = 0.1  # 10% annual discount rate
    monthly_discount_rate = (1 + discount_rate) ** (1/12) - 1
    
    df['clv'] = df.apply(lambda row: sum([
        row['monthly_value'] / (1 + monthly_discount_rate) ** i 
        for i in range(int(row['expected_lifespan']))
    ]), axis=1)
    
    # Create CLV segments using percentile boundaries
    print("Segmenting customers...")
    boundaries = [
        df['clv'].quantile(0.2),
        df['clv'].quantile(0.4),
        df['clv'].quantile(0.6),
        df['clv'].quantile(0.8)
    ]
    df['clv_segment'] = df['clv'].apply(lambda x: assign_clv_segment(x, boundaries))
    
    # Prepare final results
    results = df[[
        'ACNTS_CLIENT_NUM', 
        'ACNTS_AC_NAME1', 
        'ACNTS_PROD_CODE',
        'num_accounts',
        'transaction_count',
        'avg_transaction_amount',
        'monthly_value', 
        'churn_probability', 
        'expected_lifespan', 
        'clv', 
        'clv_segment'
    ]]
    
    # Add derived insights
    results['retention_rate'] = (1 - results['churn_probability']) * 100
    results['annual_value'] = results['monthly_value'] * 12
    
    return results

def main():
    print("Loading models...")
    rf_model, xgb_model, scaler = load_models()
    if not all([rf_model, xgb_model, scaler]):
        print("Error: Could not load models")
        return
    
    print("Loading customer data...")
    df = get_customer_data()
    
    print("Calculating CLV...")
    results = calculate_clv(df, rf_model, xgb_model, scaler)
    
    print("\nCLV Summary Statistics:")
    print(results['clv'].describe())
    
    print("\nCLV Segment Distribution:")
    print(results['clv_segment'].value_counts(normalize=True))
    
    print("\nAverage CLV by Product Type:")
    avg_clv = results.groupby('ACNTS_PROD_CODE')['clv'].agg(['mean', 'count']).round(2)
    print(avg_clv)
    
    print("\nRetention Rate by Product Type:")
    avg_retention = results.groupby('ACNTS_PROD_CODE')['retention_rate'].mean().round(2)
    print(avg_retention)
    
    print("\nRetention Rate by CLV Segment:")
    segment_retention = results.groupby('clv_segment')['retention_rate'].mean().round(2)
    print(segment_retention)
    
    print("\nSample Customer CLV Values:")
    print(results.head().round(2))
    
    # Save results
    output_path = 'data_processor/customer_clv.csv'
    results.to_csv(output_path, index=False)
    print(f"\nResults saved to: {output_path}")

if __name__ == "__main__":
    main()
