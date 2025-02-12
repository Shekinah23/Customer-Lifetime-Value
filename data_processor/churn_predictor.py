import sqlite3
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
from sklearn.metrics import classification_report, confusion_matrix
import joblib
from datetime import datetime, timedelta
import random

def generate_synthetic_features(df):
    """Generate synthetic features for testing"""
    np.random.seed(42)  # For reproducibility
    
    # Add random digital engagement with realistic distributions
    df['ACNTS_ATM_OPERN'] = np.random.exponential(scale=3, size=len(df))  # Most people use ATM moderately
    df['ACNTS_INET_OPERN'] = np.random.exponential(scale=5, size=len(df))  # Internet banking used more
    df['ACNTS_SMS_OPERN'] = np.random.exponential(scale=4, size=len(df))  # SMS banking moderate use
    
    # Round to integers and clip to reasonable ranges
    df['ACNTS_ATM_OPERN'] = df['ACNTS_ATM_OPERN'].round().clip(0, 20)
    df['ACNTS_INET_OPERN'] = df['ACNTS_INET_OPERN'].round().clip(0, 30)
    df['ACNTS_SMS_OPERN'] = df['ACNTS_SMS_OPERN'].round().clip(0, 25)
    
    # Add product codes with some products being more common
    df['ACNTS_PROD_CODE'] = np.random.choice([1, 2, 3, 4, 5], size=len(df), p=[0.4, 0.3, 0.15, 0.1, 0.05])
    df['ACNTS_AC_TYPE'] = np.random.choice([1, 2, 3], size=len(df), p=[0.6, 0.3, 0.1])
    
    # Add salary account and credit card flags with realistic probabilities
    df['ACNTS_SALARY_ACNT'] = np.random.choice([0, 1], size=len(df), p=[0.7, 0.3])
    df['ACNTS_CR_CARDS_ALLOWED'] = np.random.choice([0, 1], size=len(df), p=[0.8, 0.2])
    
    # Add dormant account flag (10% chance)
    df['ACNTS_DORMANT_ACNT'] = np.random.choice([0, 1], size=len(df), p=[0.9, 0.1])
    
    return df

def generate_transaction_dates(opening_date):
    """Generate synthetic transaction dates for testing"""
    if pd.isna(opening_date):
        return pd.NaT
    
    # Convert to timestamp if string
    if isinstance(opening_date, str):
        opening_date = pd.to_datetime(opening_date)
    
    # Generate random transaction date between opening date and now
    now = pd.Timestamp.now()
    days_since_opening = (now - opening_date).days
    
    if days_since_opening <= 0:
        return opening_date
    
    # Complex distribution based on account activity patterns
    rand = random.random()
    
    # Very active customers (30%): last 30 days
    if rand < 0.3:
        days_ago = random.randint(1, 30)
    # Moderately active (30%): 31-90 days
    elif rand < 0.6:
        days_ago = random.randint(31, 90)
    # Less active (25%): 91-180 days
    elif rand < 0.85:
        days_ago = random.randint(91, 180)
    # Inactive (15%): over 180 days
    else:
        days_ago = random.randint(181, max(182, days_since_opening))
    
    return now - pd.Timedelta(days=days_ago)

def calculate_churn_probability(row):
    """Calculate churn probability based on multiple factors"""
    prob = 0.0
    
    # Time-based factors (40% weight)
    if pd.isna(row['ACNTS_LAST_TRAN_DATE']):
        prob += 0.4  # Maximum time-based probability
    else:
        days_since = (pd.Timestamp.now() - row['ACNTS_LAST_TRAN_DATE']).days
        prob += min(0.4, (days_since / 180) * 0.4)  # Scale up to 40% max
    
    # Digital engagement (30% weight)
    digital_score = (
        row['ACNTS_ATM_OPERN'] / 20 +  # Normalized by max values
        row['ACNTS_INET_OPERN'] / 30 * 1.5 +  # Weight internet more
        row['ACNTS_SMS_OPERN'] / 25 * 1.2  # Weight SMS between ATM and internet
    ) / 3.7  # Normalize by sum of weights
    prob -= digital_score * 0.3  # Higher engagement reduces churn probability
    
    # Product relationships (20% weight)
    product_score = 0.0
    if row['ACNTS_SALARY_ACNT'] == 1:
        product_score += 0.5
    if row['ACNTS_CR_CARDS_ALLOWED'] == 1:
        product_score += 0.5
    prob -= (product_score / 2) * 0.2  # Normalize and apply weight
    
    # Account status (10% weight)
    if row['ACNTS_DORMANT_ACNT'] == 1:
        prob += 0.1
    
    # Add controlled randomness (±10%)
    prob += np.random.normal(0, 0.1)
    
    # Ensure probability is between 0 and 1
    return max(0.0, min(1.0, prob))

def load_data():
    """Load and prepare data from SQLite database"""
    conn = sqlite3.connect('banking_data.db')
    
    # Load clients data
    query = """
    SELECT 
        c.ACNTS_CLIENT_NUM,
        c.ACNTS_OPENING_DATE,
        c.ACNTS_LAST_TRAN_DATE,
        c.ACNTS_DORMANT_ACNT,
        c.ACNTS_AC_TYPE,
        c.ACNTS_PROD_CODE,
        c.ACNTS_SALARY_ACNT,
        c.ACNTS_ATM_OPERN,
        c.ACNTS_INET_OPERN,
        c.ACNTS_SMS_OPERN,
        c.ACNTS_CR_CARDS_ALLOWED,
        c.ACNTS_AC_NAME1,
        c.ACNTS_AC_ADDR1
    FROM clients c
    WHERE c.ACNTS_CLIENT_NUM != ''
    AND c.ACNTS_CLIENT_NUM IS NOT NULL
    AND c.ACNTS_OPENING_DATE IS NOT NULL
    LIMIT 10000  -- Using subset for testing
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert dates
    df['ACNTS_OPENING_DATE'] = pd.to_datetime(df['ACNTS_OPENING_DATE'])
    
    # Generate synthetic features
    df = generate_synthetic_features(df)
    
    # Generate synthetic transaction dates
    print("Generating synthetic transaction dates...")
    df['ACNTS_LAST_TRAN_DATE'] = df['ACNTS_OPENING_DATE'].apply(generate_transaction_dates)
    
    # Calculate churn probability and determine churn status
    print("Calculating churn probabilities...")
    df['churn_probability'] = df.apply(calculate_churn_probability, axis=1)
    
    # Use 75th percentile as churn threshold to get realistic distribution
    churn_threshold = df['churn_probability'].quantile(0.75)
    df['is_churned'] = (df['churn_probability'] > churn_threshold).astype(int)
    
    # Print initial data statistics
    print("\nInitial Data Statistics:")
    print(f"Total records: {len(df)}")
    print(f"Unique clients: {df['ACNTS_CLIENT_NUM'].nunique()}")
    print(f"Date range: {df['ACNTS_OPENING_DATE'].min()} to {df['ACNTS_LAST_TRAN_DATE'].max()}")
    print(f"Churn threshold: {churn_threshold:.3f}")
    print(f"Churn rate: {df['is_churned'].mean():.2%}")
    print("\nChurn Probability Distribution:")
    print(df['churn_probability'].describe())
    
    return df

def prepare_features(df):
    """Prepare features for modeling"""
    # Calculate account age in days
    now = pd.Timestamp.now()
    df['account_age_days'] = (now - df['ACNTS_OPENING_DATE']).dt.total_seconds() / (24 * 60 * 60)
    
    # Calculate days since last transaction
    df.loc[df['ACNTS_LAST_TRAN_DATE'].isna(), 'ACNTS_LAST_TRAN_DATE'] = df['ACNTS_OPENING_DATE']
    df['days_since_last_transaction'] = (now - df['ACNTS_LAST_TRAN_DATE']).dt.total_seconds() / (24 * 60 * 60)
    
    # Create digital engagement score (weighted sum)
    df['digital_engagement'] = (
        df['ACNTS_ATM_OPERN'] / df['ACNTS_ATM_OPERN'].max() +
        df['ACNTS_INET_OPERN'] / df['ACNTS_INET_OPERN'].max() * 2 +
        df['ACNTS_SMS_OPERN'] / df['ACNTS_SMS_OPERN'].max() * 1.5
    )
    
    # Create feature for having address (might indicate better customer relationship)
    df['has_address'] = df['ACNTS_AC_ADDR1'].notna().astype(int)
    
    # Create feature for account complexity
    df['account_complexity'] = (
        df['ACNTS_SALARY_ACNT'] * 2 +
        df['ACNTS_CR_CARDS_ALLOWED'] * 1.5 +
        df['digital_engagement']
    )
    
    # Select and prepare final features
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
    
    # Prepare final feature matrix
    X = df[feature_columns].copy()
    y = df['is_churned']
    
    # Add some random noise to features (0.5% noise)
    for col in X.columns:
        if X[col].dtype in ['int64', 'float64']:
            noise = np.random.normal(0, X[col].std() * 0.005, size=len(X))
            X[col] = X[col] + noise
    
    # Fill any remaining NaN values with appropriate defaults
    X = X.fillna(0)
    
    # Print data quality statistics
    print("\nData Quality Statistics:")
    print(f"Total samples: {len(df)}")
    print(f"Churn rate: {y.mean():.2%}")
    print("\nFeature Statistics:")
    print(X.describe())
    print("\nClass Distribution:")
    print(y.value_counts(normalize=True))
    
    return X, y

def train_models(X, y):
    """Train Random Forest and XGBoost models"""
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Train Random Forest
    print("\nTraining Random Forest...")
    rf_model = RandomForestClassifier(
        n_estimators=200,
        max_depth=10,
        min_samples_split=5,
        class_weight='balanced',
        random_state=42,
        n_jobs=-1
    )
    rf_model.fit(X_train_scaled, y_train)
    rf_pred = rf_model.predict(X_test_scaled)
    
    print("\nRandom Forest Results:")
    print(classification_report(y_test, rf_pred))
    
    # Train XGBoost
    print("\nTraining XGBoost...")
    xgb_model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        scale_pos_weight=len(y_train[y_train==0])/max(1, len(y_train[y_train==1])),
        random_state=42,
        n_jobs=-1
    )
    xgb_model.fit(X_train_scaled, y_train)
    xgb_pred = xgb_model.predict(X_test_scaled)
    
    print("\nXGBoost Results:")
    print(classification_report(y_test, xgb_pred))
    
    # Create ensemble predictions
    ensemble_pred = (rf_pred + xgb_pred) / 2
    ensemble_pred_binary = (ensemble_pred > 0.5).astype(int)
    
    # Print ensemble results
    print("\nEnsemble Model Results:")
    print("\nClassification Report:")
    print(classification_report(y_test, ensemble_pred_binary))
    
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, ensemble_pred_binary))
    
    # Save models and scaler
    print("\nSaving models...")
    joblib.dump(rf_model, 'data_processor/models/rf_model.joblib')
    joblib.dump(xgb_model, 'data_processor/models/xgb_model.joblib')
    joblib.dump(scaler, 'data_processor/models/scaler.joblib')
    
    return rf_model, xgb_model, scaler

def get_feature_importance(rf_model, xgb_model, feature_names):
    """Get and print feature importance from both models"""
    print("\nRandom Forest Feature Importance:")
    rf_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': rf_model.feature_importances_
    }).sort_values('importance', ascending=False)
    print(rf_importance)
    
    print("\nXGBoost Feature Importance:")
    xgb_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': xgb_model.feature_importances_
    }).sort_values('importance', ascending=False)
    print(xgb_importance)

if __name__ == "__main__":
    print("Loading data...")
    df = load_data()
    
    print("Preparing features...")
    X, y = prepare_features(df)
    
    print("Training models...")
    rf_model, xgb_model, scaler = train_models(X, y)
    
    print("Analyzing feature importance...")
    get_feature_importance(rf_model, xgb_model, X.columns)
