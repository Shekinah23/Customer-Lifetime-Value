from flask import Flask, render_template, request, redirect, url_for, session, jsonify, Response
from functools import wraps
import os
import sqlite3
import json
import pandas as pd
from datetime import timedelta
from churn_predictor import calculate_churn_probability

app = Flask(__name__, 
           template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), 'templates')),
           static_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), 'static')),
           static_url_path='/static')
app.secret_key = 'dev_secret_key_123'
app.permanent_session_lifetime = timedelta(days=1)

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session or not session.get('authenticated'):
            print(f"\nRedirecting to login - authentication required")
            print(f"Current session: {session}")
            session.clear()  # Clear any invalid session
            return redirect(url_for('login'))
        print(f"\nUser authenticated: {session['user']}")
        return f(*args, **kwargs)
    return decorated_function

def get_db():
    """Get database connection with optimized settings"""
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'banking_data.db')
    
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA cache_size=-2000')
        conn.execute('PRAGMA temp_store=MEMORY')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA mmap_size=2147483648')
        
        return conn
        
    except Exception as e:
        print(f"Database error: {str(e)}")
        return None

@app.route('/')
@login_required
def index():
    return redirect(url_for('landing_page'))

@app.route('/landing')
@login_required
def landing_page():
    return render_template('landing.html')

@app.route('/dashboard')
@login_required
def dashboard():
    # Get a sample client for churn analysis
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM clients 
        WHERE ACNTS_LAST_TRAN_DATE IS NOT NULL 
        ORDER BY RANDOM() 
        LIMIT 1
    """)
    client = cursor.fetchone()
    
    # Calculate churn factors for the sample client
    if client:
        client_dict = dict(client)
        churn_factors = {
            'time_based': 0,
            'digital_engagement': 0,
            'product_relationships': 0,
            'account_status': 0
        }
        
        # Time-based factor
        if client_dict['ACNTS_LAST_TRAN_DATE']:
            days_since = (pd.Timestamp.now() - pd.to_datetime(client_dict['ACNTS_LAST_TRAN_DATE'])).days
            churn_factors['time_based'] = min(100, (days_since / 180) * 100)
        else:
            churn_factors['time_based'] = 100
            
        # Digital engagement factor
        digital_score = (
            (client_dict['ACNTS_ATM_OPERN'] or 0) / 20 +
            (client_dict['ACNTS_INET_OPERN'] or 0) / 30 * 1.5 +
            (client_dict['ACNTS_SMS_OPERN'] or 0) / 25 * 1.2
        ) / 3.7 * 100
        churn_factors['digital_engagement'] = max(0, 100 - digital_score)
        
        # Product relationships factor
        product_score = 0
        if client_dict['ACNTS_SALARY_ACNT'] == 1:
            product_score += 50
        if client_dict['ACNTS_CR_CARDS_ALLOWED'] == 1:
            product_score += 50
        churn_factors['product_relationships'] = 100 - product_score
        
        # Account status factor
        churn_factors['account_status'] = 100 if client_dict['ACNTS_DORMANT_ACNT'] == 1 else 0
    else:
        churn_factors = {
            'time_based': 40,
            'digital_engagement': 30,
            'product_relationships': 20,
            'account_status': 10
        }
    
    metrics = {
        'avg_clv': 1500.00,
        'clv_cac_ratio': 3.5,
        'retention_rate': 85.5,
        'predicted_growth': 12.3,
        'churn_rate': 5.2,
        'churn_prediction': 'Low Risk',
        'clv_trend': 'Upward',
        'cac_breakdown': 'Marketing: 60%, Sales: 40%',
        'revenue_per_customer': 250.00,
        'cac_digital_ads': 120.00,
        'cac_content': 80.00,
        'cac_social': 60.00,
        'cac_sales_team': 150.00,
        'cac_support': 90.00,
        'cac_tools': 50.00
    }
    
    revenue_metrics = {
        'net_total_revenue': 1000000.00,
        'total_revenue': 1200000.00,
        'total_assets': 500000.00,
        'total_expenses': 400000.00,
        'total_liabilities': 300000.00,
        'net_revenue': 800000.00,
        'grand_total_revenue': 1700000.00,
        'contributing_clients': 150
    }
    
    chart_data = {
        'segments': {
            'labels': ['Premium', 'Standard', 'Basic', 'Trial'],
            'data': [30, 45, 15, 10],
            'profitability': [2500, 1200, 500, 100],  # Average revenue per customer in each segment
            'growth_rate': [15, 8, 5, 20],  # Growth rate percentage for each segment
            'descriptions': [
                'High-value accounts with multiple products',
                'Regular customers with stable engagement',
                'Single product customers',
                'New customers in evaluation period'
            ]
        },
        'churn_factors': {
            'labels': [
                'Time Since Last Transaction',
                'Digital Engagement Level',
                'Product Relationship Strength',
                'Account Activity Status'
            ],
            'data': [
                churn_factors['time_based'],
                churn_factors['digital_engagement'],
                churn_factors['product_relationships'],
                churn_factors['account_status']
            ],
            'descriptions': [
                'Based on days since last transaction',
                'Based on ATM, Internet, and SMS usage',
                'Based on salary account and credit cards',
                'Based on account dormancy status'
            ]
        },
        'cac_breakdown': {
            'labels': [
                'Digital Advertising',
                'Content Marketing',
                'Social Media',
                'Sales Team',
                'Customer Support',
                'Tools & Software'
            ],
            'data': [120.00, 80.00, 60.00, 150.00, 90.00, 50.00]
        },
        'revenue': {
            'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
            'data': [1000, 1200, 1100, 1400, 1300, 1500]
        },
        'channels': {
            'labels': ['Direct', 'Referral', 'Social', 'Email'],
            'data': [2000, 1500, 1000, 1200]
        },
        'retention': {
            'labels': ['Week 1', 'Week 2', 'Week 3', 'Week 4'],
            'data': [100, 90, 85, 82]
        }
    }
    
    chart_data_json = json.dumps(chart_data)
    return render_template('dashboard.html', 
                         metrics=metrics,
                         revenue_metrics=revenue_metrics,
                         chart_data=chart_data,
                         chart_data_json=chart_data_json)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        if username and password:
            session.clear()
            session['user'] = username
            session['authenticated'] = True
            session.permanent = True
            return redirect(url_for('landing_page'))
        else:
            return render_template('login.html', error="Please enter both username and password")
            
    elif 'user' in session and session.get('authenticated'):
        return redirect(url_for('landing_page'))
        
    return render_template('login.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
