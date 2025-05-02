from flask import Flask, render_template, request, redirect, url_for, session, jsonify, Response, send_from_directory, make_response
from num2words import num2words
from flask_caching import Cache
from functools import wraps
from datetime import datetime, timedelta
import os
import sqlite3
import json
import random
import pandas as pd
import pdfkit
from churn_predictor import calculate_churn_probability, load_data, prepare_features
from transaction_analyzer import TransactionPatternAnalyzer
from recommendation_engine import RecommendationEngine
from product_analytics_fixed import analyze_product_lifecycle, calculate_product_performance, get_revenue_by_segment, get_bundling_recommendations

# Initialize ML components
transaction_analyzer = TransactionPatternAnalyzer()
recommendation_engine = RecommendationEngine()

app = Flask(__name__, 
           template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), 'templates')),
           static_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), 'static')),
           static_url_path='/static')

# Session configuration
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = os.path.join(os.path.dirname(__file__), 'flask_session')
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.secret_key = 'dev_secret_key_123'  # Change this in production

# Cache configuration
app.config['CACHE_TYPE'] = 'SimpleCache'
app.config['CACHE_DEFAULT_TIMEOUT'] = 300  # 5 minutes

# Initialize cache
cache = Cache(app)

# Initialize Flask-Session
from flask_session import Session
Session(app)

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

def number_to_words(number):
    """Convert a number to words and format for display"""
    if number > 100000:
        try:
            words = num2words(int(number))
            return f"[{words}]"
        except:
            return ""
    return ""

def get_db():
    """Get database connection with optimized settings"""
    conn = None
    try:
        # Get absolute path to database
        current_dir = os.getcwd()
        db_path = os.path.join(current_dir, 'banking_data.db')
        print(f"\nAttempting to connect to database at: {db_path}")
        
        if not os.path.exists(db_path):
            print(f"Database file not found at: {db_path}")
            return None
                
        # Connect to database
        conn = sqlite3.connect(db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        
        # Set PRAGMA statements first for better performance
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA cache_size=-2000')
            conn.execute('PRAGMA temp_store=MEMORY')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA mmap_size=2147483648')
            conn.commit()
            print("PRAGMA statements executed successfully")
        except sqlite3.Error as e:
            print(f"Error setting PRAGMA statements: {str(e)}")
            if conn:
                conn.close()
            return None
        
        # Check database tables
        cursor = conn.cursor()
        
        # Get list of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print(f"Found tables: {[table[0] for table in tables]}")
        
        # Check if required tables exist
        required_tables = ['clients', 'data_quality_issues']
        for table in required_tables:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
            if not cursor.fetchone():
                print(f"{table} table does not exist in the database")
                conn.close()
                return None
            print(f"{table} table found")
            
            # Get record count for each table
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"Found {count} records in {table} table")
        
        # Create indexes for better performance
        conn.execute('CREATE INDEX IF NOT EXISTS idx_client_num ON clients(ACNTS_CLIENT_NUM)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_opening_date ON clients(ACNTS_OPENING_DATE)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_last_tran ON clients(ACNTS_LAST_TRAN_DATE)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_prod_code ON clients(ACNTS_PROD_CODE)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_dormant ON clients(ACNTS_DORMANT_ACNT)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_inop ON clients(ACNTS_INOP_ACNT)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_ac_name ON clients(ACNTS_AC_NAME1)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON clients(ACNTS_DORMANT_ACNT, ACNTS_INOP_ACNT)')
        conn.commit()
        
        return conn
        
    except Exception as e:
        print(f"Database error: {str(e)}")
        if conn:
            conn.close()
        return None

@app.route('/')
@login_required
def index():
    return redirect(url_for('dashboard'))

@app.route('/api/transaction-patterns/<client_id>')
@login_required
def get_transaction_patterns(client_id):
    try:
        # Load transaction data for client
        df = transaction_analyzer.load_transactions()
        client_df = df[df['client_id'] == client_id]
        
        if len(client_df) == 0:
            return jsonify({'error': 'No transactions found for client'}), 404
            
        # Calculate metrics
        metrics_df = transaction_analyzer.calculate_client_metrics(client_df)
        
        # Get segment analysis
        segment_analysis, segments = transaction_analyzer.identify_customer_segments(metrics_df)
        
        # Detect anomalies
        anomalies = transaction_analyzer.detect_anomalies(metrics_df)
        
        # Analyze trends
        trends = transaction_analyzer.analyze_trends(client_df)
        
        # Generate insights
        insights = transaction_analyzer.generate_insights(client_df, metrics_df, segments, anomalies)
        
        return jsonify({
            'segment_analysis': segment_analysis.to_dict('records'),
            'anomalies_detected': bool(anomalies.any()),
            'trends': trends.to_dict('records'),
            'insights': insights.to_dict('records')
        })
        
    except Exception as e:
        print(f"Error analyzing transaction patterns: {str(e)}")
        return jsonify({'error': 'Failed to analyze transaction patterns'}), 500

@app.route('/api/recommendations/<client_id>')
@login_required
def get_recommendations(client_id):
    try:
        # Load data if not already loaded
        recommendation_engine.load_data()
        
        # Get recommendations
        recommendations = recommendation_engine.get_product_recommendations(client_id)
        
        # Get cross-selling opportunities
        opportunities = recommendation_engine.get_cross_selling_opportunities(client_id)
        
        return jsonify({
            'recommendations': recommendations.to_dict('records'),
            'cross_selling_opportunities': opportunities.to_dict('records')
        })
        
    except Exception as e:
        print(f"Error generating recommendations: {str(e)}")
        return jsonify({'error': 'Failed to generate recommendations'}), 500

@app.route('/api/loans')
@login_required
def get_loans():
    try:
        conn = get_db()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        view_type = request.args.get('view', 'individual')
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        currency_filter = request.args.get('currency', None)
        
        # Base query depending on view type, using actual loan data from the database
        if view_type == 'individual':
            # Get only the latest transaction for each account using a subquery
            query = """
                WITH LatestLoans AS (
                SELECT 
                        li.acc_num,
                        MAX(li.origination_date) as latest_date
                    FROM loan_info li
                    GROUP BY li.acc_num
                )
                SELECT 
                    li.proj_id as id,
                    li.ACC_NUM as account_number,
                    li.ACC_NUM as acc_num,
                    li.loan_type,
                    li.original_amount,
                    lb.outstanding_balance,
                    li.interest_rate,
                    li.start_date,
                    li.maturity_date,
                    li.origination_date,
                    li.monthly_payment,
                    lp.payment_count,
                    li.currency,
                    CASE 
                        WHEN lb.days_past_due > 90 THEN 'Defaulted'
                        WHEN lb.days_past_due > 30 THEN 'Late'
                        WHEN lb.days_past_due > 0 THEN 'Overdue'
                        ELSE 'Current'
                    END as status
                FROM loan_info li
                JOIN LatestLoans ll ON li.acc_num = ll.acc_num AND li.origination_date = ll.latest_date
                LEFT JOIN loan_balance lb ON li.proj_id = lb.proj_id 
                    AND li.loan_type = lb.loan_type 
                    AND li.origination_date = lb.origination_date
                LEFT JOIN loan_payments lp ON li.proj_id = lp.proj_id 
                    AND li.loan_type = lp.loan_type 
                    AND li.origination_date = lp.origination_date
            """
            
            # Add currency filter if specified
            if currency_filter:
                query += f" WHERE li.currency = '{currency_filter}'"
                
        else:  # aggregated view
            query = """
                SELECT 
                    li.loan_type,
                    COUNT(*) as total_loans,
                    SUM(li.original_amount) as total_original_amount,
                    SUM(lb.outstanding_balance) as total_outstanding,
                    AVG(li.interest_rate) as avg_interest_rate,
                    COUNT(CASE WHEN lb.days_past_due = 0 THEN 1 END) as active_loans,
                    COUNT(CASE WHEN lb.days_past_due > 0 THEN 1 END) as inactive_loans
                FROM loan_info li
                LEFT JOIN loan_balance lb ON li.proj_id = lb.proj_id 
                    AND li.loan_type = lb.loan_type 
                    AND li.origination_date = lb.origination_date
            """
            
            # Add currency filter if specified
            if currency_filter:
                query += f" WHERE li.currency = '{currency_filter}'"
                
            query += " GROUP BY li.loan_type"
        
        # Get total count with the same filters
        count_query = f"SELECT COUNT(*) FROM ({query})"
        cursor.execute(count_query)
        total_count = cursor.fetchone()[0]
        total_pages = (total_count + per_page - 1) // per_page
        
        # Add pagination
        offset = (page - 1) * per_page
        query += f" LIMIT {per_page} OFFSET {offset}"
        
        # Execute query
        cursor.execute(query)
        loans = [dict(row) for row in cursor.fetchall()]
        
        # For debugging, print the number of loans returned
        print(f"Loans query returned {len(loans)} results")
        
        return jsonify({
            'loans': loans,
            'current_page': page,
            'per_page': per_page,
            'total_pages': total_pages,
            'total_count': total_count
        })
        
    except Exception as e:
        print(f"Error fetching loan data: {str(e)}")
        return jsonify({'error': 'Failed to fetch loan data'}), 500

@app.route('/api/loan-payments/<proj_id>/<loan_type>/<origination_date>')
@login_required
def get_loan_payments(proj_id, loan_type, origination_date):
    try:
        conn = get_db()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Get loan info to calculate payment details
        cursor.execute("""
            SELECT 
                li.monthly_payment, 
                li.interest_rate, 
                li.start_date, 
                li.original_amount,
                lp.payment_count,
                li.currency
            FROM loan_info li
            LEFT JOIN loan_payments lp ON li.proj_id = lp.proj_id 
                AND li.loan_type = lp.loan_type 
                AND li.origination_date = lp.origination_date
            WHERE li.proj_id = ? AND li.loan_type = ? AND li.origination_date = ?
        """, (proj_id, loan_type, origination_date))
        
        loan_details = cursor.fetchone()
        
        # Generate synthetic payment history
        payments = []
        
        if loan_details:
            # Convert to dict for easier access
            loan_details = dict(loan_details)
            
            # Get values with defaults
            monthly_payment = loan_details.get('monthly_payment', 0) or 0
            interest_rate = loan_details.get('interest_rate', 0) or 0
            payment_count = loan_details.get('payment_count', 0) or 0
            original_amount = loan_details.get('original_amount', 0) or 0
            currency = loan_details.get('currency', 'USD') or 'USD'
            
            # Calculate principal and interest portions
            monthly_interest_rate = interest_rate / 100 / 12
            interest_portion = original_amount * monthly_interest_rate
            principal_portion = monthly_payment - interest_portion
            
            # Generate payment history
            from datetime import datetime, timedelta
            
            start_date = None
            if loan_details.get('start_date'):
                try:
                    start_date = datetime.strptime(loan_details['start_date'], '%Y-%m-%d')
                except:
                    # Use current date if start_date cannot be parsed
                    start_date = datetime.now() - timedelta(days=30 * payment_count)
            else:
                # Use current date if no start_date
                start_date = datetime.now() - timedelta(days=30 * payment_count)
            
            # Generate payment history
            for i in range(payment_count):
                payment_date = start_date + timedelta(days=30 * i)
                
                # Adjust principal/interest ratio for each payment
                remaining_balance = original_amount - (principal_portion * i)
                if remaining_balance < 0:
                    remaining_balance = 0
                
                current_interest = remaining_balance * monthly_interest_rate
                current_principal = monthly_payment - current_interest
                
                # Ensure we don't go negative
                if current_principal < 0:
                    current_principal = 0
                
                # Determine payment status
                status = 'Completed'
                
                payments.append({
                    'date': payment_date.strftime('%Y-%m-%d'),
                    'amount': float(monthly_payment),
                    'principal': float(current_principal),
                    'interest': float(current_interest),
                    'status': status,
                    'currency': currency
                })
        
        # Sort payments by date (most recent first)
        payments.sort(key=lambda x: x['date'], reverse=True)
        
        return jsonify({'payments': payments})
        
    except Exception as e:
        print(f"Error fetching loan payments: {str(e)}")
        return jsonify({'error': 'Failed to fetch payment history'}), 500

@app.route('/api/loan-metrics')
@login_required
def get_loan_metrics():
    try:
        conn = get_db()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Calculate loan metrics from actual data
        
        # 1. Get total loans, average interest rate, and maturity dates
        cursor.execute("""
            SELECT 
                COUNT(*) as total_loans,
                AVG(interest_rate) as avg_interest_rate,
                MIN(maturity_date) as earliest_maturity,
                MAX(maturity_date) as latest_maturity,
                GROUP_CONCAT(DISTINCT currency) as currencies
            FROM loan_info
        """)
        
        metrics = dict(cursor.fetchone() or {})
        
        # Set default values if not present
        metrics.setdefault('total_loans', 0)
        metrics.setdefault('avg_interest_rate', 0)
        metrics.setdefault('earliest_maturity', 'N/A')
        metrics.setdefault('latest_maturity', 'N/A')
        metrics.setdefault('currencies', 'USD')
        
        # 2. Get total outstanding and original amounts
        cursor.execute("""
            SELECT 
                SUM(li.original_amount) as total_original,
                SUM(lb.outstanding_balance) as total_outstanding,
                li.currency
            FROM loan_info li
            LEFT JOIN loan_balance lb ON li.proj_id = lb.proj_id 
                AND li.loan_type = lb.loan_type 
                AND li.origination_date = lb.origination_date
            GROUP BY li.currency
        """)
        
        amounts_by_currency = {}
        for row in cursor.fetchall():
            row_dict = dict(row)
            currency = row_dict.get('currency', 'USD') or 'USD'
            amounts_by_currency[currency] = {
                'total_original': row_dict.get('total_original', 0) or 0,
                'total_outstanding': row_dict.get('total_outstanding', 0) or 0
            }
        
        # Use the first currency if amounts_by_currency is empty
        if not amounts_by_currency:
            default_currency = metrics['currencies'].split(',')[0] if metrics['currencies'] else 'USD'
            amounts_by_currency[default_currency] = {
                'total_original': 0,
                'total_outstanding': 0
            }
        
        # Add amounts to metrics
        metrics['amounts_by_currency'] = amounts_by_currency
        
        # 3. Get delinquency metrics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_loans,
                COUNT(CASE WHEN days_past_due > 0 THEN 1 END) as delinquent_loans,
                AVG(days_past_due) as avg_days_past_due,
                SUM(outstanding_balance) as total_outstanding
            FROM loan_balance
        """)
        delinquency_summary = cursor.fetchone()
        
        # 4. Get payment metrics
        cursor.execute("""
            SELECT 
                SUM(payment_count) as total_payments
            FROM loan_payments
        """)
        payment_summary = cursor.fetchone()
        
        # Convert to dictionaries
        delinquency_summary = dict(delinquency_summary) if delinquency_summary else {}
        payment_summary = dict(payment_summary) if payment_summary else {}
        
        # Calculate additional metrics
        
        # Get values with defaults
        total_loans = metrics['total_loans'] or 0
        avg_interest_rate = metrics['avg_interest_rate'] or 0
        total_loan_amount = metrics['total_original'] or 0
        
        total_outstanding = delinquency_summary.get('total_outstanding', 0) or 0
        delinquent_loans = delinquency_summary.get('delinquent_loans', 0) or 0
        
        total_payments = payment_summary.get('total_payments', 0) or 0
        
        # Calculate interest metrics
        avg_loan_term_months = 60  # Assume 5-year loans on average
        monthly_interest_rate = avg_interest_rate / 100 / 12
        
        # Calculate total expected interest over life of loans
        expected_interest = total_loan_amount * (1 + monthly_interest_rate) ** avg_loan_term_months - total_loan_amount
        
        # Calculate total interest paid based on payments and outstanding balance
        total_interest_paid = expected_interest * (1 - total_outstanding / total_loan_amount)
        
        # Calculate risk metrics
        default_risk_score = (delinquent_loans / total_loans * 100) if total_loans > 0 else 0
        prepayment_risk_score = 15.0  # Fixed estimate
        
        # Calculate profitability metrics
        loan_profitability = total_interest_paid * 0.8  # Assume 80% of interest is profit
        clv_contribution = (loan_profitability / total_loan_amount * 100) if total_loan_amount > 0 else 0
        
        # Calculate performance metrics
        delinquency_rate = (delinquent_loans / total_loans * 100) if total_loans > 0 else 0
        cross_sell_score = 8.0  # Fixed estimate
        
        # Calculate time metrics
        from datetime import datetime
        
        # Parse earliest and latest maturity dates
        earliest_maturity = metrics['earliest_maturity']
        latest_maturity = metrics['latest_maturity']
        
        # Calculate average time remaining in months
        avg_time_remaining = 0
        if earliest_maturity:
            try:
                earliest_date = datetime.strptime(earliest_maturity, '%Y-%m-%d')
                now = datetime.now()
                months_remaining = (earliest_date.year - now.year) * 12 + earliest_date.month - now.month
                avg_time_remaining = max(0, months_remaining)
            except:
                avg_time_remaining = 36  # Default value
        
        # Format metrics for response
        metrics = {
            'total_interest_paid': round(float(total_interest_paid), 2),
            'expected_interest': round(float(expected_interest), 2),
            'prepayment_risk_score': round(float(prepayment_risk_score), 1),
            'default_risk_score': round(float(default_risk_score), 1),
            'loan_profitability': round(float(loan_profitability), 2),
            'clv_contribution': round(float(clv_contribution), 1),
            'delinquency_rate': round(float(delinquency_rate), 2),
            'cross_sell_score': round(float(cross_sell_score), 1),
            'avg_time_remaining': int(avg_time_remaining),
            'earliest_maturity': earliest_maturity or '2023-12-15',
            'latest_maturity': latest_maturity or '2028-06-30'
        }
        
        return jsonify(metrics)
        
    except Exception as e:
        print(f"Error fetching loan metrics: {str(e)}")
        # Return default metrics as fallback
        default_metrics = {
            'total_interest_paid': 125000.00,
            'expected_interest': 350000.00,
            'prepayment_risk_score': 18.5,
            'default_risk_score': 7.2,
            'loan_profitability': 215000.00,
            'clv_contribution': 35.8,
            'delinquency_rate': 3.45,
            'cross_sell_score': 8.7,
            'avg_time_remaining': 36,
            'earliest_maturity': '2023-12-15',
            'latest_maturity': '2028-06-30'
        }
        return jsonify(default_metrics)

@app.route('/client/<client_id>')
@login_required
def client_details(client_id):
    conn = get_db()
    if conn is None:
        return render_template('error.html', 
                            error="Database connection failed", 
                            message="Could not connect to the database. Please try again later."), 500
        
    cursor = conn.cursor()
    
    # Get client basic info with parameterized query
    cursor.execute("""
        SELECT * FROM clients 
        WHERE ACNTS_CLIENT_NUM = ?
    """, (client_id,))
    
    client_row = cursor.fetchone()
    if client_row is None:
        return render_template('error.html',
                            error="Client not found",
                            message=f"Could not find client with ID {client_id}"), 404
        
    client = dict(client_row)
    
    # Calculate churn factors for this client
    churn_factors = {
        'time_based': 0,
        'digital_engagement': 0,
        'product_relationships': 0,
        'account_status': 0
    }
    
    # Time-based factor
    if client['ACNTS_LAST_TRAN_DATE']:
        days_since = (pd.Timestamp.now() - pd.to_datetime(client['ACNTS_LAST_TRAN_DATE'])).days
        churn_factors['time_based'] = min(100, (days_since / 180) * 100)
    else:
        churn_factors['time_based'] = 100
        
    # Digital engagement factor
    digital_score = (
        (client['ACNTS_ATM_OPERN'] or 0) / 20 +
        (client['ACNTS_INET_OPERN'] or 0) / 30 * 1.5 +
        (client['ACNTS_SMS_OPERN'] or 0) / 25 * 1.2
    ) / 3.7 * 100
    churn_factors['digital_engagement'] = max(0, 100 - digital_score)
    
    # Product relationships factor
    product_score = 0
    if client['ACNTS_SALARY_ACNT'] == 1:
        product_score += 50
    if client['ACNTS_CR_CARDS_ALLOWED'] == 1:
        product_score += 50
    churn_factors['product_relationships'] = 100 - product_score
    
    # Account status factor
    churn_factors['account_status'] = 100 if client['ACNTS_DORMANT_ACNT'] == 1 else 0

    # Calculate CLV values based on client segment and activity
    base_clv = 1000  # Default base value
    if client['ACNTS_PROD_CODE'] == 3102:  # Premium
        base_clv = 5000
    elif client['ACNTS_PROD_CODE'] == 3002:  # Business
        base_clv = 3000
    elif client['ACNTS_PROD_CODE'] == 3101:  # Retail
        base_clv = 2000
        
    # Adjust for activity level
    activity_multiplier = 1.0
    if client['ACNTS_ATM_OPERN'] == 1:
        activity_multiplier += 0.4
    if client['ACNTS_INET_OPERN'] == 1:
        activity_multiplier += 0.35
    if client['ACNTS_SMS_OPERN'] == 1:
        activity_multiplier += 0.25
        
    current_clv = base_clv * activity_multiplier
    
    # Calculate health score
    health_score = 100 - (sum(churn_factors.values()) / len(churn_factors))
    if health_score >= 80:
        health_status = "Excellent"
        health_color = "text-green-600"
    elif health_score >= 60:
        health_status = "Good"
        health_color = "text-blue-600"
    elif health_score >= 40:
        health_status = "Fair"
        health_color = "text-yellow-600"
    else:
        health_status = "Poor"
        health_color = "text-red-600"

    # Prepare metrics
    metrics = {
        'current_clv': current_clv,
        'predicted_clv': current_clv * 1.2,  # 20% growth prediction
        'value_trend': 20.0 if client['ACNTS_DORMANT_ACNT'] == 0 else -15.0,
        'health_score': f"{health_score:.0f}",
        'health_status': health_status,
        'health_color': health_color,
        'activity': {
            'digital_usage': {
                'last_30_days': random.randint(5, 30),
                'channels': {
                    'ATM': 40 if client['ACNTS_ATM_OPERN'] == 1 else 0,
                    'Internet': 35 if client['ACNTS_INET_OPERN'] == 1 else 0,
                    'SMS': 25 if client['ACNTS_SMS_OPERN'] == 1 else 0
                },
                'channels_data': {
                    'ATM': {'active': client['ACNTS_ATM_OPERN'] == 1, 'activities': range(random.randint(1, 10))},
                    'Internet': {'active': client['ACNTS_INET_OPERN'] == 1, 'activities': range(random.randint(1, 15))},
                    'SMS': {'active': client['ACNTS_SMS_OPERN'] == 1, 'activities': range(random.randint(1, 8))}
                }
            },
            'product_engagement': {
                'total_products': sum([
                    client['ACNTS_ATM_OPERN'] or 0,
                    client['ACNTS_INET_OPERN'] or 0,
                    client['ACNTS_SMS_OPERN'] or 0,
                    client['ACNTS_SALARY_ACNT'] or 0,
                    client['ACNTS_CR_CARDS_ALLOWED'] or 0
                ])
            },
            'transaction_activity': {
                'has_recent_activity': client['ACNTS_LAST_TRAN_DATE'] is not None,
                'days_since_last': (pd.Timestamp.now() - pd.to_datetime(client['ACNTS_LAST_TRAN_DATE'])).days if client['ACNTS_LAST_TRAN_DATE'] else None
            }
        }
    }

    # Prepare chart data
    chart_data = {
        'clv_trend': {
            'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
            'historical': [current_clv * 0.8, current_clv * 0.85, current_clv * 0.9, current_clv * 0.95, current_clv, current_clv],
            'predicted': [current_clv, current_clv * 1.05, current_clv * 1.1, current_clv * 1.15, current_clv * 1.2, current_clv * 1.25]
        },
        'product_usage': {
            'labels': ['ATM', 'Internet', 'SMS', 'Salary', 'Credit Card'],
            'data': [
                random.randint(5, 15) if client['ACNTS_ATM_OPERN'] == 1 else 0,
                random.randint(8, 20) if client['ACNTS_INET_OPERN'] == 1 else 0,
                random.randint(3, 12) if client['ACNTS_SMS_OPERN'] == 1 else 0,
                1 if client['ACNTS_SALARY_ACNT'] == 1 else 0,
                random.randint(2, 8) if client['ACNTS_CR_CARDS_ALLOWED'] == 1 else 0
            ]
        },
        'loyalty': {
            'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
            'points': [random.randint(100, 500) for _ in range(6)]
        },
        'risk_factors': {
            'labels': ['Transaction Frequency', 'Digital Engagement', 'Product Utilization'],
            'data': [churn_factors['time_based'], churn_factors['digital_engagement'], churn_factors['product_relationships']]
        }
    }

    # Prepare retention actions
    retention_actions = [
        {
            'type': 'Product Recommendation',
            'description': 'Suggest credit card upgrade based on spending pattern',
            'status': 'pending',
            'date': '2025-05-15'
        },
        {
            'type': 'Digital Channel Activation',
            'description': 'Enable SMS banking for instant notifications',
            'status': 'completed',
            'date': '2025-04-20'
        },
        {
            'type': 'Loyalty Program',
            'description': 'Enroll in premium rewards program',
            'status': 'scheduled',
            'date': '2025-05-01'
        }
    ]

    # Prepare risk factors detail
    risk_factors = [
        {
            'name': 'Transaction Pattern',
            'impact': -15.5 if churn_factors['time_based'] > 50 else 10.5,
            'description': 'Based on frequency and value of transactions'
        },
        {
            'name': 'Digital Engagement',
            'impact': -12.3 if churn_factors['digital_engagement'] > 50 else 8.7,
            'description': 'Based on usage of digital banking channels'
        },
        {
            'name': 'Product Usage',
            'impact': -18.9 if churn_factors['product_relationships'] > 50 else 15.2,
            'description': 'Based on number and type of products used'
        }
    ]

    # Convert chart data to JSON for JavaScript
    chart_data_json = json.dumps(chart_data)
    
    return render_template('client_details.html',
                         client=client,
                         metrics=metrics,
                         chart_data=chart_data,
                         chart_data_json=chart_data_json,
                         retention_actions=retention_actions,
                         risk_factors=risk_factors)
    

@app.route('/clients')
@login_required
def clients():
    try:
        conn = get_db()
        if conn is None:
            return render_template('error.html',
                                error="Database connection failed",
                                message="Could not connect to the database. Please try again later."), 500

        cursor = conn.cursor()

        # Get query parameters for filtering and pagination
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('limit', 10, type=int)
        status = request.args.get('status', '')
        branch = request.args.get('branch', '')
        segment = request.args.get('segment', '')
        search = request.args.get('search', '')

        # Build the base query with proper table joins - updated to use only clients table
        query = """
            SELECT 
                c.ACNTS_CLIENT_NUM as id,
                COALESCE(c.ACNTS_AC_NAME1, '') || ' ' || COALESCE(c.ACNTS_AC_NAME2, '') as name,
                CASE 
                    WHEN c.ACNTS_PROD_CODE = 3102 THEN 'Premium'
                    WHEN c.ACNTS_PROD_CODE = 3002 THEN 'Business'
                    WHEN c.ACNTS_PROD_CODE = 3101 THEN 'Retail'
                    ELSE 'Other'
                END as segment,
                c.ACNTS_LAST_TRAN_DATE,
                CASE 
                    WHEN c.ACNTS_DORMANT_ACNT = 1 THEN 'Inactive'
                    WHEN c.ACNTS_INOP_ACNT = 1 THEN 'At Risk'
                    ELSE 'Active'
                END as status,
                c.ACNTS_BRN_CODE as branch,
                (CASE 
                    WHEN c.ACNTS_PROD_CODE = 3102 THEN 5000
                    WHEN c.ACNTS_PROD_CODE = 3002 THEN 3000
                    WHEN c.ACNTS_PROD_CODE = 3101 THEN 1000
                    ELSE 1000
                END) * (1 + 
                    CASE WHEN c.ACNTS_ATM_OPERN = 1 THEN 0.4 ELSE 0 END +
                    CASE WHEN c.ACNTS_INET_OPERN = 1 THEN 0.35 ELSE 0 END +
                    CASE WHEN c.ACNTS_SMS_OPERN = 1 THEN 0.25 ELSE 0 END
                ) as clv
            FROM clients c
            WHERE 1=1
        """
        params = []

        # Add filters
        if status:
            if status == 'Active':
                query += " AND c.ACNTS_DORMANT_ACNT = 0 AND c.ACNTS_INOP_ACNT = 0"
            elif status == 'Inactive':
                query += " AND c.ACNTS_DORMANT_ACNT = 1"
            elif status == 'At Risk':
                query += " AND c.ACNTS_INOP_ACNT = 1"

        if branch:
            query += " AND c.ACNTS_BRN_CODE = ?"
            params.append(branch)
            
        # Add segment filter
        if segment:
            if segment == 'Premium':
                query += " AND c.ACNTS_PROD_CODE = 3102"
            elif segment == 'Business':
                query += " AND c.ACNTS_PROD_CODE = 3002"
            elif segment == 'Retail':
                query += " AND c.ACNTS_PROD_CODE = 3101"

        if search:
            query += """ AND (
                c.ACNTS_CLIENT_NUM LIKE ? OR 
                c.ACNTS_AC_NAME1 LIKE ? OR 
                c.ACNTS_AC_NAME2 LIKE ?
            )"""
            search_term = f"%{search}%"
            params.extend([search_term, search_term, search_term])

        # Get total count for pagination
        count_query = f"SELECT COUNT(*) FROM ({query})"
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        total_pages = (total_count + per_page - 1) // per_page

        # Add pagination and ordering
        query += " ORDER BY c.ACNTS_CLIENT_NUM"
        query += f" LIMIT {per_page} OFFSET {(page - 1) * per_page}"
        
        # Execute final query
        cursor.execute(query, params)
        clients = [dict(row) for row in cursor.fetchall()]

        return render_template('clients.html',
                            clients=clients,
                            total_count=total_count,
                            current_page=page,
                            total_pages=total_pages,
                            per_page=per_page,
                            status=status,
                            branch=branch,
                            segment=segment,
                            search=search)
                            
    except Exception as e:
        print(f"Error loading clients: {str(e)}")
        return render_template('error.html',
                            error="Failed to Load Clients",
                            message="An error occurred while loading the client list. Please try again later."), 500

@app.route('/products')
@login_required
@cache.cached(timeout=300)  # Cache this view for 5 minutes
def products():
    try:
        conn = get_db()
        if conn is None:
            return render_template('error.html',
                                error="Database connection failed",
                                message="Could not connect to the database. Please try again later."), 500
                                
        cursor = conn.cursor()

        # Get query parameters for filtering and pagination
        search = request.args.get('search', '')
        limit = request.args.get('limit', None, type=int)

        # Build base query without the charges table
        query = """
            SELECT p.*
            FROM products p
            WHERE 1=1
        """
        params = []

        # Add search filter
        if search:
            query += """ AND (
                p.PRODUCT_CODE LIKE ? OR 
                p.PRODUCT_NAME LIKE ? OR
                p.PRODUCT_GROUP_CODE LIKE ? OR
                p.PRODUCT_CLASS LIKE ?
            )"""
            search_term = f"%{search}%"
            params.extend([search_term] * 4)

        # Add order by for consistent results
        query += " ORDER BY p.PRODUCT_NAME"

        # Add limit if specified
        if limit:
            query += f" LIMIT {limit}"

        # Execute query with timeout and fetch all at once for better performance
        cursor.execute("PRAGMA query_only = ON")  # Read-only optimization
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        products = []
        
        # Process each product
        for row in rows:
            product = dict(row)
            try:
                # Get lifecycle stage for each product - passing connection
                lifecycle_info = analyze_product_lifecycle(product['PRODUCT_CODE'], conn)
                product['lifecycle_stage'] = next((stage['name'] for stage in lifecycle_info if stage['current']), 'Introduction')
            except Exception as e:
                print(f"Error analyzing lifecycle for product {product['PRODUCT_CODE']}: {str(e)}")
                product['lifecycle_stage'] = 'Unknown'
                
            # Add simulated charge info instead of using charges table
                product['charge'] = {
                'CHARGES_FIXED_AMT': product['PRODUCT_CODE'] % 100 + 10,  # Simulated charge amount
                'CHARGES_CHG_AMT_CHOICE': 1,  # Fixed amount
                'CHARGES_CHGS_PERCENTAGE': 0,
                'CHARGES_CHG_CURR': 'USD'
                }
            products.append(product)

        # Get analysis data with the same connection to avoid multiple connections
        try:
            # Create metrics table if needed (safely)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    metric_key TEXT PRIMARY KEY,
                    metric_value TEXT,
                    segment_distribution TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            
            # Get cached performance metrics or use simulated data
            cursor.execute("""
                SELECT metric_value FROM performance_metrics 
                WHERE metric_key = 'overall_performance'
                AND last_updated > datetime('now', '-1 hour')
            """)
            cached_performance = cursor.fetchone()
            
            if cached_performance:
                performance = json.loads(cached_performance['metric_value'])
            else:
                # Simulated performance metrics
                performance = {
                    'total_sales': 15000,
                    'avg_clv': 2500,
                    'revenue': 750000,
                    'monthly_sales': [12000, 15000, 10000, 18000, 16000, 14000]
                }
        except Exception as e:
            print(f"Error calculating product performance: {str(e)}")
            performance = {'total_sales': 0, 'avg_clv': 0, 'revenue': 0}
            
        try:
            # Using simulated segment data
            revenue_by_segment = [
                {'name': 'Premium', 'revenue': 350000, 'count': 120},
                {'name': 'Business', 'revenue': 250000, 'count': 200},
                {'name': 'Retail', 'revenue': 150000, 'count': 300}
            ]
        except Exception as e:
            print(f"Error getting revenue by segment: {str(e)}")
            revenue_by_segment = []
            
        try:
            # Using simulated bundling recommendations
            bundling_recommendations = [
                {'name': 'Savings + Credit Card', 'revenue_increase': 25, 'products': ['Savings Account', 'Gold Credit Card']},
                {'name': 'Business + Overdraft', 'revenue_increase': 18, 'products': ['Business Account', 'Overdraft Protection']},
                {'name': 'Personal + Mortgage', 'revenue_increase': 35, 'products': ['Personal Account', 'Home Mortgage']}
            ]
        except Exception as e:
            print(f"Error getting bundling recommendations: {str(e)}")
            bundling_recommendations = []

        # Create analysis data structure
        analysis = {
                               'total_sales': performance['total_sales'],
                               'avg_clv': performance['avg_clv'],
                               'revenue': performance['revenue'],
                               'segment_distribution': revenue_by_segment,
                               'bundle_recommendations': bundling_recommendations
        }
        
        # Create chart data for the template
        chart_data = {
            'sales': {
                'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
                'data': performance.get('monthly_sales', [12000, 15000, 10000, 18000, 16000, 14000])
            },
            'segments': {
                'labels': [segment['name'] for segment in revenue_by_segment],
                'data': [segment['revenue'] for segment in revenue_by_segment]
            },
            'bundle_recommendations': bundling_recommendations
        }
            
        conn.close()

        return render_template('products.html',
                           products=products,
                           search=search,
                           limit=limit,
                           analysis=analysis,
                           analysis_json=json.dumps(chart_data))
                           
    except Exception as e:
        print(f"Error loading products: {str(e)}")
        return render_template('error.html',
                           error="Failed to Load Products",
                           message="An error occurred while loading the product list. Please try again later."), 500

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        conn = get_db()
        if not conn:
            return render_template('error.html',
                                error="Database Connection Failed",
                                message="Could not connect to the database. Please try again later."), 500
        
        try:
            # For demo purposes - you should implement proper authentication
            if username and password:  # Add your actual authentication logic here
                session.permanent = True
                session['user'] = username
                session['authenticated'] = True
                return redirect(url_for('dashboard'))
                
        except Exception as e:
            print(f"Login error: {str(e)}")
            return render_template('error.html',
                                error="Login Failed",
                                message="An error occurred during login. Please try again."), 500
        finally:
            conn.close()
            
    return render_template('login.html')

@app.route('/dashboard')
@login_required
def dashboard():
    conn = get_db()
    cursor = conn.cursor()

    # Get account statistics with period comparison
    cursor.execute("""
        WITH current_stats AS (
            SELECT 
                COUNT(*) as total_clients,
                SUM(CASE WHEN ACNTS_DORMANT_ACNT = 0 AND ACNTS_INOP_ACNT = 0 THEN 1 ELSE 0 END) as active_clients,
                SUM(CASE WHEN ACNTS_DORMANT_ACNT = 1 THEN 1 ELSE 0 END) as inactive_clients,
                SUM(CASE WHEN ACNTS_INOP_ACNT = 1 THEN 1 ELSE 0 END) as closed_clients,
                COUNT(DISTINCT ACNTS_BRN_CODE) as total_branches,
                COUNT(DISTINCT CASE WHEN ACNTS_DORMANT_ACNT = 0 THEN ACNTS_BRN_CODE END) as active_branches
            FROM clients
            WHERE ACNTS_OPENING_DATE >= date('now', '-1 month')
        ),
        prev_stats AS (
            SELECT 
                COUNT(*) as prev_total_clients,
                SUM(CASE WHEN ACNTS_DORMANT_ACNT = 0 AND ACNTS_INOP_ACNT = 0 THEN 1 ELSE 0 END) as prev_active_clients,
                COUNT(DISTINCT ACNTS_BRN_CODE) as prev_total_branches,
                COUNT(DISTINCT CASE WHEN ACNTS_DORMANT_ACNT = 0 THEN ACNTS_BRN_CODE END) as prev_active_branches
            FROM clients
            WHERE ACNTS_OPENING_DATE BETWEEN date('now', '-2 month') AND date('now', '-1 month')
        )
        SELECT 
            c.*,
            ROUND(((c.total_clients - p.prev_total_clients) * 100.0 / NULLIF(p.prev_total_clients, 0)), 2) as total_clients_growth,
            ROUND(((c.active_clients - p.prev_active_clients) * 100.0 / NULLIF(p.prev_active_clients, 0)), 2) as active_clients_growth,
            ROUND(((c.total_branches - p.prev_total_branches) * 100.0 / NULLIF(p.prev_total_branches, 0)), 2) as total_branches_growth,
            ROUND(((c.active_branches - p.prev_active_branches) * 100.0 / NULLIF(p.prev_active_branches, 0)), 2) as active_branches_growth
        FROM current_stats c
        CROSS JOIN prev_stats p
    """)
    stats = dict(cursor.fetchone())

    # Get revenue metrics - Using fixed values instead of opening balance
    cursor.execute("""
        WITH current_revenue AS (
            SELECT 
                COUNT(DISTINCT ACNTS_CLIENT_NUM) as contributing_clients,
                COUNT(DISTINCT ACNTS_BRN_CODE) as total_branches,
                COUNT(DISTINCT CASE WHEN ACNTS_DORMANT_ACNT = 0 THEN ACNTS_BRN_CODE END) as active_branches,
                SUM(CASE WHEN ACNTS_ATM_OPERN = 1 THEN 100 ELSE 0 END) as atm_revenue,
                SUM(CASE WHEN ACNTS_INET_OPERN = 1 THEN 150 ELSE 0 END) as internet_revenue,
                SUM(CASE WHEN ACNTS_SMS_OPERN = 1 THEN 50 ELSE 0 END) as sms_revenue
            FROM clients
            WHERE ACNTS_OPENING_DATE >= date('now', '-1 month')
        )
        SELECT 
            contributing_clients,
            total_branches,
            active_branches,
            atm_revenue,
            internet_revenue,
            sms_revenue,
            (atm_revenue + internet_revenue + sms_revenue) as total_revenue,
            contributing_clients as closed_clients,
            contributing_clients as inactive_clients
        FROM current_revenue
    """)
    revenue_metrics = dict(cursor.fetchone())
    
    # Get loan revenue data
    try:
        cursor.execute("""
            SELECT 
                SUM(li.original_amount * li.interest_rate / 100) as loan_revenue,
                SUM(CASE WHEN li.currency = 'USD' THEN li.original_amount * li.interest_rate / 100 ELSE 0 END) as usd_revenue,
                SUM(CASE WHEN li.currency = 'ZWL' THEN li.original_amount * li.interest_rate / 100 ELSE 0 END) as zwl_revenue
            FROM loan_info li
        """)
        loan_data = dict(cursor.fetchone() or {})
        
        # Get channel revenue
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN ACNTS_ATM_OPERN = 1 THEN 100 ELSE 0 END) as atm_revenue,
                SUM(CASE WHEN ACNTS_INET_OPERN = 1 THEN 150 ELSE 0 END) as internet_revenue,
                SUM(CASE WHEN ACNTS_SMS_OPERN = 1 THEN 50 ELSE 0 END) as sms_revenue
            FROM clients
        """)
        channel_data = dict(cursor.fetchone() or {})
        
        # Get actual values from database
        loan_revenue = loan_data.get('loan_revenue', 0) or 0
        atm_revenue = channel_data.get('atm_revenue', 0) or 0
        internet_revenue = channel_data.get('internet_revenue', 0) or 0
        sms_revenue = channel_data.get('sms_revenue', 0) or 0
        
        # Add loan currency data
        revenue_metrics['usd_revenue'] = loan_data.get('usd_revenue', 0) or 0
        revenue_metrics['zwl_revenue'] = loan_data.get('zwl_revenue', 0) or 0
        
        # Calculate total revenue from all sources
        total_revenue = loan_revenue + atm_revenue + internet_revenue + sms_revenue
        
        # Set all revenue values
        revenue_metrics['loan_revenue'] = loan_revenue
        revenue_metrics['atm_revenue'] = atm_revenue
        revenue_metrics['internet_revenue'] = internet_revenue
        revenue_metrics['sms_revenue'] = sms_revenue
        revenue_metrics['total_revenue'] = total_revenue
        
        # Calculate percentages
        if total_revenue > 0:
            revenue_metrics['loan_contribution'] = (loan_revenue / total_revenue * 100)
            revenue_metrics['atm_percentage'] = (atm_revenue / total_revenue * 100)
            revenue_metrics['internet_percentage'] = (internet_revenue / total_revenue * 100)
            revenue_metrics['sms_percentage'] = (sms_revenue / total_revenue * 100)
        else:
            # Fallback if total revenue is zero
            revenue_metrics['loan_contribution'] = 0
            revenue_metrics['atm_percentage'] = 0
            revenue_metrics['internet_percentage'] = 0
            revenue_metrics['sms_percentage'] = 0
            
    except Exception as e:
        print(f"Error calculating revenue breakdown: {str(e)}")
        # Fallback values if calculation fails
        revenue_metrics['loan_revenue'] = revenue_metrics.get('total_revenue', 0) * 0.4
        revenue_metrics['loan_contribution'] = 40.0
        revenue_metrics['atm_percentage'] = 20.0
        revenue_metrics['internet_percentage'] = 30.0
        revenue_metrics['sms_percentage'] = 10.0
        revenue_metrics['usd_revenue'] = revenue_metrics.get('total_revenue', 0) * 0.7
        revenue_metrics['zwl_revenue'] = revenue_metrics.get('total_revenue', 0) * 0.3
    
    # Get client metrics
    cursor.execute("""
        SELECT 
            COUNT(*) as total_clients,
            SUM(CASE WHEN ACNTS_DORMANT_ACNT = 0 AND ACNTS_INOP_ACNT = 0 THEN 1 ELSE 0 END) as active_clients,
            SUM(CASE WHEN ACNTS_DORMANT_ACNT = 1 THEN 1 ELSE 0 END) as inactive_clients,
            SUM(CASE WHEN ACNTS_INOP_ACNT = 1 THEN 1 ELSE 0 END) as closed_clients
        FROM clients
    """)
    client_metrics = dict(cursor.fetchone())
    
    # Get a sample client for churn analysis
    cursor.execute("""
        SELECT * FROM clients
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
    
    # Calculate metrics including CLV
    total_clients = client_metrics['total_clients'] or 1  # Avoid division by zero
    avg_revenue = revenue_metrics.get('total_revenue', 0) / total_clients if total_clients > 0 else 0
    
    # Calculate churn rate and trend
    churn_rate = 5.2
    churn_trend = churn_rate - 6  # Negative means improvement
    churn_trend_value = abs(churn_trend)
    
    # Calculate growth rates (using simulated data)
    total_clients_growth = stats.get('total_clients_growth', 0) or 0
    active_clients_growth = stats.get('active_clients_growth', 0) or 0
    total_branches_growth = stats.get('total_branches_growth', 0) or 0
    active_branches_growth = stats.get('active_branches_growth', 0) or 0
    total_revenue_growth = 5.2  # Simulated growth rate
    atm_revenue_growth = 3.8    # Simulated growth rate
    internet_revenue_growth = 7.5  # Simulated growth rate
    sms_revenue_growth = 2.3    # Simulated growth rate
    clients_growth = 4.1        # Simulated growth rate

    # Add word representations for large numbers
    def add_word_format(value):
        # Simply return the value directly instead of creating a dictionary
        return value
    
    # Set default values for any NULL revenue metrics
    revenue_metrics = {k: v if v is not None else 0 for k, v in revenue_metrics.items()}
    
    metrics = {
        'avg_clv': add_word_format(avg_revenue * 12),  # Annualized CLV
        'clv_cac_ratio': 3.5,  # Example ratio
        'retention_rate': 85.5,  # Example retention rate
        'predicted_growth': 12.3,
        'churn_rate': churn_rate,
        'churn_trend': churn_trend_value,
        'churn_improving': churn_trend < 0,
        'churn_prediction': 'Low Risk',
        'clv_trend': 'Upward',
        'cac_breakdown': 'Marketing: 60%, Sales: 40%',
        'revenue_per_customer': avg_revenue,
        'cac_digital_ads': 120.00,
        'cac_content': 80.00,
        'cac_social': 60.00,
        'cac_sales_team': 150.00,
        'cac_support': 90.00,
        'cac_tools': 50.00,
        # Account statistics from stats
        'total_clients': stats['total_clients'],
        'active_clients': stats['active_clients'],
        'inactive_clients': stats['inactive_clients'],
        'closed_clients': stats['closed_clients'],
        'total_clients_growth': total_clients_growth,
        'total_clients_growth_abs': abs(total_clients_growth),
        'active_clients_growth': active_clients_growth,
        'active_clients_growth_abs': abs(active_clients_growth),
        # Branch information
        'total_branches': stats['total_branches'],
        'active_branches': stats['active_branches'],
        'total_branches_growth': total_branches_growth,
        'total_branches_growth_abs': abs(total_branches_growth),
        'active_branches_growth': active_branches_growth,
        'active_branches_growth_abs': abs(active_branches_growth),
        # Channel revenue
        'total_revenue': add_word_format(revenue_metrics['total_revenue'] or 0),
        'atm_revenue': add_word_format(revenue_metrics['atm_revenue'] or 0),
        'internet_revenue': add_word_format(revenue_metrics['internet_revenue'] or 0),
        'sms_revenue': add_word_format(revenue_metrics['sms_revenue'] or 0),
        'total_revenue_growth': total_revenue_growth,
        'total_revenue_growth_abs': abs(total_revenue_growth),
        'atm_revenue_growth': atm_revenue_growth,
        'atm_revenue_growth_abs': abs(atm_revenue_growth),
        'internet_revenue_growth': internet_revenue_growth,
        'internet_revenue_growth_abs': abs(internet_revenue_growth),
        'sms_revenue_growth': sms_revenue_growth,
        'sms_revenue_growth_abs': abs(sms_revenue_growth),
        'clients_growth': clients_growth,
        'clients_growth_abs': abs(clients_growth)
    }

    # Chart data for visualization
    chart_data = {
        'segments': {
            'labels': ['Premium', 'Standard', 'Basic', 'Trial'],
            'data': [30, 45, 15, 10],
            'profitability': [2500, 1200, 500, 100],
            'growth_rate': [15, 8, 5, 20],
            'descriptions': [
                'High-value accounts with multiple products',
                'Regular customers with stable engagement',
                'Single product customers',
                'New customers in evaluation period'
            ]
        },
        'revenue': {
            'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
            'data': [1000, 1200, 1100, 1400, 1300, 1500]
        },
        'channels': {
            'labels': ['Direct', 'Referral', 'Social', 'Email'],
            'data': [3500, 2800, 2100, 1800]
        },
        'retention': {
            'labels': ['Q1', 'Q2', 'Q3', 'Q4'],
            'data': [92, 88, 85, 89]
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
        }
    }
    
    # Convert chart data to JSON for JavaScript
    chart_data_json = json.dumps(chart_data)
    
    return render_template('dashboard.html', 
                         revenue_metrics=revenue_metrics,
                         client_metrics=client_metrics,
                         metrics=metrics,
                         chart_data=chart_data,
                         chart_data_json=chart_data_json)

@app.route('/reports')
@login_required
def reports():
    try:
        conn = get_db()
        if conn is None:
            return render_template('error.html',
                                error="Database connection failed",
                                message="Could not connect to the database. Please try again later."), 500

        cursor = conn.cursor()
        
        # Get client revenue metrics with fixed values instead of missing columns
        cursor.execute("""
            WITH current_period AS (
                SELECT 
                    COUNT(DISTINCT ACNTS_CLIENT_NUM) as new_clients,
                    SUM(CASE WHEN ACNTS_DORMANT_ACNT = 0 AND ACNTS_INOP_ACNT = 0 THEN 1 ELSE 0 END) as retained_clients,
                    SUM(CASE WHEN ACNTS_ATM_OPERN = 1 THEN 100 ELSE 0 END) +
                    SUM(CASE WHEN ACNTS_INET_OPERN = 1 THEN 150 ELSE 0 END) +
                    SUM(CASE WHEN ACNTS_SMS_OPERN = 1 THEN 50 ELSE 0 END) as total_revenue
                FROM clients
                WHERE ACNTS_OPENING_DATE >= date('now', '-30 days')
            )
            SELECT * FROM current_period
        """)
        
        revenue_data = cursor.fetchone()
        if not revenue_data:
            revenue_metrics = {
                'new_client_revenue': 0,
                'retained_client_revenue': 0,
                'avg_new_revenue': 0,
                'avg_retained_revenue': 0
            }
        else:
            new_clients = revenue_data['new_clients'] or 0
            retained_clients = revenue_data['retained_clients'] or 0
            total_revenue = revenue_data['total_revenue'] or 0
            
            # Calculate revenue breakdown
            revenue_metrics = {
                'new_client_revenue': total_revenue * 0.3,  # Assume 30% from new clients
                'retained_client_revenue': total_revenue * 0.7,  # 70% from retained clients
                'avg_new_revenue': (total_revenue * 0.3) / new_clients if new_clients > 0 else 0,
                'avg_retained_revenue': (total_revenue * 0.7) / retained_clients if retained_clients > 0 else 0
            }

        # Get trend data for charts
        cursor.execute("""
            SELECT 
                strftime('%Y-%m', ACNTS_OPENING_DATE) as month,
                COUNT(*) as new_clients,
                SUM(CASE WHEN ACNTS_DORMANT_ACNT = 0 AND ACNTS_INOP_ACNT = 0 THEN 1 ELSE 0 END) as retained_clients
            FROM clients
            WHERE ACNTS_OPENING_DATE >= date('now', '-6 months')
            GROUP BY month
            ORDER BY month
        """)
        
        trend_data = cursor.fetchall()
        chart_data = {
            'labels': [],
            'new_clients': [],
            'retained_clients': []
        }
        
        if trend_data:
            for row in trend_data:
                chart_data['labels'].append(row['month'])
                chart_data['new_clients'].append(row['new_clients'])
                chart_data['retained_clients'].append(row['retained_clients'])
        else:
            # Sample data if no trends available
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']
            chart_data = {
                'labels': months,
                'new_clients': [50, 45, 60, 55, 48, 52],
                'retained_clients': [200, 210, 205, 220, 215, 225]
            }

        return render_template('reports.html',
                            revenue_metrics=revenue_metrics,
                            trend_data_json=json.dumps(chart_data))
                            
    except Exception as e:
        print(f"Error generating reports: {str(e)}")
        return render_template('error.html',
                            error="Report Generation Failed",
                            message="An error occurred while generating reports. Please try again later."), 500

@app.route('/data-quality', methods=['GET', 'POST'])
@login_required
def data_quality():
    try:
        conn = get_db()
        if conn is None:
            return render_template('error.html',
                                error="Database connection failed",
                                message="Could not connect to the database. Please try again later."), 500
            
        cursor = conn.cursor()
        
        # Ensure the data_quality_issues table exists with the right structure
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_issues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id TEXT NOT NULL,
                    issue_type TEXT NOT NULL,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'open',
                    resolution_notes TEXT,
                    resolved_at TIMESTAMP
                )
            """)
            conn.commit()
            
            # Add sample data if the table is empty
            cursor.execute("SELECT COUNT(*) FROM data_quality_issues")
            count = cursor.fetchone()[0]
            
            if count == 0:
                print("Adding sample data quality issues")
                sample_issues = [
                    ('1000001', 'Missing Data', 'open', None, None),
                    ('1000002', 'Incorrect Format', 'open', None, None),
                    ('1000003', 'Inconsistent Data', 'resolved', 'Fixed by data cleanup script', '2025-04-01 14:30:00')
                ]
                
                cursor.executemany("""
                    INSERT INTO data_quality_issues (client_id, issue_type, status, resolution_notes, resolved_at)
                    VALUES (?, ?, ?, ?, ?)
                """, sample_issues)
                conn.commit()
        except Exception as e:
            print(f"Error ensuring data_quality_issues table exists: {str(e)}")
            # Continue anyway
        
        # Handle issue resolution
        if request.method == 'POST':
            issue_id = request.form.get('issue_id')
            resolution_notes = request.form.get('resolution_notes')
            
            if not issue_id or not resolution_notes:
                return render_template('error.html',
                                    error="Invalid Resolution Data",
                                    message="Please provide both issue ID and resolution notes."), 400
                                    
            try:
                cursor.execute("""
                    UPDATE data_quality_issues 
                    SET status = 'resolved',
                        resolution_notes = ?,
                        resolved_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (resolution_notes, issue_id))
                conn.commit()
            except Exception as e:
                print(f"Error updating issue: {str(e)}")
                conn.rollback()
                return render_template('error.html',
                                    error="Update Failed",
                                    message="Failed to update the issue. Please try again."), 500

        # Get filter parameters
        status = request.args.get('status', '')
        issue_type = request.args.get('type', '')
        
        # Build simplified query with only essential columns that exist
        query = """
            SELECT 
                i.id,
                i.client_id,
                i.issue_type,
                i.detected_at,
                i.status,
                i.resolution_notes,
                i.resolved_at,
                c.ACNTS_AC_NAME1 as client_name
            FROM data_quality_issues i
            LEFT JOIN clients c ON i.client_id = c.ACNTS_CLIENT_NUM
            WHERE 1=1
        """
        params = []
        
        if status:
            query += " AND i.status = ?"
            params.append(status)
            
        if issue_type:
            query += " AND i.issue_type = ?"
            params.append(issue_type)
            
        query += " ORDER BY i.detected_at DESC"
        
        cursor.execute(query, params)
        issues = [dict(row) for row in cursor.fetchall()]
        
        return render_template('data_quality.html', 
                            issues=issues,
                            selected_status=status,
                            selected_type=issue_type)
                            
    except Exception as e:
        print(f"Error loading data quality issues: {str(e)}")
        return render_template('error.html',
                            error="Failed to Load Issues",
                            message="An error occurred while loading data quality issues. Please try again later."), 500

@app.route('/loans')
@login_required
def loans():
    try:
        conn = get_db()
        if conn is None:
            return render_template('error.html',
                                error="Database connection failed",
                                message="Could not connect to the database. Please try again later."), 500
                                
        cursor = conn.cursor()

        # Get loan summary statistics with fixed values instead of missing columns
        cursor.execute("""
            SELECT 
                COUNT(*) as total_loans,
                COUNT(*) * 10000 as total_disbursed, -- Fixed value: average loan amount of 10000
                COUNT(*) * 8500 as total_outstanding, -- Fixed value: average outstanding of 8500
                5.75 as avg_interest_rate, -- Fixed interest rate
                date('now', '-1 year') as earliest_maturity,
                date('now', '+3 years') as latest_maturity,
                COUNT(CASE WHEN ACNTS_DORMANT_ACNT = 0 AND ACNTS_INOP_ACNT = 0 THEN 1 END) as performing_loans,
                COUNT(CASE WHEN ACNTS_DORMANT_ACNT = 1 OR ACNTS_INOP_ACNT = 1 THEN 1 END) as non_performing_loans
            FROM clients
            WHERE ACNTS_PROD_CODE IN (3002, 3004, 3006)  -- Assuming these are loan product codes
        """)
        
        loan_metrics = dict(cursor.fetchone())
        
        return render_template('loans.html',
                            loan_metrics=loan_metrics)
                            
    except Exception as e:
        print(f"Error loading loans page: {str(e)}")
        return render_template('error.html',
                            error="Failed to Load Loans",
                            message="An error occurred while loading the loans page. Please try again later."), 500

@app.route('/client/<client_id>/channel/<channel_name>')
@login_required
def channel_transactions(client_id, channel_name):
    try:
        conn = get_db()
        if conn is None:
            return render_template('error.html',
                                error="Database connection failed",
                                message="Could not connect to the database. Please try again later."), 500
                                
        cursor = conn.cursor()
        
        # Get client basic info
        cursor.execute("""
            SELECT * FROM clients 
            WHERE ACNTS_CLIENT_NUM = ?
        """, (client_id,))
        
        client_row = cursor.fetchone()
        if client_row is None:
            return render_template('error.html',
                                error="Client not found",
                                message=f"Could not find client with ID {client_id}"), 404
                                
        client = dict(client_row)
        
        # Get channel-specific transactions (sample data as transactions are not in the model)
        transactions = []
        
        # Generate sample transactions based on the channel
        if channel_name == 'ATM':
            # Only show ATM transactions if the client has ATM operations enabled
            if client['ACNTS_ATM_OPERN'] == 1:
                transactions = [
                    {'date': '2025-04-10', 'amount': 200.00, 'type': 'Withdrawal', 'location': 'ATM #123'},
                    {'date': '2025-04-05', 'amount': 100.00, 'type': 'Withdrawal', 'location': 'ATM #456'},
                    {'date': '2025-03-28', 'amount': 300.00, 'type': 'Withdrawal', 'location': 'ATM #789'},
                    {'date': '2025-03-20', 'amount': 50.00, 'type': 'Balance Inquiry', 'location': 'ATM #123'}
                ]
        elif channel_name == 'Internet':
            # Only show Internet transactions if the client has Internet operations enabled
            if client['ACNTS_INET_OPERN'] == 1:
                transactions = [
                    {'date': '2025-04-12', 'amount': 500.00, 'type': 'Transfer', 'recipient': 'Jane Doe'},
                    {'date': '2025-04-08', 'amount': 120.50, 'type': 'Bill Payment', 'recipient': 'Electric Company'},
                    {'date': '2025-04-01', 'amount': 75.25, 'type': 'Bill Payment', 'recipient': 'Water Company'},
                    {'date': '2025-03-25', 'amount': 1000.00, 'type': 'Transfer', 'recipient': 'Savings Account'}
                ]
        elif channel_name == 'SMS':
            # Only show SMS transactions if the client has SMS operations enabled
            if client['ACNTS_SMS_OPERN'] == 1:
                transactions = [
                    {'date': '2025-04-11', 'amount': 0.00, 'type': 'Balance Inquiry', 'response': '$2,450.75'},
                    {'date': '2025-04-09', 'amount': 25.00, 'type': 'Airtime Purchase', 'recipient': 'Self'},
                    {'date': '2025-04-02', 'amount': 50.00, 'type': 'Airtime Purchase', 'recipient': '+1234567890'},
                    {'date': '2025-03-27', 'amount': 0.00, 'type': 'Mini Statement', 'response': 'Last 3 transactions'}
                ]
        
        # Get client name for display
        client_name = f"{client['ACNTS_AC_NAME1']} {client['ACNTS_AC_NAME2']}".strip()
        
        # Channel specific metrics
        channel_metrics = {
            'total_transactions': len(transactions),
            'last_activity': transactions[0]['date'] if transactions else 'No activity',
            'channel_name': channel_name,
            'is_active': (client['ACNTS_ATM_OPERN'] == 1 and channel_name == 'ATM') or
                        (client['ACNTS_INET_OPERN'] == 1 and channel_name == 'Internet') or
                        (client['ACNTS_SMS_OPERN'] == 1 and channel_name == 'SMS')
        }
        
        return render_template('channel_transactions.html',
                           client=client,
                           client_name=client_name,
                           channel_name=channel_name,
                           transactions=transactions,
                           metrics=channel_metrics)
                           
    except Exception as e:
        print(f"Error loading channel transactions: {str(e)}")
        return render_template('error.html',
                           error="Failed to Load Transactions",
                           message="An error occurred while loading channel transactions. Please try again later."), 500

@app.errorhandler(404)
def not_found_error(error):
    return render_template('error.html',
                        error="Page Not Found",
                        message="The page you requested could not be found."), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html',
                        error="Internal Server Error", 
                        message="An unexpected error occurred. Please try again later."), 500
                        
@app.errorhandler(401)
def unauthorized_error(error):
    return render_template('error.html',
                        error="Unauthorized",
                        message="You need to login to access this page."), 401

@app.errorhandler(403)
def forbidden_error(error):
    return render_template('error.html',
                        error="Access Forbidden",
                        message="You don't have permission to access this resource."), 403

@app.route('/api/loan-currencies')
@login_required
def get_loan_currencies():
    try:
        conn = get_db()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Get distinct currency values
        cursor.execute("""
            SELECT DISTINCT currency 
            FROM loan_info 
            WHERE currency IS NOT NULL AND currency != ''
            ORDER BY currency
        """)
        
        currencies = [dict(row)['currency'] for row in cursor.fetchall()]
        
        # Add USD as default if not already present
        if not currencies or 'USD' not in currencies:
            currencies.insert(0, 'USD')
            
        return jsonify({'currencies': currencies})
        
    except Exception as e:
        print(f"Error fetching currency data: {str(e)}")
        return jsonify({'error': 'Failed to fetch currency data'}), 500

if __name__ == '__main__':
    app.run(debug=True)