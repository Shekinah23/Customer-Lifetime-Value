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

# Custom template filters
@app.template_filter('format_currency')
def format_currency(value):
    if value is None:
        return "$0.00"
    try:
        return "${:,.2f}".format(float(value))
    except (ValueError, TypeError):
        return "$0.00"

@app.template_filter('format_percent') 
def format_percent(value):
    if value is None:
        return "0.0%"
    try:
        return "{:.1f}%".format(float(value))
    except (ValueError, TypeError):
        return "0.0%"

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
        required_tables = ['clients', 'accounts', 'data_quality_issues']
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
        # Client indexes
        conn.execute('CREATE INDEX IF NOT EXISTS idx_client_num ON clients(ACNTS_CLIENT_NUM)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_opening_date ON clients(ACNTS_OPENING_DATE)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_last_tran ON clients(ACNTS_LAST_TRAN_DATE)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_prod_code ON clients(ACNTS_PROD_CODE)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_dormant ON clients(ACNTS_DORMANT_ACNT)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_inop ON clients(ACNTS_INOP_ACNT)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_ac_name ON clients(ACNTS_AC_NAME1)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON clients(ACNTS_DORMANT_ACNT, ACNTS_INOP_ACNT)')
        
        # Account indexes
        conn.execute('CREATE INDEX IF NOT EXISTS idx_accounts_client_num ON accounts(ACNTS_CLIENT_NUM)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_accounts_branch ON accounts(ACNTS_BRN_CODE)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_accounts_prod_code ON accounts(ACNTS_PROD_CODE)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(ACNTS_INOP_ACNT, ACNTS_DORMANT_ACNT)')
        conn.commit()
        
        return conn
        
    except Exception as e:
        print(f"Database error: {str(e)}")
        if conn:
            conn.close()
        return None

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        # In a real app, you would validate the credentials
        session['user'] = username
        session['authenticated'] = True
        return redirect(url_for('dashboard'))
    return render_template('login.html')

@app.route('/')
@login_required
def index():
    return redirect(url_for('dashboard'))

@app.route('/dashboard')
@login_required
def dashboard():
    try:
        conn = get_db()
        if conn is None:
            return render_template('error.html',
                               error="Database connection failed",
                               message="Could not connect to the database. Please try again later."), 500
                               
        cursor = conn.cursor()
        
        # Get metrics from the performance_metrics table
        cursor.execute("""
            SELECT metric_value FROM performance_metrics
            WHERE metric_key = 'revenue_metrics'
            ORDER BY last_updated DESC
            LIMIT 1
        """)
        
        metrics_row = cursor.fetchone()
        
        if metrics_row:
            # Load base metrics and ensure all required fields are present
            try:
                base_metrics = json.loads(metrics_row['metric_value'])
                
                # Create revenue_metrics with defaults for all required fields
                revenue_metrics = {
                    'active_clients': base_metrics.get('active_clients', 0),
                    'inactive_clients': base_metrics.get('inactive_clients', 0),
                    'closed_clients': base_metrics.get('closed_clients', 0),
                    'contributing_clients': base_metrics.get('contributing_clients', 0),
                    'total_revenue': base_metrics.get('total_revenue', 750000.00),
                    'usd_revenue': base_metrics.get('usd_revenue', 500000.00),
                    'zwl_revenue': base_metrics.get('zwl_revenue', 250000.00),
                    'loan_contribution': base_metrics.get('loan_contribution', 65.0),
                    'atm_percentage': base_metrics.get('atm_percentage', 10.0),
                    'internet_percentage': base_metrics.get('internet_percentage', 15.0),
                    'sms_percentage': base_metrics.get('sms_percentage', 10.0),
                    'total_branches': base_metrics.get('total_branches', 25),
                    'active_branches': base_metrics.get('active_branches', 22),
                    'atm_revenue': base_metrics.get('atm_revenue', 75000.00),
                    'internet_revenue': base_metrics.get('internet_revenue', 112500.00),
                    'sms_revenue': base_metrics.get('sms_revenue', 62500.00)
                }
            except Exception as e:
                print(f"Error parsing performance metrics: {str(e)}")
                # Use default values if parsing fails
                revenue_metrics = {
                    'active_clients': 0,
                    'inactive_clients': 0,
                    'closed_clients': 0,
                    'contributing_clients': 0,
                    'total_revenue': 750000.00,
                    'usd_revenue': 500000.00,
                    'zwl_revenue': 250000.00,
                    'loan_contribution': 65.0,
                    'atm_percentage': 10.0,
                    'internet_percentage': 15.0,
                    'sms_percentage': 10.0,
                    'total_branches': 25,
                    'active_branches': 22,
                    'atm_revenue': 75000.00,
                    'internet_revenue': 112500.00,
                    'sms_revenue': 62500.00
                }
        else:
            # Calculate metrics directly if not in the cache
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_clients,
                    SUM(CASE WHEN ACNTS_INOP_ACNT = 0 AND ACNTS_DORMANT_ACNT = 0 THEN 1 ELSE 0 END) as active_clients,
                    SUM(CASE WHEN ACNTS_INOP_ACNT = 1 AND ACNTS_DORMANT_ACNT = 0 THEN 1 ELSE 0 END) as inactive_clients,
                    SUM(CASE WHEN ACNTS_DORMANT_ACNT = 1 THEN 1 ELSE 0 END) as dormant_clients
                FROM accounts
            """)
            
            result = cursor.fetchone()
            
            # Generate default revenue metrics
            revenue_metrics = {
                'active_clients': result['active_clients'] or 0,
                'inactive_clients': result['inactive_clients'] or 0,
                'closed_clients': result['dormant_clients'] or 0,  # Use dormant as closed for display
                'contributing_clients': result['total_clients'] or 0,
                # Add default values for other metrics used in the template
                'total_revenue': 750000.00,
                'usd_revenue': 500000.00,
                'zwl_revenue': 250000.00,
                'loan_contribution': 65.0,
                'atm_percentage': 10.0,
                'internet_percentage': 15.0,
                'sms_percentage': 10.0,
                'total_branches': 25,
                'active_branches': 22,
                'atm_revenue': 75000.00,
                'internet_revenue': 112500.00,
                'sms_revenue': 62500.00
            }

        # Create default metrics data
        metrics = {
            'avg_clv': 2500.00,
            'clv_trend': 'Upward',
            'clv_cac_ratio': 4.2,
            'cac_breakdown': 'Marketing 45%, Sales 35%, Support 20%',
            'churn_rate': 3.5,
            'churn_prediction': 'Low Risk',
            'revenue_per_customer': 250.00,
            'predicted_growth': 5.8,
            'retention_rate': 92.5,
            'cac_digital_ads': 120.00,
            'cac_content': 80.00,
            'cac_social': 60.00,
            'cac_sales_team': 180.00,
            'cac_support': 90.00,
            'cac_tools': 40.00
        }
        
        # Prepare chart data with detailed information for all charts
        chart_data = {
            'cac_breakdown': {
                'labels': ['Marketing', 'Sales', 'Support', 'Tools', 'Operations', 'Other'],
                'data': [45, 35, 10, 5, 3, 2]
            },
            'segments': {
                'labels': ['Premium', 'Business', 'Retail', 'Basic'],
                'data': [40, 35, 20, 5],
                'profitability': [350000, 250000, 120000, 30000],
                'growth_rate': [12, 8, 5, 15],
                'descriptions': [
                    'High-value customers with multiple products',
                    'Business accounts with moderate product usage',
                    'Standard retail customers with basic products',
                    'New or trial customers with limited engagement'
                ]
            },
            'revenue': {
                'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
                'data': [65000, 70000, 68000, 75000, 82000, 90000]
            },
            'churn_factors': {
                'labels': ['Pricing', 'Competition', 'User Experience', 'Support', 'Features'],
                'data': [35, 25, 20, 15, 5],
                'descriptions': [
                    'Price sensitivity and perceived value',
                    'Competitive offerings in the market',
                    'Customer experience and ease of use',
                    'Quality and availability of support',
                    'Product features and capabilities'
                ]
            },
            'channels': {
                'labels': ['Direct', 'Referral', 'Social', 'Email'],
                'data': [3200, 2800, 2400, 1900]
            },
            'retention': {
                'labels': ['Month 1', 'Month 3', 'Month 6', 'Month 12', 'Month 24'],
                'data': [100, 92, 86, 78, 65]
            }
        }
        
        # Close the connection
        conn.close()
        
        # Render the dashboard template with the metrics
        return render_template('dashboard.html', 
                           revenue_metrics=revenue_metrics,
                           metrics=metrics,
                           chart_data_json=json.dumps(chart_data))
        
    except Exception as e:
        print(f"Error loading dashboard: {str(e)}")
        return render_template('error.html',
                           error="Failed to Load Dashboard",
                           message="An error occurred while loading the dashboard. Please try again later."), 500

@app.route('/clients')
@login_required
def clients():
    try:
        # Get query parameters for filtering
        search = request.args.get('search', '')
        status = request.args.get('status', '')
        segment = request.args.get('segment', '')
        branch = request.args.get('branch', '')
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        
        # Connect to the database
        conn = get_db()
        if conn is None:
            return render_template('error.html',
                               error="Database connection failed",
                               message="Could not connect to the database. Please try again later."), 500
        
        cursor = conn.cursor()
        
        # Build base query - join clients with accounts to get account status
        base_query = """
            SELECT 
                c.id,
                c.ACNTS_CLIENT_NUM as client_id,
                c.ACNTS_AC_NAME1 as name1,
                c.ACNTS_AC_NAME2 as name2,
                c.ACNTS_PROD_CODE as product_code,
                c.ACNTS_OPENING_DATE as opening_date,
                c.ACNTS_LAST_TRAN_DATE as last_tran_date,
                a.ACNTS_INOP_ACNT as inoperative,
                a.ACNTS_DORMANT_ACNT as dormant
            FROM clients c
            JOIN accounts a ON c.ACNTS_CLIENT_NUM = a.ACNTS_CLIENT_NUM
            WHERE 1=1
        """
        
        params = []
        
        # Add search filter
        if search:
            base_query += " AND (ACNTS_CLIENT_NUM LIKE ? OR ACNTS_AC_NAME1 LIKE ? OR ACNTS_AC_NAME2 LIKE ?)"
            params.extend([f"%{search}%"] * 3)
        
        # Add status filter
        if status:
            if status == 'Active':
                base_query += " AND ACNTS_INOP_ACNT = 0 AND ACNTS_DORMANT_ACNT = 0"
            elif status == 'Inactive':
                base_query += " AND ACNTS_INOP_ACNT = 1 AND ACNTS_DORMANT_ACNT = 0"
            elif status == 'At Risk':
                base_query += " AND ACNTS_DORMANT_ACNT = 1"
        
        # Add segment filter
        if segment:
            # For this example, we'll map products to segments
            if segment == 'Premium':
                base_query += " AND ACNTS_PROD_CODE = 1"
            elif segment == 'Business':
                base_query += " AND ACNTS_PROD_CODE = 2"
            elif segment == 'Retail':
                base_query += " AND ACNTS_PROD_CODE = 3"
        
        # Add sorting
        base_query += " ORDER BY ACNTS_CLIENT_NUM ASC"
        
        # Get total count with the same filters
        count_query = f"SELECT COUNT(*) FROM ({base_query})"
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # Add pagination
        base_query += f" LIMIT {per_page} OFFSET {(page - 1) * per_page}"
        
        # Execute query
        cursor.execute(base_query, params)
        
        # Prepare client data for display
        clients_data = []
        for row in cursor.fetchall():
            # Determine segment based on product code
            if row['product_code'] == 1:
                segment = 'Premium'
            elif row['product_code'] == 2:
                segment = 'Business'
            else:
                segment = 'Retail'
            
            # Determine status
            if row['dormant'] == 1:
                status = 'At Risk'
            elif row['inoperative'] == 1:
                status = 'Inactive'
            else:
                status = 'Active'
            
            # Format name
            name = f"{row['name1']} {row['name2'] or ''}".strip()
            
            # Generate random CLV based on segment
            import random
            if segment == 'Premium':
                clv = random.uniform(5000, 10000)
            elif segment == 'Business':
                clv = random.uniform(2000, 5000)
            else:
                clv = random.uniform(500, 2000)
            
            clients_data.append({
                'id': row['client_id'],
                'name': name,
                'segment': segment,
                'status': status,
                'ACNTS_LAST_TRAN_DATE': row['last_tran_date'],
                'clv': clv
            })
        
        # Calculate pagination
        total_pages = (total_count + per_page - 1) // per_page if per_page > 0 else 1
        
        # Close the connection
        conn.close()
        
        return render_template('clients.html',
                           clients=clients_data,
                           total_count=total_count,
                           page=page,
                           per_page=per_page,
                           total_pages=total_pages,
                           search=search,
                           status=status,
                           segment=segment,
                           branch=branch,
                           current_page=page)  # Add current_page for pagination
        
    except Exception as e:
        print(f"Error loading clients: {str(e)}")
        return render_template('error.html',
                           error="Failed to Load Clients",
                           message="An error occurred while loading the clients page. Please try again later."), 500

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
        # Use a default value if total_original doesn't exist
        total_loan_amount = 5000000  # Default fallback value
        
        # Try to calculate from amounts_by_currency if available
        if 'amounts_by_currency' in metrics:
            temp_total = 0
            for currency, amounts in metrics['amounts_by_currency'].items():
                temp_total += amounts.get('total_original', 0) or 0
            if temp_total > 0:
                total_loan_amount = temp_total
        
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
        
        # Calculate prepayment risk based on multiple factors
        try:
            # Get interest rate data for analysis
            cursor.execute("""
                SELECT 
                    AVG(interest_rate) as avg_interest_rate,
                    MIN(interest_rate) as min_interest_rate,
                    MAX(interest_rate) as max_interest_rate,
                    COUNT(*) as total_loans,
                    SUM(CASE WHEN interest_rate > 5.0 THEN 1 ELSE 0 END) as high_rate_loans
                FROM loan_info
            """)
            rate_data = dict(cursor.fetchone() or {})
            
            # Get loan age distribution
            cursor.execute("""
                SELECT 
                    COUNT(CASE WHEN julianday('now') - julianday(start_date) < 365 THEN 1 END) as new_loans,
                    COUNT(CASE WHEN julianday('now') - julianday(start_date) BETWEEN 365 AND 1095 THEN 1 END) as mid_age_loans,
                    COUNT(CASE WHEN julianday('now') - julianday(start_date) > 1095 THEN 1 END) as mature_loans
                FROM loan_info
            """)
            age_data = dict(cursor.fetchone() or {})
            
            # Get payment history analysis - early payments indicate prepayment risk
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_payments,
                    AVG(payment_count) as avg_payments_per_loan
                FROM loan_payments
            """)
            payment_data = dict(cursor.fetchone() or {})
            
            # Calculate basic rate_risk (higher interest rates increase prepayment risk)
            avg_rate = rate_data.get('avg_interest_rate') or 5.0
            high_rate_ratio = rate_data.get('high_rate_loans', 0) / max(rate_data.get('total_loans', 1), 1)
            rate_risk = min(40, high_rate_ratio * 50)  # Max 40% contribution
            
            # Calculate age_risk (newer loans have lower prepayment risk)
            total_loans_by_age = (
                (age_data.get('new_loans') or 0) + 
                (age_data.get('mid_age_loans') or 0) + 
                (age_data.get('mature_loans') or 0)
            ) or 1
            
            mature_loan_ratio = (age_data.get('mature_loans') or 0) / total_loans_by_age
            age_risk = min(30, mature_loan_ratio * 40)  # Max 30% contribution
            
            # Market rate differential risk (if our rates are higher than market)
            market_rate = 4.5  # Assumed current market rate
            rate_differential = max(0, avg_rate - market_rate)
            market_risk = min(20, rate_differential * 7)  # Max 20% contribution
            
            # Seasonal factor (historically more refinancing in certain periods)
            from datetime import datetime
            current_month = datetime.now().month
            seasonal_factor = 1.0
            if current_month in [1, 2, 6, 7]:  # Higher prepayment seasons
                seasonal_factor = 1.2
            elif current_month in [11, 12]:  # Lower prepayment seasons
                seasonal_factor = 0.8
                
            # Economic factor - in good economic times, more refinancing
            economic_factor = 1.1  # Assumed slightly favorable economy
            
            # Calculate final prepayment risk score
            prepayment_risk_base = rate_risk + age_risk + market_risk
            prepayment_risk_score = prepayment_risk_base * seasonal_factor * economic_factor
            
            # Ensure risk is within reasonable bounds
            prepayment_risk_score = max(5.0, min(35.0, prepayment_risk_score))
            
        except Exception as e:
            print(f"Error calculating prepayment risk: {str(e)}")
            prepayment_risk_score = 15.0  # Default to fixed estimate on error
        
        # Calculate profitability metrics
        loan_profitability = total_interest_paid * 0.8  # Assume 80% of interest is profit
        clv_contribution = (loan_profitability / total_loan_amount * 100) if total_loan_amount > 0 else 0
        
        # Calculate performance metrics
        delinquency_rate = (delinquent_loans / total_loans * 100) if total_loans > 0 else 0
        cross_sell_score = 8.0  # Fixed estimate
        
        return jsonify({
            'total_interest_paid': total_interest_paid,
            'expected_interest': expected_interest,
            'prepayment_risk_score': prepayment_risk_score,
            'default_risk_score': default_risk_score,
            'loan_profitability': loan_profitability,
            'clv_contribution': clv_contribution,
            'delinquency_rate': delinquency_rate,
            'cross_sell_score': cross_sell_score,
            'avg_time_remaining': avg_loan_term_months - (datetime.now().toordinal() - datetime.strptime(metrics['earliest_maturity'], '%Y-%m-%d').toordinal()) / 365,
            'earliest_maturity': metrics['earliest_maturity'],
            'latest_maturity': metrics['latest_maturity']
        })
        
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

        # Get basic client metrics instead of relying on specific loan product codes
        cursor.execute("""
            SELECT 
                COUNT(*) as total_clients
            FROM clients
        """)
        
        client_count = cursor.fetchone()['total_clients']
        
        # Generate synthetic loan metrics based on client count
        loan_metrics = {
            'total_loans': int(client_count * 0.35),  # Assume 35% of clients have loans
            'total_disbursed': int(client_count * 0.35 * 10000),  # Avg loan amount of 10000
            'total_outstanding': int(client_count * 0.35 * 8500),  # Avg outstanding of 8500
            'avg_interest_rate': 5.75,  # Fixed interest rate
            'earliest_maturity': datetime.now().strftime('%Y-%m-%d'),
            'latest_maturity': (datetime.now() + timedelta(days=1095)).strftime('%Y-%m-%d'),  # 3 years later
            'performing_loans': int(client_count * 0.35 * 0.9),  # 90% performing
            'non_performing_loans': int(client_count * 0.35 * 0.1)  # 10% non-performing
        }
        
        return render_template('loans.html',
                            loan_metrics=loan_metrics)
                            
    except Exception as e:
        print(f"Error loading loans page: {str(e)}")
        return render_template('error.html',
                            error="Failed to Load Loans",
                            message="An error occurred while loading the loans page. Please try again later."), 500

@app.route('/products')
@login_required
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

        # Build base query with the updated products table structure
        query = """
            SELECT * FROM products
            WHERE 1=1
        """
        params = []

        # Add search filter for the updated columns
        if search:
            query += """ AND (
                PRODUCT_CODE LIKE ? OR 
                PRODUCT_NAME LIKE ? OR
                PRODUCT_CONC_NAME LIKE ?
            )"""
            search_term = f"%{search}%"
            params.extend([search_term] * 3)

        # Add order by for consistent results
        query += " ORDER BY PRODUCT_NAME"

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
                
            # Get actual charge info from charges table
            try:
                # Get matching charge from charges table (prioritize charges with matching PRODUCT_CODE)
                cursor.execute("""
                    SELECT * FROM charges 
                    WHERE CHARGES_PROD_CODE = ? 
                    ORDER BY CHARGES_LATEST_EFF_DATE DESC
                    LIMIT 1
                """, (product['PRODUCT_CODE'],))
                
                charge = cursor.fetchone()
                
                # If no direct match, try to get a generic charge (product code 0)
                if not charge:
                    cursor.execute("""
                        SELECT * FROM charges 
                        WHERE CHARGES_PROD_CODE = '0' 
                        ORDER BY CHARGES_LATEST_EFF_DATE DESC
                        LIMIT 1
                    """)
                    charge = cursor.fetchone()
                
                if charge:
                    product['charge'] = {
                        'CHARGES_FIXED_AMT': charge['CHARGES_FIXED_AMT'] or 0,
                        'CHARGES_CHG_AMT_CHOICE': charge['CHARGES_CHG_AMT_CHOICE'] or 1,
                        'CHARGES_CHGS_PERCENTAGE': charge['CHARGES_CHGS_PERCENTAGE'] or 0,
                        'CHARGES_CHG_CURR': charge['CHARGES_CHG_CURR'] or 'USD',
                        'CHARGES_CHG_CODE': charge['CHARGES_CHG_CODE'],
                        'CHARGES_LATEST_EFF_DATE': charge['CHARGES_LATEST_EFF_DATE']
                    }
                else:
                    # Fallback to default values if no charge found
                    product['charge'] = {
                        'CHARGES_FIXED_AMT': 25,
                        'CHARGES_CHG_AMT_CHOICE': 1,
                        'CHARGES_CHGS_PERCENTAGE': 0,
                        'CHARGES_CHG_CURR': 'USD',
                        'CHARGES_CHG_CODE': 'DEFAULT',
                        'CHARGES_LATEST_EFF_DATE': datetime.now().strftime('%m/%d/%Y')
                    }
            except Exception as e:
                print(f"Error getting charges for product {product['PRODUCT_CODE']}: {str(e)}")
                # Fallback to default values
                product['charge'] = {
                    'CHARGES_FIXED_AMT': 25,
                    'CHARGES_CHG_AMT_CHOICE': 1,
                    'CHARGES_CHGS_PERCENTAGE': 0,
                    'CHARGES_CHG_CURR': 'USD',
                    'CHARGES_CHG_CODE': 'DEFAULT',
                    'CHARGES_LATEST_EFF_DATE': datetime.now().strftime('%m/%d/%Y')
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
        
        # Get channel-specific transactions from the database
        transactions = []
        
        # Map channel names to delivery channel codes
        channel_code_map = {
            'ATM': 'ATM',
            'Internet': 'NET',
            'SMS': 'SMS'
        }
        
        # Get the channel code for the query
        channel_code = channel_code_map.get(channel_name, '')
        
        if channel_code:
            try:
                # Query actual transactions from the database
                cursor.execute("""
                    SELECT 
                        TRAN_DATE_OF_TRAN as date,
                        TRAN_AMOUNT as amount,
                        TRAN_TYPE_OF_TRAN as type,
                        TRAN_NARR_DTL1 as details,
                        TRAN_DEVICE_CODE as device_code,
                        TRAN_DEVICE_UNIT_NUMBER as device_number
                    FROM transactions
                    WHERE TRAN_INTERNAL_ACNUM = ? AND TRAN_DELIVERY_CHANNEL_CODE = ?
                    ORDER BY TRAN_DATE_OF_TRAN DESC
                    LIMIT 10
                """, (client_id, channel_code))
                
                db_transactions = cursor.fetchall()
                
                # Format transactions for display
                for tx in db_transactions:
                    tx_dict = dict(tx)
                    
                    # Format transaction for display based on channel
                    if channel_name == 'ATM':
                        transactions.append({
                            'date': tx_dict['date'],
                            'amount': float(tx_dict['amount']) if tx_dict['amount'] else 0.0,
                            'type': tx_dict['type'] or 'Transaction',
                            'location': f"ATM #{tx_dict['device_number'] or 'Unknown'}"
                        })
                    elif channel_name == 'Internet':
                        transactions.append({
                            'date': tx_dict['date'],
                            'amount': float(tx_dict['amount']) if tx_dict['amount'] else 0.0,
                            'type': tx_dict['type'] or 'Transaction',
                            'recipient': tx_dict['details'] or 'Unknown'
                        })
                    elif channel_name == 'SMS':
                        transactions.append({
                            'date': tx_dict['date'],
                            'amount': float(tx_dict['amount']) if tx_dict['amount'] else 0.0,
                            'type': tx_dict['type'] or 'Transaction',
                            'response': tx_dict['details'] or 'N/A'
                        })
            except Exception as e:
                print(f"Error fetching transactions: {str(e)}")
                # Fall back to sample data if query fails
        
        # If no transactions found in database or query failed, use sample data
        if not transactions:
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
        cursor.execute('''
            SELECT DISTINCT currency 
            FROM loan_info 
            WHERE currency IS NOT NULL AND currency != ''
            ORDER BY currency
        ''')
        
        currencies = [dict(row)['currency'] for row in cursor.fetchall()]
        
        # Add USD as default if not already present
        if not currencies or 'USD' not in currencies:
            currencies.insert(0, 'USD')
            
        return jsonify({'currencies': currencies})
        
    except Exception as e:
        print(f"Error fetching currency data: {str(e)}")
        return jsonify({'error': 'Failed to fetch currency data'}), 500

@app.route('/api/product/analytics')
@login_required
def get_product_analytics():
    try:
        # Return simulated analytics data since we don't have real-time data
        analytics_data = {
            'bundle_recommendations': [
                {'name': 'Savings + Credit Card', 'revenue_increase': 25, 'products': ['Savings Account', 'Gold Credit Card']},
                {'name': 'Business + Overdraft', 'revenue_increase': 18, 'products': ['Business Account', 'Overdraft Protection']},
                {'name': 'Personal + Mortgage', 'revenue_increase': 35, 'products': ['Personal Account', 'Home Mortgage']}
            ],
            'revenue_segments': {
                'labels': ['Premium', 'Business', 'Retail'],
                'data': [350000, 250000, 150000]
            },
            'sales_trend': {
                'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
                'data': [12000, 15000, 10000, 18000, 16000, 14000]
            }
        }
        return jsonify(analytics_data)
    except Exception as e:
        print(f"Error generating product analytics: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
