from flask import Flask, render_template, request, redirect, url_for, session, jsonify, Response, send_from_directory, make_response
from flask_caching import Cache
from functools import wraps
import os
import sqlite3
import json
import random
import pandas as pd
import pdfkit
from datetime import datetime, timedelta
from churn_predictor import calculate_churn_probability

# Cache configuration
cache_config = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300  # 5 minutes
}

app = Flask(__name__, 
           template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), 'templates')),
           static_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), 'static')),
           static_url_path='/static')
app.secret_key = 'dev_secret_key_123'
app.permanent_session_lifetime = timedelta(days=1)

# Initialize cache
cache = Cache(app)
app.config.from_mapping(cache_config)

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
    
    if not os.path.exists(db_path):
        print(f"Database file not found at: {db_path}")
        return None
        
    try:
        print(f"Connecting to database at: {db_path}")
        conn = sqlite3.connect(db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        
        # Set PRAGMA statements first for better performance
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA cache_size=-2000')
        conn.execute('PRAGMA temp_store=MEMORY')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA mmap_size=2147483648')
        conn.commit()
        
        # Check if clients table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='clients'")
        table_exists = cursor.fetchone()
        if not table_exists:
            print("Clients table does not exist in the database")
            conn.close()
            return None
        
        print("Clients table found")
        
        # Verify table has data
        cursor.execute("SELECT COUNT(*) FROM clients")
        count = cursor.fetchone()[0]
        print(f"Found {count} records in clients table")
        
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
        if 'conn' in locals():
            conn.close()
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

@app.route('/client/<client_id>')
@login_required
def client_details(client_id):
    conn = None
    try:
        conn = get_db()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Get client basic info
        cursor.execute("""
            SELECT 
                c.ACNTS_CLIENT_NUM as id,
                COALESCE(c.ACNTS_AC_NAME1, '') || ' ' || COALESCE(c.ACNTS_AC_NAME2, '') as name,
                CASE 
                    WHEN c.ACNTS_PROD_CODE = 3102 THEN 'Premium'
                    WHEN c.ACNTS_PROD_CODE = 3002 THEN 'Business'
                    WHEN c.ACNTS_PROD_CODE = 3101 THEN 'Retail'
                    ELSE 'Other'
                END as segment,
                CASE 
                    WHEN c.ACNTS_DORMANT_ACNT = 1 THEN 'Inactive'
                    WHEN c.ACNTS_INOP_ACNT = 1 THEN 'At Risk'
                    ELSE 'Active'
                END as status,
                c.*
            FROM clients c
            WHERE c.ACNTS_CLIENT_NUM = ?
        """, (client_id,))
        
        client = dict(cursor.fetchone())
        
        # Calculate current CLV
        account_age_days = 0
        if client['ACNTS_OPENING_DATE']:
            cursor.execute("SELECT julianday('now') - julianday(?) as age_days", 
                         (client['ACNTS_OPENING_DATE'],))
            age_result = cursor.fetchone()
            if age_result:
                account_age_days = age_result['age_days']
        
        base_clv = {
            3102: 5000,  # Premium accounts
            3002: 3000,  # Business accounts
            3101: 1000   # Retail accounts
        }.get(client['ACNTS_PROD_CODE'], 1000)
        
        account_age_years = account_age_days / 365.0
        current_clv = base_clv * (1 + account_age_years * 0.1)
        
        # Calculate predicted CLV and trend
        engagement_score = (
            (client['ACNTS_ATM_OPERN'] or 0) / 20 +
            (client['ACNTS_INET_OPERN'] or 0) / 30 * 1.5 +
            (client['ACNTS_SMS_OPERN'] or 0) / 25 * 1.2
        ) / 3.7 * 100
        
        product_score = 0
        if client['ACNTS_SALARY_ACNT'] == 1:
            product_score += 50
        if client['ACNTS_CR_CARDS_ALLOWED'] == 1:
            product_score += 50
        
        # Predict future value trend based on engagement and products
        value_trend = ((engagement_score / 100) * 0.6 + (product_score / 100) * 0.4 - 0.5) * 100
        predicted_clv = current_clv * (1 + value_trend / 100)
        
        metrics = {
            'current_clv': current_clv,
            'predicted_clv': predicted_clv,
            'value_trend': value_trend
        }
        
        # Generate historical and predicted CLV data
        months = 12
        historical_clv = []
        predicted_clv_trend = []
        
        for i in range(-months, months + 1):
            if i <= 0:
                # Historical data
                point_value = current_clv * (1 + (i / months) * 0.2)  # Simplified historical trend
                historical_clv.append(point_value)
                predicted_clv_trend.append(None)
            else:
                # Future predictions
                historical_clv.append(None)
                point_value = current_clv * (1 + (i / months) * (value_trend / 100))
                predicted_clv_trend.append(point_value)
        
        # Generate sample retention actions
        retention_actions = [
            {
                'type': 'Personalized Offer',
                'description': 'Premium account upgrade offer sent',
                'date': '2025-02-15',
                'status': 'completed'
            },
            {
                'type': 'Loyalty Program',
                'description': 'Bonus points campaign',
                'date': '2025-02-20',
                'status': 'pending'
            },
            {
                'type': 'Customer Outreach',
                'description': 'Scheduled account review call',
                'date': '2025-03-01',
                'status': 'scheduled'
            }
        ]
        
        # Calculate risk factors
        risk_factors = [
            {
                'name': 'Transaction Frequency',
                'impact': -15 if client['ACNTS_DORMANT_ACNT'] == 1 else 10,
                'description': 'Based on account activity patterns'
            },
            {
                'name': 'Digital Engagement',
                'impact': engagement_score - 50,  # Normalize to +/- scale
                'description': 'Based on digital channel usage'
            },
            {
                'name': 'Product Utilization',
                'impact': product_score - 50,  # Normalize to +/- scale
                'description': 'Based on product portfolio'
            }
        ]
        
        # Prepare chart data
        chart_data = {
            'clv_trend': {
                'labels': [f"M{i}" for i in range(-months, months + 1)],
                'historical': historical_clv,
                'predicted': predicted_clv_trend
            },
            'product_usage': {
                'labels': ['ATM', 'Internet Banking', 'SMS Banking', 'Credit Card'],
                'data': [
                    client['ACNTS_ATM_OPERN'] or 0,
                    client['ACNTS_INET_OPERN'] or 0,
                    client['ACNTS_SMS_OPERN'] or 0,
                    client['ACNTS_CR_CARDS_ALLOWED'] or 0
                ]
            },
            'loyalty': {
                'labels': [f"Month {i+1}" for i in range(6)],
                'points': [random.randint(50, 200) for _ in range(6)]  # Sample loyalty points data
            },
            'risk_factors': {
                'labels': ['Transaction Frequency', 'Digital Engagement', 'Product Utilization'],
                'data': [
                    100 if client['ACNTS_DORMANT_ACNT'] == 1 else 30,
                    max(0, 100 - engagement_score),
                    max(0, 100 - product_score)
                ]
            }
        }
        
        conn.close()
        return render_template('client_details.html',
                             client=client,
                             metrics=metrics,
                             retention_actions=retention_actions,
                             risk_factors=risk_factors,
                             chart_data_json=json.dumps(chart_data))
                             
    except Exception as e:
        print(f"Error loading client details: {str(e)}")
        if conn:
            conn.close()
        return jsonify({'error': 'Failed to load client details'}), 500

@app.route('/products')
@login_required
def products():
    conn = None
    try:
        conn = get_db()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Get filter parameters
        search = request.args.get('search', '').strip()
        limit = request.args.get('limit', type=int)
        
        # Build query with DISTINCT on product fields
        query = """
            SELECT DISTINCT 
                p.PRODUCT_CODE,
                p.PRODUCT_NAME,
                p.PRODUCT_GROUP_CODE,
                p.PRODUCT_CLASS,
                p.PRODUCT_FOR_DEPOSITS,
                p.PRODUCT_FOR_LOANS,
                p.PRODUCT_REVOKED_ON,
                (
                    SELECT json_group_object(
                        'charge',
                        json_object(
                            'CHARGES_CHG_AMT_CHOICE', c.CHARGES_CHG_AMT_CHOICE,
                            'CHARGES_FIXED_AMT', c.CHARGES_FIXED_AMT,
                            'CHARGES_CHG_CURR', c.CHARGES_CHG_CURR,
                            'CHARGES_CHGS_PERCENTAGE', c.CHARGES_CHGS_PERCENTAGE
                        )
                    )
                    FROM charges c 
                    WHERE c.CHARGES_PROD_CODE = p.PRODUCT_CODE
                    LIMIT 1
                ) as charge_info
            FROM products p 
            WHERE 1=1
        """
        params = []
        
        if search:
            query += """
                AND (
                    CAST(p.PRODUCT_CODE AS TEXT) LIKE ? OR
                    p.PRODUCT_NAME LIKE ? OR
                    p.PRODUCT_GROUP_CODE LIKE ? OR
                    p.PRODUCT_CLASS LIKE ?
                )
            """
            search_param = f"%{search}%"
            params.extend([search_param] * 4)
            
        if limit:
            query += " LIMIT ?"
            params.append(limit)
            
        cursor.execute(query, params)
        products = []
        for row in cursor.fetchall():
            product = dict(row)
            # Calculate lifecycle stage based on some metrics
            if not product['PRODUCT_REVOKED_ON']:
                product['lifecycle_stage'] = 'Growth'
            else:
                product['lifecycle_stage'] = 'Decline'
            
            # Parse charge information from JSON
            if product['charge_info']:
                charge_data = json.loads(product['charge_info'])
                product['charge'] = charge_data.get('charge')
            else:
                product['charge'] = None
            
            # Remove the JSON string from the final product object
            del product['charge_info']
            
            products.append(product)
            
        # Sample analysis data
        analysis = {
            'total_sales': 150000.00,
            'avg_clv': 2500.00,
            'revenue': 75000.00,
            'bundle_recommendations': [
                {
                    'name': 'Premium Bundle',
                    'description': 'High-value product combination',
                    'products': [
                        {'name': 'Premium Savings', 'adoption_rate': 45.5},
                        {'name': 'Investment Account', 'adoption_rate': 32.8}
                    ],
                    'revenue_increase': 25,
                    'pair_count': 150
                }
            ]
        }
        
        analysis_json = json.dumps({
            'sales': {
                'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
                'data': [1000, 1200, 1100, 1400, 1300, 1500]
            },
            'segments': {
                'labels': ['Premium', 'Standard', 'Basic', 'Trial'],
                'data': [40, 30, 20, 10]
            },
            'bundle_recommendations': analysis['bundle_recommendations']
        })
        
        conn.close()
        return render_template('products.html',
                             products=products,
                             search=search,
                             limit=limit,
                             analysis=analysis,
                             analysis_json=analysis_json)
                             
    except Exception as e:
        print(f"Error loading products: {str(e)}")
        if conn:
            conn.close()
        return jsonify({'error': 'Failed to load products'}), 500

@app.route('/clients')
@cache.cached(timeout=300, query_string=True)  # Cache for 5 minutes, include query params in cache key
def clients():
    if 'user' not in session or not session.get('authenticated'):
        return redirect(url_for('login'))
    conn = None
    try:
        # Get filter parameters
        status = request.args.get('status', '')
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('limit', 20, type=int)
        search = request.args.get('search', '').strip()
        
        conn = get_db()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        
        # First get total count with optimized query
        count_query = """SELECT COUNT(*) as total
FROM clients c
WHERE c.ACNTS_CLIENT_NUM IS NOT NULL
    AND c.ACNTS_OPENING_DATE IS NOT NULL"""
        
        # Pre-calculate status for better performance
        status_case = """
            CASE 
                WHEN c.ACNTS_DORMANT_ACNT = 1 THEN 'Inactive'
                WHEN c.ACNTS_INOP_ACNT = 1 THEN 'At Risk'
                ELSE 'Active'
            END
        """
        
        if status:
            count_query += f" AND {status_case} = ?"
        if search:
            count_query += """
                AND (
                    CAST(c.ACNTS_CLIENT_NUM AS TEXT) LIKE ? OR
                    c.ACNTS_AC_NAME1 LIKE ?
                )
            """
        
        count_params = []
        if status:
            count_params.append(status)
        if search:
            search_param = f"%{search}%"
            count_params.extend([search_param] * 2)
            
        cursor.execute(count_query, count_params)
        total_count = cursor.fetchone()[0]
        
        # Build optimized main query
        query = f"""
            WITH client_status AS (
                SELECT 
                    c.*,
                    {status_case} as status,
                    CASE 
                        WHEN c.ACNTS_PROD_CODE = 3102 THEN 'Premium'
                        WHEN c.ACNTS_PROD_CODE = 3002 THEN 'Business'
                        WHEN c.ACNTS_PROD_CODE = 3101 THEN 'Retail'
                        ELSE 'Other'
                    END as segment,
                    CASE 
                        WHEN c.ACNTS_PROD_CODE = 3102 THEN 5000
                        WHEN c.ACNTS_PROD_CODE = 3002 THEN 3000
                        WHEN c.ACNTS_PROD_CODE = 3101 THEN 1000
                        ELSE 1000
                    END * (1 + (julianday('now') - julianday(c.ACNTS_OPENING_DATE)) / 365.0 * 0.1) as clv
                FROM clients c
                WHERE c.ACNTS_CLIENT_NUM IS NOT NULL
                    AND c.ACNTS_OPENING_DATE IS NOT NULL
            )
            SELECT 
                cs.ACNTS_CLIENT_NUM as id,
                COALESCE(cs.ACNTS_AC_NAME1, '') || ' ' || COALESCE(cs.ACNTS_AC_NAME2, '') as name,
                cs.ACNTS_PROD_CODE,
                cs.ACNTS_DORMANT_ACNT,
                cs.ACNTS_INOP_ACNT,
                cs.ACNTS_LAST_TRAN_DATE,
                cs.segment,
                cs.status,
                cs.clv
            FROM client_status cs
            WHERE 1=1
        """
        
        params = []
        if status:
            query += " AND cs.status = ?"
            params.append(status)
        
        if search:
            query += """
                AND (
                    CAST(cs.ACNTS_CLIENT_NUM AS TEXT) LIKE ? OR
                    cs.ACNTS_AC_NAME1 LIKE ?
                )
            """
            search_param = f"%{search}%"
            params.extend([search_param] * 2)
        
        query += """
            ORDER BY cs.ACNTS_CLIENT_NUM ASC
            LIMIT ? OFFSET ?
        """
        
        # Calculate pagination
        offset = (page - 1) * per_page
        params.extend([per_page, offset])
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Calculate pagination
        total_pages = (total_count + per_page - 1) // per_page
        
        # Convert rows to list of dicts
        clients_data = [dict(row) for row in rows]
        
        conn.close()
        return render_template('clients.html',
                             clients=clients_data,
                             status=status,
                             per_page=per_page,
                             search=search,
                             current_page=page,
                             total_pages=total_pages,
                             total_count=total_count)
                             
    except Exception as e:
        error_msg = f"Error loading clients: {str(e)}"
        print(error_msg)
        import traceback
        print(traceback.format_exc())
        if conn:
            conn.close()
        return jsonify({'error': error_msg}), 500

@app.route('/reports', methods=['GET', 'POST'])
@login_required
def reports():
    # Get selected period (default to 1 month)
    selected_period = request.form.get('period', '1')
    months = int(selected_period)
    
    conn = None
    try:
        conn = get_db()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Calculate date range
        cursor.execute("""
            WITH date_range AS (
                SELECT date('now', '-' || ? || ' months') as start_date,
                       date('now') as end_date
            )
            SELECT 
                COUNT(DISTINCT c.ACNTS_CLIENT_NUM) as contributing_clients,
                SUM(CASE 
                    WHEN c.ACNTS_PROD_CODE = 3102 THEN 5000 * ?
                    WHEN c.ACNTS_PROD_CODE = 3002 THEN 3000 * ?
                    WHEN c.ACNTS_PROD_CODE = 3101 THEN 1000 * ?
                    ELSE 1000 * ?
                END) as total_revenue,
                SUM(CASE 
                    WHEN c.ACNTS_PROD_CODE IN (3102, 3002) THEN 2000 * ?
                    ELSE 500 * ?
                END) as total_assets,
                SUM(CASE 
                    WHEN c.ACNTS_DORMANT_ACNT = 1 THEN 300 * ?
                    ELSE 100 * ?
                END) as total_expenses,
                SUM(CASE 
                    WHEN c.ACNTS_INOP_ACNT = 1 THEN 200 * ?
                    ELSE 50 * ?
                END) as total_liabilities
            FROM clients c, date_range
            WHERE c.ACNTS_OPENING_DATE <= date_range.end_date
                AND (c.ACNTS_LAST_TRAN_DATE >= date_range.start_date OR c.ACNTS_LAST_TRAN_DATE IS NULL)
        """, (months, months, months, months, months, months, months, months, months, months, months))
        
        result = cursor.fetchone()
        
        # Calculate metrics based on query results
        total_revenue = result['total_revenue'] or 0
        total_assets = result['total_assets'] or 0
        total_expenses = result['total_expenses'] or 0
        total_liabilities = result['total_liabilities'] or 0
        
        revenue_metrics = {
            'net_total_revenue': total_revenue + total_assets - total_expenses - total_liabilities,
            'total_revenue': total_revenue,
            'total_assets': total_assets,
            'total_expenses': total_expenses,
            'total_liabilities': total_liabilities,
            'contributing_clients': result['contributing_clients'] or 0
        }
        
        # Sample churn metrics
        churn_metrics = {
            'churn_rate': 5.2,
            'at_risk_count': 45,
            'churned_value': 75000.00,
            'retention_rate': 94.8,
            'top_factors': [
                {
                    'name': 'Account Inactivity',
                    'impact': 2.5,
                    'description': 'Extended periods without transactions',
                    'severity': 75
                },
                {
                    'name': 'Digital Engagement',
                    'impact': 1.8,
                    'description': 'Low usage of digital banking services',
                    'severity': 60
                },
                {
                    'name': 'Product Utilization',
                    'impact': 1.2,
                    'description': 'Limited product relationships',
                    'severity': 45
                },
                {
                    'name': 'Balance Trends',
                    'impact': -0.3,
                    'description': 'Improving average balance levels',
                    'severity': 25
                }
            ]
        }
        
        # Sample segmentation data
        segmentation = {
            'segments': [
                {
                    'name': 'Premium',
                    'count': 250,
                    'percentage': 25.0,
                    'avg_revenue': 5000.00,
                    'total_value': 1250000.00,
                    'characteristics': [
                        'High transaction volume',
                        'Multiple product relationships',
                        'Regular digital banking users',
                        'Long-term customers'
                    ]
                },
                {
                    'name': 'Standard',
                    'count': 450,
                    'percentage': 45.0,
                    'avg_revenue': 2000.00,
                    'total_value': 900000.00,
                    'characteristics': [
                        'Moderate transaction frequency',
                        'Basic product portfolio',
                        'Mixed channel usage',
                        'Stable relationships'
                    ]
                },
                {
                    'name': 'Basic',
                    'count': 200,
                    'percentage': 20.0,
                    'avg_revenue': 800.00,
                    'total_value': 160000.00,
                    'characteristics': [
                        'Low transaction volume',
                        'Single product users',
                        'Limited digital engagement',
                        'Newer relationships'
                    ]
                },
                {
                    'name': 'New',
                    'count': 100,
                    'percentage': 10.0,
                    'avg_revenue': 400.00,
                    'total_value': 40000.00,
                    'characteristics': [
                        'Recent account openings',
                        'Basic services only',
                        'Building relationship',
                        'High growth potential'
                    ]
                }
            ],
            'chart_data': {
                'labels': ['Premium', 'Standard', 'Basic', 'New'],
                'data': [1250000.00, 900000.00, 160000.00, 40000.00]
            }
        }
        
        return render_template('reports.html',
                             selected_period=selected_period,
                             revenue_metrics=revenue_metrics,
                             churn_metrics=churn_metrics,
                             segmentation=segmentation)
                             
    except Exception as e:
        print(f"Error loading reports: {str(e)}")
        if conn:
            conn.close()
        return jsonify({'error': 'Failed to load reports'}), 500

def generate_pdf_report(template_name, **kwargs):
    """Generate a PDF report from a template"""
    # Configure pdfkit options
    options = {
        'page-size': 'Letter',
        'margin-top': '20mm',
        'margin-right': '20mm',
        'margin-bottom': '20mm',
        'margin-left': '20mm',
        'encoding': 'UTF-8',
        'no-outline': None,
        'enable-local-file-access': None
    }
    
    # Render the template
    rendered = render_template(template_name, **kwargs)
    
    # Generate PDF
    pdf = pdfkit.from_string(rendered, False, options=options)
    return pdf

@app.route('/download_revenue_csv')
@login_required
def download_revenue_csv():
    try:
        # Create CSV content
        csv_content = "Period,Net Revenue,Total Revenue,Total Assets,Total Expenses,Total Liabilities\n"
        
        # Get current metrics
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                SUM(CASE 
                    WHEN c.ACNTS_PROD_CODE = 3102 THEN 5000
                    WHEN c.ACNTS_PROD_CODE = 3002 THEN 3000
                    WHEN c.ACNTS_PROD_CODE = 3101 THEN 1000
                    ELSE 1000
                END) as total_revenue,
                COUNT(*) as client_count
            FROM clients c
            WHERE c.ACNTS_OPENING_DATE <= date('now')
        """)
        result = cursor.fetchone()
        
        total_revenue = result['total_revenue'] or 0
        total_assets = total_revenue * 0.4
        total_expenses = total_revenue * 0.3
        total_liabilities = total_revenue * 0.2
        net_revenue = total_revenue + total_assets - total_expenses - total_liabilities
        
        # Add data row
        csv_content += f"Current,{net_revenue},{total_revenue},{total_assets},{total_expenses},{total_liabilities}\n"
        
        # Create response
        response = make_response(csv_content)
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = 'attachment; filename=revenue_report.csv'
        return response
    except Exception as e:
        print(f"Error generating revenue CSV: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/download_churn_csv')
@login_required
def download_churn_csv():
    try:
        # Create CSV content
        csv_content = "Metric,Value\n"
        csv_content += "Churn Rate,5.2%\n"
        csv_content += "At Risk Accounts,45\n"
        csv_content += "Churned Value,$75000.00\n"
        csv_content += "Retention Rate,94.8%\n"
        
        # Create response
        response = make_response(csv_content)
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = 'attachment; filename=churn_report.csv'
        return response
    except Exception as e:
        print(f"Error generating churn CSV: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/download_segmentation_csv')
@login_required
def download_segmentation_csv():
    try:
        # Create CSV content
        csv_content = "Segment,Count,Percentage,Average Revenue,Total Value\n"
        
        segments = [
            {
                'name': 'Premium',
                'count': 250,
                'percentage': 25.0,
                'avg_revenue': 5000.00,
                'total_value': 1250000.00
            },
            {
                'name': 'Standard',
                'count': 450,
                'percentage': 45.0,
                'avg_revenue': 2000.00,
                'total_value': 900000.00
            },
            {
                'name': 'Basic',
                'count': 200,
                'percentage': 20.0,
                'avg_revenue': 800.00,
                'total_value': 160000.00
            },
            {
                'name': 'New',
                'count': 100,
                'percentage': 10.0,
                'avg_revenue': 400.00,
                'total_value': 40000.00
            }
        ]
        
        for segment in segments:
            csv_content += f"{segment['name']},{segment['count']},{segment['percentage']}%,${segment['avg_revenue']:.2f},${segment['total_value']:.2f}\n"
        
        # Create response
        response = make_response(csv_content)
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = 'attachment; filename=segmentation_report.csv'
        return response
    except Exception as e:
        print(f"Error generating segmentation CSV: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/download_report_pdf')
@login_required
def download_report_pdf():
    try:
        # Get the current period and metrics
        selected_period = request.args.get('period', '1')
        period_map = {'1': 'Last Month', '3': 'Last 3 Months', '6': 'Last 6 Months', '12': 'Last Year'}
        period_text = period_map.get(selected_period, 'Last Month')
        
        # Get report type
        report_type = request.args.get('type', 'full')
        filename = f"{report_type}_report.pdf"
        
        # Get current metrics from the reports route
        metrics = reports()
        if isinstance(metrics, tuple):  # Error case
            return metrics
            
        # Generate PDF using the template
        pdf = generate_pdf_report(
            'pdf_report.html',
            revenue_metrics=metrics.get('revenue_metrics', {}),
            churn_metrics=metrics.get('churn_metrics', {}),
            segmentation=metrics.get('segmentation', {}),
            period_text=period_text,
            datetime=datetime
        )
        
        # Create response
        response = make_response(pdf)
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
        return response
    except Exception as e:
        print(f"Error generating PDF: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/product/analytics')
@login_required
def product_analytics():
    try:
        # Return sample analytics data
        data = {
            'sales_trend': {
                'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
                'data': [1000, 1200, 1100, 1400, 1300, 1500]
            },
            'revenue_segments': {
                'labels': ['Premium', 'Standard', 'Basic', 'Trial'],
                'data': [40, 30, 20, 10]
            },
            'bundle_recommendations': [
                {
                    'name': 'Premium Bundle',
                    'description': 'High-value product combination',
                    'products': [
                        {'name': 'Premium Savings', 'adoption_rate': 45.5},
                        {'name': 'Investment Account', 'adoption_rate': 32.8}
                    ],
                    'revenue_increase': 25,
                    'pair_count': 150
                }
            ]
        }
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/favicon.ico')
def favicon():
    try:
        return send_from_directory(
            os.path.join(app.root_path, 'static'),
            'favicon.ico', 
            mimetype='image/vnd.microsoft.icon'
        )
    except:
        return '', 404  # Return empty response with 404 if favicon.ico doesn't exist

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
