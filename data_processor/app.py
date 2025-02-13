from flask import Flask, render_template, request, redirect, url_for, session, jsonify, Response
from functools import wraps
import os
import sqlite3
import json

from datetime import timedelta

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

print(f"Template folder: {app.template_folder}")
print(f"Static folder: {app.static_folder}")
print(f"Logo path: {os.path.join(app.static_folder, 'logo', 'logo.png')}")
print(f"Logo exists: {os.path.exists(os.path.join(app.static_folder, 'logo', 'logo.png'))}")

def get_db():
    """Get database connection with optimized settings"""
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'banking_data.db')
    
    try:
        # Configure connection for better performance
        conn = sqlite3.connect(db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        
        # Enable WAL mode and other optimizations
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA cache_size=-2000')  # Use 2MB of cache
        conn.execute('PRAGMA temp_store=MEMORY')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA mmap_size=2147483648')  # 2GB memory map
        
        cursor = conn.cursor()
        
        # Check if tables exist
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('clients', 'products')")
        table_count = cursor.fetchone()[0]
        
        if table_count < 2:
            print("Tables not found, initializing database")
            try:
                schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'schema.sql')
                with open(schema_path, 'r') as f:
                    conn.executescript(f.read())
                conn.commit()
                print("Database schema initialized successfully")
            except Exception as e:
                print(f"Error initializing database schema: {e}")
                return None
        
        # Verify data exists
        cursor.execute("SELECT COUNT(*) FROM clients")
        client_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM products")
        product_count = cursor.fetchone()[0]
        
        if client_count == 0 and product_count == 0:
            print("Database is empty")
            return None
        
        return conn
        
    except sqlite3.Error as e:
        print(f"Database error: {str(e)}")
        return None
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

@app.route('/static/<path:filename>')
def serve_static(filename):
    print(f"Serving static file: {filename}")
    try:
        return app.send_static_file(filename)
    except Exception as e:
        print(f"Error serving static file: {str(e)}")
        return str(e), 500

@app.route('/favicon.ico')
def favicon():
    return '', 204  # Return no content for favicon requests

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
            'data': [30, 45, 15, 10]
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
    
    top_clients = [
        {'name': 'Client A', 'type': 'Corporate', 'revenue': 50000.00},
        {'name': 'Client B', 'type': 'Individual', 'revenue': 35000.00}
    ]
    
    top_products = [
        {'name': 'Premium Account', 'clients': 150, 'revenue': 75000.00},
        {'name': 'Investment Plan', 'clients': 100, 'revenue': 50000.00}
    ]
    
    chart_data_json = json.dumps(chart_data)
    return render_template('dashboard.html', 
                         metrics=metrics,
                         revenue_metrics=revenue_metrics,
                         chart_data=chart_data,
                         chart_data_json=chart_data_json,
                         top_clients=top_clients,
                         top_products=top_products)

@app.route('/clients')
@login_required
def clients():
    conn = None
    try:
        # Get filter parameters
        status = request.args.get('status', '')
        limit = request.args.get('limit', type=int)
        search = request.args.get('search', '').strip()
        
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'banking_data.db')
        print(f"Connecting to database at: {db_path}")
        print(f"Database exists: {os.path.exists(db_path)}")
        
        conn = get_db()
        if conn is None:
            print("Database connection failed")
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        
        # Check if clients table exists and has data
        cursor.execute("SELECT COUNT(*) FROM clients")
        count = cursor.fetchone()[0]
        print(f"Found {count} clients in database")
        
        # Build query with filters
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
                CASE 
                    WHEN c.ACNTS_DORMANT_ACNT = 1 THEN 'Inactive'
                    WHEN c.ACNTS_INOP_ACNT = 1 THEN 'At Risk'
                    ELSE 'Active'
                END as status,
                c.ACNTS_OPENING_DATE,
                c.ACNTS_LAST_TRAN_DATE,
                c.ACNTS_AC_TYPE,
                c.ACNTS_PROD_CODE
            FROM clients c
            WHERE c.ACNTS_CLIENT_NUM IS NOT NULL
        """
        params = []
        
        if status:
            query += """
                AND CASE 
                    WHEN c.ACNTS_DORMANT_ACNT = 1 THEN 'Inactive'
                    WHEN c.ACNTS_INOP_ACNT = 1 THEN 'At Risk'
                    ELSE 'Active'
                END = ?
            """
            params.append(status)
        
        if search:
            query += """
                AND (
                    CAST(c.ACNTS_CLIENT_NUM AS TEXT) LIKE ? OR
                    c.ACNTS_AC_NAME1 LIKE ?
                )
            """
            search_param = f"%{search}%"
            params.extend([search_param] * 2)
        
        query += " ORDER BY c.ACNTS_CLIENT_NUM ASC"
        
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        
        print("\nExecuting query:", query.replace('\n', ' '))
        print("With params:", params)
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            print(f"Query returned {len(rows)} rows")
            if len(rows) > 0:
                print("Sample row:", dict(rows[0]))
        except sqlite3.Error as e:
            print(f"SQL Error: {str(e)}")
            raise
        
        # Convert rows to list of dicts
        clients_data = []
        for row in rows:
            client = dict(row)
            
            # Calculate rough CLV based on account type and age
            account_age_days = 0
            if client['ACNTS_OPENING_DATE']:
                cursor.execute("SELECT julianday('now') - julianday(?) as age_days", 
                             (client['ACNTS_OPENING_DATE'],))
                age_result = cursor.fetchone()
                if age_result:
                    account_age_days = age_result['age_days']
            
            # Base CLV calculation
            base_clv = {
                3102: 5000,  # Premium accounts
                3002: 3000,  # Business accounts
                3101: 1000   # Retail accounts
            }.get(client['ACNTS_PROD_CODE'], 1000)
            
            # Adjust CLV based on account age (years)
            account_age_years = account_age_days / 365.0
            client['clv'] = base_clv * (1 + account_age_years * 0.1)
            
            clients_data.append(client)
        
        conn.close()
        
        print("Clients data:", clients_data[:2] if clients_data else "No clients data")
        print("Status:", status)
        print("Limit:", limit)
        print("Search:", search)
        
        rendered = render_template('clients.html',
                                 clients=clients_data,
                                 status=status,
                                 limit=limit,
                                 search=search)
        return rendered
                             
    except Exception as e:
        print(f"Error loading clients: {str(e)}")
        if conn:
            conn.close()
        return jsonify({'error': 'Failed to load clients'}), 500

from product_analytics import (
    calculate_product_performance,
    get_revenue_by_segment,
    analyze_product_lifecycle,
    get_bundling_recommendations,
    get_sales_trend_data,
    get_default_lifecycle_stages,
    get_default_sales_trend_data
)

@app.route('/api/product/lifecycle/<product_code>')
@login_required
def get_product_lifecycle(product_code):
    lifecycle_stages = analyze_product_lifecycle(product_code)
    return jsonify(lifecycle_stages)

def get_product_data(conn, limit=None, search=None):
    """Get product data with optional filtering"""
    cursor = conn.cursor()
    params = []
    query = """
        SELECT 
           p.PRODUCT_CODE, 
           p.PRODUCT_NAME, 
           p.PRODUCT_GROUP_CODE, 
           p.PRODUCT_CLASS,
           p.PRODUCT_FOR_DEPOSITS, 
           p.PRODUCT_FOR_LOANS, 
           p.PRODUCT_REVOKED_ON,
           c.CHARGES_CHG_AMT_CHOICE, 
           c.CHARGES_FIXED_AMT, 
           c.CHARGES_CHGS_PERCENTAGE,
           c.CHARGES_CHG_CURR
        FROM products p
        LEFT JOIN charges c ON p.PRODUCT_CODE = c.CHARGES_PROD_CODE
        WHERE 1=1
    """
    
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
    
    query += " ORDER BY p.PRODUCT_NAME"
    
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    
    cursor.execute(query, params)
    return cursor.fetchall()

@app.route('/products')
@login_required
def products():
    conn = None
    try:
        # Get filter parameters
        limit = request.args.get('limit', type=int) or 50  # Default to 50 products
        search = request.args.get('search', '').strip()
        
        conn = get_db()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            # Get product data
            rows = get_product_data(conn, limit, search)
            if not rows:
                print("No products found")
                return render_template('products.html',
                                    products=[],
                                    limit=limit,
                                    search=search,
                                    analysis={},
                                    analysis_json="{}")
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return jsonify({'error': 'Database error occurred'}), 500
        
        # Convert rows to list of dicts with nested charge info
        products_data = []
        for row in rows:
            product = dict(row)
            if row['CHARGES_CHG_AMT_CHOICE'] is not None:
                product['charge'] = {
                    'CHARGES_CHG_AMT_CHOICE': row['CHARGES_CHG_AMT_CHOICE'],
                    'CHARGES_FIXED_AMT': row['CHARGES_FIXED_AMT'],
                    'CHARGES_CHGS_PERCENTAGE': row['CHARGES_CHGS_PERCENTAGE'],
                    'CHARGES_CHG_CURR': row['CHARGES_CHG_CURR']
                }
            else:
                product['charge'] = None
            
            # Get lifecycle stage for each product
            lifecycle_stages = analyze_product_lifecycle(product['PRODUCT_CODE'])
            product['lifecycle_stage'] = next((stage['name'] for stage in lifecycle_stages if stage['current']), 'Introduction')
            
            products_data.append(product)
        
        # Get performance metrics
        performance = calculate_product_performance()
        segment_distribution = performance.get('segment_distribution', {
            'Premium': 0,
            'Business': 0,
            'Retail': 0
        })
        
        # Prepare analysis data
        analysis = {
            'total_sales': performance['total_sales'],
            'avg_clv': performance['avg_clv'],
            'revenue': performance['revenue'],
            'segment_distribution': segment_distribution,
            'lifecycle_stages': get_default_lifecycle_stages("Growth")
        }
        
        # Get chart data
        revenue_segments = get_revenue_by_segment()
        sales_trend = get_default_sales_trend_data()
        bundle_recommendations = get_bundling_recommendations()
        
        chart_data = {
            'sales': sales_trend,
            'segments': revenue_segments,
            'segment_distribution': segment_distribution,
            'bundle_recommendations': bundle_recommendations
        }
        
        # Update analysis with bundle recommendations
        analysis['bundle_recommendations'] = bundle_recommendations
        
        conn.close()
        
        return render_template('products.html',
                             products=products_data,
                             limit=limit,
                             search=search,
                             analysis=analysis,
                             analysis_json=json.dumps(chart_data))
                             
    except Exception as e:
        print(f"Error loading products: {str(e)}")
        if conn:
            conn.close()
        return jsonify({'error': 'Failed to load products'}), 500

@app.route('/api/product/analytics')
@login_required
def get_product_analytics():
    """API endpoint for loading remaining analytics data asynchronously"""
    try:
        # Get bundle recommendations
        bundle_recommendations = get_bundling_recommendations()
        
        # Get revenue segments and sales trend data
        revenue_segments = get_revenue_by_segment()
        sales_trend = get_sales_trend_data()
        
        # Return all analytics data
        return jsonify({
            'bundle_recommendations': bundle_recommendations,
            'revenue_segments': revenue_segments,
            'sales_trend': sales_trend
        })
    except Exception as e:
        print(f"Error loading additional analytics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/reports')
@login_required
def reports():
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
    return render_template('reports.html', revenue_metrics=revenue_metrics)

@app.route('/download_revenue_csv')
@login_required
def download_revenue_csv():
    return Response(
        'Date,Revenue\n2024-01-01,1000\n',
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=revenue.csv'}
    )

@app.route('/logout')
def logout():
    print(f"Logging out user: {session.get('user')}")
    session.clear()  # Clear all session data
    response = redirect(url_for('login'))
    response.delete_cookie('user')
    print("Session cleared and user logged out")
    return response

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        # For demo purposes, accept any non-empty username/password
        if username and password:
            # Set session variables
            session.clear()  # Clear any existing session
            session['user'] = username
            session['authenticated'] = True
            session.permanent = True  # Make session permanent
            print(f"Login successful for user: {username}")
            print(f"Session data: {session}")
            
            # Redirect to the page they were trying to access, or landing page
            next_page = request.args.get('next')
            if next_page and next_page.startswith('/'):  # Ensure URL is relative
                return redirect(next_page)
            return redirect(url_for('landing_page'))
        else:
            print("Login failed: Missing username or password")
            return render_template('login.html', error="Please enter both username and password")
            
    elif 'user' in session and session.get('authenticated'):
        print(f"User already logged in: {session['user']}")
        return redirect(url_for('landing_page'))
        
    # Store the page they were trying to access
    if request.args.get('next'):
        session['next'] = request.args.get('next')
        
    return render_template('login.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
