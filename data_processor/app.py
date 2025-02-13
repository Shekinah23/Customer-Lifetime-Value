from flask import Flask, render_template, request, redirect, url_for, session, jsonify, Response
from functools import wraps
import os
import sqlite3
import json

app = Flask(__name__, 
           template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), 'templates')),
           static_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), 'static')),
           static_url_path='/static')
app.secret_key = 'dev_secret_key_123'

print(f"Template folder: {app.template_folder}")
print(f"Static folder: {app.static_folder}")
print(f"Logo path: {os.path.join(app.static_folder, 'logo', 'logo.png')}")
print(f"Logo exists: {os.path.exists(os.path.join(app.static_folder, 'logo', 'logo.png'))}")

def get_db():
    try:
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'banking_data.db')
        print(f"Connecting to database at: {db_path}")
        if not os.path.exists(db_path):
            print("Database file not found, will be created on first write")
            return None
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM clients")
            count = cursor.fetchone()[0]
            print(f"Found {count} clients in database")
            return conn
        except sqlite3.OperationalError:
            print("Database exists but tables not initialized")
            return None
    except Exception as e:
        print(f"Database error: {str(e)}")
        return None

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            print(f"\nRedirecting to login - no user in session")
            print(f"Current session: {session}")
            return redirect(url_for('login'))
        print(f"\nUser authenticated: {session['user']}")
        return f(*args, **kwargs)
    return decorated_function

@app.route('/static/<path:filename>')
def serve_static(filename):
    print(f"Serving static file: {filename}")
    try:
        return app.send_static_file(filename)
    except Exception as e:
        print(f"Error serving static file: {str(e)}")
        return str(e), 500

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
    # Get filter parameters
    status = request.args.get('status', '')
    limit = request.args.get('limit', type=int)
    
    # Sample client data
    all_clients = [
        {'id': 1, 'name': 'John Smith', 'type': 'Individual', 'segment': 'Premium', 'clv': 50000.00, 'status': 'Active'},
        {'id': 2, 'name': 'ABC Corp', 'type': 'Corporate', 'segment': 'Premium', 'clv': 150000.00, 'status': 'Active'},
        {'id': 3, 'name': 'Jane Doe', 'type': 'Individual', 'segment': 'Standard', 'clv': 25000.00, 'status': 'At Risk'},
        {'id': 4, 'name': 'XYZ Ltd', 'type': 'Corporate', 'segment': 'Basic', 'clv': 75000.00, 'status': 'Inactive'},
        {'id': 5, 'name': 'Bob Wilson', 'type': 'Individual', 'segment': 'Basic', 'clv': 15000.00, 'status': 'Active'},
        {'id': 6, 'name': 'Tech Corp', 'type': 'Corporate', 'segment': 'Premium', 'clv': 200000.00, 'status': 'Active'},
        {'id': 7, 'name': 'Sarah Brown', 'type': 'Individual', 'segment': 'Standard', 'clv': 35000.00, 'status': 'At Risk'},
        {'id': 8, 'name': 'Global Inc', 'type': 'Corporate', 'segment': 'Premium', 'clv': 180000.00, 'status': 'Active'},
        {'id': 9, 'name': 'Mike Johnson', 'type': 'Individual', 'segment': 'Basic', 'clv': 20000.00, 'status': 'Inactive'},
        {'id': 10, 'name': 'First Bank', 'type': 'Corporate', 'segment': 'Premium', 'clv': 250000.00, 'status': 'Active'}
    ]
    
    # Filter by status if specified
    if status:
        clients_data = [client for client in all_clients if client['status'] == status]
    else:
        clients_data = all_clients
    
    # Apply limit if specified
    if limit:
        clients_data = sorted(clients_data, key=lambda x: x['clv'], reverse=True)[:limit]
    
    return render_template('clients.html', 
                         clients=clients_data,
                         status=status,
                         limit=limit)

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

@app.route('/products')
@login_required
def products():
    # Get filter parameters
    limit = request.args.get('limit', type=int)
    search = request.args.get('search', '').strip()
    
    conn = get_db()
    if conn is None:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor()
    
    params = []
    
    # Build query based on filters with DISTINCT to remove duplicates
    query = """
        SELECT DISTINCT
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
    
    # Add search condition if search term provided
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
        params.extend([search_param] * 4)  # Add search parameter 4 times for each OR condition
    
    query += " ORDER BY p.PRODUCT_NAME"
    
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    # Get all product codes first
    product_codes = [row['PRODUCT_CODE'] for row in rows]
    
    # Get all lifecycle stages in a single batch
    lifecycle_stages = {}
    if product_codes:
        # Build query to get all product metrics at once
        metrics_query = """
        WITH ProductMetrics AS (
            SELECT 
                c.ACNTS_PROD_CODE,
                MIN(c.ACNTS_OPENING_DATE) as first_account_date,
                COUNT(DISTINCT c.ACNTS_ACCOUNT_NUMBER) as total_accounts,
                SUM(CASE WHEN c.ACNTS_AC_TYPE = 1 THEN 1 ELSE 0 END) as premium_accounts,
                SUM(CASE WHEN c.ACNTS_AC_TYPE = 2 THEN 1 ELSE 0 END) as business_accounts
            FROM clients c
            WHERE c.ACNTS_PROD_CODE IN ({})
            GROUP BY c.ACNTS_PROD_CODE
        ),
        MonthlyStats AS (
            SELECT 
                c.ACNTS_PROD_CODE,
                strftime('%Y-%m', c.ACNTS_OPENING_DATE) as month,
                COUNT(*) as total_accounts
            FROM clients c
            WHERE c.ACNTS_OPENING_DATE IS NOT NULL
            AND c.ACNTS_PROD_CODE IN ({})
            AND c.ACNTS_OPENING_DATE >= date('now', '-12 months')
            GROUP BY c.ACNTS_PROD_CODE, month
        )
        SELECT 
            p.PRODUCT_CODE,
            pm.first_account_date,
            pm.total_accounts,
            pm.premium_accounts,
            pm.business_accounts,
            GROUP_CONCAT(ms.month || ':' || ms.total_accounts) as monthly_stats
        FROM products p
        LEFT JOIN ProductMetrics pm ON p.PRODUCT_CODE = pm.ACNTS_PROD_CODE
        LEFT JOIN MonthlyStats ms ON p.PRODUCT_CODE = ms.ACNTS_PROD_CODE
        WHERE p.PRODUCT_CODE IN ({})
        GROUP BY p.PRODUCT_CODE
        """.format(
            ','.join(f"'{code}'" for code in product_codes),
            ','.join(f"'{code}'" for code in product_codes),
            ','.join(f"'{code}'" for code in product_codes)
        )
        
        # Create a new cursor for metrics query
        metrics_cursor = conn.cursor()
        metrics_cursor.execute(metrics_query)
        metrics_data = metrics_cursor.fetchall()
        
        # Calculate lifecycle stages for all products using the same connection
        for metrics in metrics_data:
            product_code = metrics['PRODUCT_CODE']
            
            # Calculate product age in months
            if metrics['first_account_date']:
                metrics_cursor.execute("SELECT julianday('now') - julianday(?) as age_days", 
                                    (metrics['first_account_date'],))
                age_months = metrics_cursor.fetchone()['age_days'] / 30.44
            else:
                age_months = 0
            
            # New products start in Introduction
            if age_months < 6:
                lifecycle_stages[product_code] = "Introduction"
                continue
            
            # Products with few accounts
            total_accounts = metrics['total_accounts'] or 0
            if total_accounts < 50:
                lifecycle_stages[product_code] = "Introduction" if age_months < 12 else "Decline"
                continue
            
            # Analyze growth trends
            monthly_stats = {}
            if metrics['monthly_stats']:
                for stat in metrics['monthly_stats'].split(','):
                    month, count = stat.split(':')
                    monthly_stats[month] = int(count)
            
            # Calculate growth rates
            if monthly_stats:
                months = sorted(monthly_stats.keys(), reverse=True)
                recent_total = sum(monthly_stats[m] for m in months[:6] if m in monthly_stats)
                older_total = sum(monthly_stats[m] for m in months[6:] if m in monthly_stats)
                
                if older_total > 0:
                    growth_rate = ((recent_total - older_total) / older_total * 100)
                    
                    # Calculate premium ratio
                    premium_ratio = (metrics['premium_accounts'] or 0) / total_accounts if total_accounts > 0 else 0
                    business_ratio = (metrics['business_accounts'] or 0) / total_accounts if total_accounts > 0 else 0
                    
                    # Determine stage
                    if growth_rate > 30 or (growth_rate > 20 and premium_ratio > 0.3):
                        lifecycle_stages[product_code] = "Growth"
                    elif growth_rate < -15 or (growth_rate < -10 and total_accounts < 100):
                        lifecycle_stages[product_code] = "Decline"
                    elif abs(growth_rate) <= 15 and (premium_ratio > 0.3 or business_ratio > 0.4):
                        lifecycle_stages[product_code] = "Maturity"
                    else:
                        lifecycle_stages[product_code] = "Growth" if growth_rate > 0 else "Maturity"
                else:
                    lifecycle_stages[product_code] = "Introduction"
            else:
                lifecycle_stages[product_code] = "Introduction"
    
    # Convert rows to list of dicts with nested charge info and lifecycle stage
    products_data = []
    for row in rows:
        product = dict(row)
        # Create nested charge object if charge data exists
        if row['CHARGES_CHG_AMT_CHOICE'] is not None:
            product['charge'] = {
                'CHARGES_CHG_AMT_CHOICE': row['CHARGES_CHG_AMT_CHOICE'],
                'CHARGES_FIXED_AMT': row['CHARGES_FIXED_AMT'],
                'CHARGES_CHGS_PERCENTAGE': row['CHARGES_CHGS_PERCENTAGE'],
                'CHARGES_CHG_CURR': row['CHARGES_CHG_CURR']
            }
        else:
            product['charge'] = None
        
        # Get lifecycle stage from pre-calculated results
        product['lifecycle_stage'] = lifecycle_stages.get(product['PRODUCT_CODE'], 'N/A')
        products_data.append(product)
    
    try:
        # Close database connection now that we're done with all queries
        conn.close()
        # Get performance metrics (includes segment distribution)
        performance = calculate_product_performance()
        segment_distribution = performance.get('segment_distribution', {
            'Premium': 0,
            'Business': 0,
            'Retail': 0
        })
        
        # Prepare analysis data with minimal metrics for fast initial load
        analysis = {
            'total_sales': performance['total_sales'],
            'avg_clv': performance['avg_clv'],
            'revenue': performance['revenue'],
            'segment_distribution': segment_distribution,
            'lifecycle_stages': get_default_lifecycle_stages("Growth"),
            'bundle_recommendations': get_bundling_recommendations()  # Load recommendations immediately
        }
        
        # Get initial data
        revenue_segments = get_revenue_by_segment()
        sales_trend = get_default_sales_trend_data()
        bundle_recommendations = get_bundling_recommendations()
        
        # Prepare initial chart data
        chart_data = {
            'sales': sales_trend,
            'segments': revenue_segments,
            'segment_distribution': segment_distribution
        }

        # Update analysis with bundle recommendations
        analysis['bundle_recommendations'] = bundle_recommendations
        
        return render_template('products.html', 
                             products=products_data,
                             limit=limit,
                             search=search,
                             analysis=analysis,
                             analysis_json=json.dumps(chart_data))
                             
    except Exception as e:
        print(f"Error loading product analytics: {str(e)}")
        return jsonify({'error': 'Failed to load product analytics'}), 500

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
    session.pop('user', None)
    response = redirect(url_for('login'))
    response.delete_cookie('user')
    return response

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        session['user'] = username
        return redirect(url_for('landing_page'))
    return render_template('login.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
