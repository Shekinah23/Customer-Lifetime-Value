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
        'revenue_per_customer': 250.00
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

@app.route('/products')
@login_required
def products():
    # Get filter parameters
    product_type = request.args.get('type', '')
    limit = request.args.get('limit', type=int)
    
    conn = get_db()
    if conn is None:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor()
    
    # Build query based on filters
    query = """
        SELECT PRODUCT_CODE, PRODUCT_NAME, PRODUCT_GROUP_CODE, PRODUCT_CLASS,
               PRODUCT_FOR_DEPOSITS, PRODUCT_FOR_LOANS, PRODUCT_REVOKED_ON
        FROM products
        WHERE 1=1
    """
    params = []
    
    if product_type:
        if product_type == 'deposit':
            query += " AND PRODUCT_FOR_DEPOSITS = 1"
        elif product_type == 'loan':
            query += " AND PRODUCT_FOR_LOANS = 1"
    
    query += " ORDER BY PRODUCT_CODE"
    
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    
    cursor.execute(query, params)
    products_data = cursor.fetchall()
    conn.close()
    
    return render_template('products.html', 
                         products=products_data,
                         type=product_type,
                         limit=limit)

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
