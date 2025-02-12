from flask import Flask
import os

# Create Flask app
app = Flask(__name__)

# Set template and static folders
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data_processor', 'templates'))
static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data_processor', 'static'))

app.template_folder = template_dir
app.static_folder = static_dir
app.secret_key = 'your_secret_key_here'

print(f"Template folder: {app.template_folder}")
print(f"Template exists: {os.path.exists(os.path.join(app.template_folder, 'landing.html'))}")

# Import routes
from data_processor.app import landing_page, dashboard, clients, products, reports, login, logout

# Register routes
app.add_url_rule('/', 'landing_page', landing_page)
app.add_url_rule('/landing', 'landing', landing_page)
app.add_url_rule('/dashboard', 'dashboard', dashboard)
app.add_url_rule('/clients', 'clients', clients)
app.add_url_rule('/products', 'products', products)
app.add_url_rule('/reports', 'reports', reports)
app.add_url_rule('/login', 'login', login, methods=['GET', 'POST'])
app.add_url_rule('/logout', 'logout', logout)

if __name__ == '__main__':
    print("Starting app...")
    app.run(host='127.0.0.1', port=5000, debug=True)
