from flask import Flask, render_template, jsonify, request, abort, redirect, url_for, session, Response
from functools import wraps
import os
import json
import sqlite3
from contextlib import closing

app = Flask(__name__)  # Ensure app is defined here
app.secret_key = 'your_secret_key_here'  # Set a secret key for session management

def get_db():
    try:
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'banking_data.db')
        print(f"Connecting to database at: {db_path}")  # Debug print
        conn = sqlite3.connect(db_path)
        # Test query
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM clients")
        count = cursor.fetchone()[0]
        print(f"Found {count} clients in database")  # Debug print
        return conn
    except Exception as e:
        print(f"Database error: {str(e)}")  # Debug print
        raise

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/login', methods=['GET', 'POST'])  # Added login route
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        session['user'] = username
        return redirect(url_for('dashboard'))
    return render_template('login.html')  # Render login template

if __name__ == '__main__':
    app.run(debug=True, port=5000)
