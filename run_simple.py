import os
import sys

print("Starting Flask application...")
print("Working directory:", os.getcwd())

# Change to the data_processor directory
os.chdir('data_processor')
print("Changed to directory:", os.getcwd())

# Add the current directory to Python path
sys.path.append(os.getcwd())

# Import and run the Flask app
try:
    from app import app
    print("App imported successfully")
    app.run(debug=True, port=5000)
except Exception as e:
    print(f"Error starting app: {e}")
    sys.exit(1) 