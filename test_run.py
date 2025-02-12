import os
from flask import Flask, render_template

# Set up template and static paths
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data_processor', 'templates'))
static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data_processor', 'static'))

print(f"Template directory: {template_dir}")
print(f"Static directory: {static_dir}")

# Create Flask app with explicit template path
app = Flask(__name__, 
           template_folder=template_dir,
           static_folder=static_dir)

@app.route('/')
def home():
    try:
        template_path = os.path.join(template_dir, 'landing.html')
        print(f"\nAttempting to render landing page:")
        print(f"- Template exists: {os.path.exists(template_path)}")
        with open(template_path, 'r') as f:
            content = f.read()
            print(f"- Template content length: {len(content)} bytes")
        return render_template('landing.html')
    except Exception as e:
        print(f"Error: {str(e)}")
        return str(e), 500

if __name__ == '__main__':
    print("Starting test server...")
    app.run(host='127.0.0.1', port=5013, debug=True)
