from flask import Flask, render_template
import os

app = Flask(__name__)

# Set template folder explicitly
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data_processor', 'templates'))
app.template_folder = template_dir

print(f"Template folder: {app.template_folder}")
print(f"Template exists: {os.path.exists(os.path.join(app.template_folder, 'landing.html'))}")

@app.route('/')
def home():
    return render_template('landing.html')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5014, debug=True)
