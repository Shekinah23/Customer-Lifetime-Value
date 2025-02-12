from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    return "Hello from Flask!"

if __name__ == '__main__':
    print("Starting Flask app on port 5009...")
    app.run(host='127.0.0.1', port=5009, debug=True, use_reloader=False)
