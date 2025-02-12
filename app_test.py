from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
    return "Hello from Flask!"

if __name__ == '__main__':
    print("Starting Flask app on localhost:5007...")
    app.run(host='localhost', port=5007, debug=True)
