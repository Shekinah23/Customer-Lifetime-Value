from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
    return "Hello from test server!"

if __name__ == '__main__':
    print("Starting test server...")
    app.run(debug=True, port=5004)
