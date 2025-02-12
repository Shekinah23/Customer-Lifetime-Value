from data_processor.app import app

if __name__ == '__main__':
    print("Starting app...")
    app.run(host='127.0.0.1', port=5015, debug=True)
