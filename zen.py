from flask import Flask
import os
import threading
import zenorc  # Import your main logic file

app = Flask(__name__)

@app.route('/')
def home():
    return "Zenorc system is live!"

# Start the Zenorc processing thread
def start_bot():
    zenorc.start()  # You need to put your Zenorc main loop inside a start() function

if __name__ == "_main_":
    threading.Thread(target=start_bot, daemon=True).start()
    port = int(os.getenv("PORT", 10000))
    app.run(host='0.0.0.0', port=port)  # ✅ Correct version
