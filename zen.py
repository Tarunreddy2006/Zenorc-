from flask import Flask
import os
import threading
import zenorc  # Import your main logic file

app = Flask(__name__)

@app.route('/')
def home():
    return "Zenorc system is live!"

# Start the Zenorc processing threads

def start_bot():
    threading.Thread(target=zenorc.processor, daemon=True).start()
    threading.Thread(target=zenorc.main_loop, daemon=True).start()

if __name__ == "__main__":
    start_bot()
    port = int(os.getenv("PORT", "10000"))
    app.run(host='0.0.0.0', port=port)  # ✅ Correct version
