import imaplib, email, time, os, re, threading
from collections import deque
from datetime import datetime

import gspread, paho.mqtt.client as mqtt
from oauth2client.service_account import ServiceAccountCredentials
from gtts import gTTS
from flask import Flask, jsonify
from dotenv import load_dotenv

# ---------- ENV ----------
load_dotenv()

EMAIL_ID       = os.getenv("EMAIL_ID")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

MQTT_BROKER    = os.getenv("MQTT_BROKER")
MQTT_PORT      = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME  = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD  = os.getenv("MQTT_PASSWORD")
MQTT_TOPIC     = os.getenv("MQTT_TOPIC", "Zenorc")

GOOGLE_CREDS   = os.getenv("GOOGLE_CREDS")
SHEET_URL      = os.getenv("SHEET_URL")

SEARCH_STRINGS      = ("₹5", "Rs 5")
COOLDOWN_SECONDS    = 40
RENDER_ENV          = os.getenv("RENDER", "false").lower() == "true"

seen_uids: set[bytes] = set()
queue: deque[str]     = deque()
status: dict[str, str] = {}
last_processed_time   = 0

# ---------- TTS ----------
def play_tts(msg: str):
    if RENDER_ENV:
        print(f"[RENDER TTS disabled] {msg}")
        return
    try:
        from pygame import mixer
        tts = gTTS(msg, lang="en")
        tts.save("tts.mp3")
        mixer.init()
        mixer.music.load("tts.mp3")
        mixer.music.play()
        while mixer.music.get_busy():
            time.sleep(0.1)
        mixer.quit()
        os.remove("tts.mp3")
    except Exception as e:
        print("TTS Error:", e)

# ---------- Google Sheet ----------
def log_payment(txn: str, amt="5"):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS, scope)
        sheet = gspread.authorize(creds).open_by_url(SHEET_URL).sheet1
        now = datetime.now()
        sheet.append_row([txn, amt, now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S")])
        print(f"Logged {txn} to Google Sheet")
    except Exception as e:
        print("Google Sheets Error:", e)

# ---------- MQTT ----------
def send_mqtt(retries=3, delay=5):
    for attempt in range(1, retries + 1):
        try:
            client = mqtt.Client(protocol=mqtt.MQTTv311)
            client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
            client.tls_set()
            client.connect(MQTT_BROKER, MQTT_PORT)
            client.publish(MQTT_TOPIC, "paid")
            client.disconnect()
            print("MQTT message sent")
            return True
        except Exception as e:
            print(f"MQTT Error (attempt {attempt}):", e)
            if attempt < retries:
                time.sleep(delay)
    return False

# ---------- Gmail Payment Scanner ----------
def poll_email() -> str | None:
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_ID, EMAIL_PASSWORD)
        mail.select("inbox")
        typ, data = mail.search(None, "(UNSEEN)")
        uid_list = (data[0] or b"").split()[-20:][::-1]

        for uid in uid_list:
            if uid in seen_uids:
                continue
            typ, msg_data = mail.fetch(uid, "(RFC822)")
            msg = email.message_from_bytes(msg_data[0][1])

            raw_subject = msg["Subject"] or ""
            subject = str(email.header.make_header(email.header.decode_header(raw_subject)))
            body = ""
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_payload(decode=True).decode(errors="ignore")
                    break

            if any(s in subject for s in SEARCH_STRINGS) or any(s in body for s in SEARCH_STRINGS):
                seen_uids.add(uid)
                ref = re.search(r"Reference\s*No\.?[:\s]*(\d+)", body)
                txn = ref.group(1) if ref else f"TXN{int(time.time())}"
                print(f"Found ₹5 payment → TXN: {txn}")
                return txn
        return None
    except Exception as e:
        print("Gmail Error:", e)
        return None

# ---------- Processor Thread ----------
def processor():
    global last_processed_time
    while True:
        if queue:
            now = time.time()
            if now - last_processed_time >= COOLDOWN_SECONDS:
                txn = queue.popleft()
                status[txn] = "Processing"
                print(f"⚙ Processing: {txn}")

                ok = send_mqtt()
                if ok:
                    print("TTS: Payment of five rupees received")
                    status[txn] = "Completed"
                    print(f"Completed: {txn}")
                else:
                    print("TTS: Payment failed to reach the system.")
                    status[txn] = "Failed"

                last_processed_time = time.time()
            else:
                time.sleep(1)
        else:
            time.sleep(1)

# ---------- Flask Web UI ----------
app = Flask(__name__)

@app.route("/")
def home():
    return (
        "<h3>Zenorc Payment Listener</h3>"
        f"<p>Status: running</p>"
        f"<p>Queued: {len(queue)}</p>"
    )

@app.route("/health")
def health():
    return jsonify(ok=True)

# ---------- Main ----------
def main_loop():
    while True:
        print("Scanning inbox for payments...")
        txn = poll_email()
        if txn and txn not in status:
            log_payment(txn)
            queue.append(txn)
            status[txn] = "Queued"
            print(f"Queued: {txn}")
        time.sleep(2)

if __name__ == "__main__":
    threading.Thread(target=processor, daemon=True).start()
    threading.Thread(target=main_loop, daemon=True).start()
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
