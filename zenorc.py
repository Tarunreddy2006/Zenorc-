from __future__ import annotations

import imaplib
import email
import os
import re
import threading
import time
from collections import deque
from datetime import datetime
from typing import Optional

import gspread
import paho.mqtt.client as mqtt
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from flask import Flask, jsonify

# ───────────────────────────────── ENV ──────────────────────────────────
load_dotenv()

EMAIL_ID: str | None = os.getenv("EMAIL_ID")
EMAIL_PASSWORD: str | None = os.getenv("EMAIL_PASSWORD")

MQTT_BROKER: str = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT: int = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME: Optional[str] = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD: Optional[str] = os.getenv("MQTT_PASSWORD")
MQTT_TOPIC: str = os.getenv("MQTT_TOPIC", "Zenorc")

SHEET_URL: str | None = os.getenv("GSHEET_URL")
GSHEET_CREDS_PATH: str = os.getenv("GSHEET_CREDS_PATH", "/etc/secrets/Zenorc.json")

SEARCH_STRINGS: tuple[str, ...] = tuple(s.strip() for s in os.getenv("SEARCH_STRINGS", "₹5,Rs 5").split(","))
COOLDOWN_SECONDS: int = int(os.getenv("COOLDOWN_SECONDS", "40"))

# ────────────────────────────── INTERNAL STATE ──────────────────────────
seen_uids: set[bytes] = set()
queue: deque[str] = deque()
status: dict[str, str] = {}  # txn_id → state
last_processed: float = 0.0
imap_lock = threading.Lock()

# ─────────────────────────────── UTILITIES ──────────────────────────────

def debug(msg: str) -> None:
    print(msg, flush=True)

# ───────────────────────── GOOGLE SHEETS LOGGER ─────────────────────────

def _get_gsheet() -> gspread.Worksheet:
    if not SHEET_URL:
        raise RuntimeError("SHEET_URL env not set")
    if not os.path.isfile(SHEET_CREDS_PATH):
        raise FileNotFoundError(f"Google creds not found: {GSHEET_CREDS_PATH}")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GSHEET_CREDS_PATH, scope)
    sheet = gspread.authorize(creds).open_by_url(SHEET_URL).sheet1
    return sheet

def log_payment(txn_id: str, amount: str = "5") -> None:
    try:
        sheet = _get_gsheet()
        now = datetime.now()
        sheet.append_row([
            txn_id,
            amount,
            now.strftime("%Y-%m-%d"),
            now.strftime("%H:%M:%S"),
        ])
        debug(f"✔ Logged {txn_id} to Google Sheets")
    except Exception as e:
        debug(f"Sheets Error: {e}")

# ──────────────────────────────── MQTT PUBLISH ──────────────────────────

def send_mqtt(max_retries: int = 3, retry_delay: int = 5) -> bool:
    """Publish "paid" once. Returns True on success."""
    for attempt in range(1, max_retries + 1):
        try:
            client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            client.tls_set()  # uses certifi CAs
            if MQTT_USERNAME:
                client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

            def on_connect(cli, userdata, flags, reason_code, properties=None):
                debug(f"   ↳ MQTT connected rc={reason_code}")

            client.on_connect = on_connect
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=20)
            client.loop_start()

            result, mid = client.publish(MQTT_TOPIC, "paid", qos=1)
            debug(f"   ↳ MQTT publish result = {mqtt.error_string(result)} (mid={mid})")

            client.loop_stop()
            client.disconnect()
            return result == mqtt.MQTT_ERR_SUCCESS
        except Exception as exc:
            debug(f"MQTT attempt {attempt}/{max_retries} failed: {exc}")
            if attempt < max_retries:
                time.sleep(retry_delay)
    return False

# ─────────────────────────────── EMAIL POLLING ──────────────────────────

def get_imap() -> imaplib.IMAP4_SSL:
    if not (EMAIL_ID and EMAIL_PASSWORD):
        raise RuntimeError("EMAIL_ID/EMAIL_PASSWORD env vars not set")
    imap = imaplib.IMAP4_SSL("imap.gmail.com")
    imap.login(EMAIL_ID, EMAIL_PASSWORD)
    return imap

def extract_txn_id(body: str) -> str:
    ref = re.search(r"Reference\s*No\.?[:\s]*(\d+)", body)
    return ref.group(1) if ref else f"TXN{int(time.time())}"

def poll_email() -> Optional[str]:
    """Return a txn_id if a new payment mail is found, otherwise None."""
    with imap_lock:
        try:
            with get_imap() as mail:
                mail.select("inbox")
                typ, data = mail.search(None, "(UNSEEN)")
                uid_list = (data[0] or b"").split()[-25:][::-1]  # latest 25
                for uid in uid_list:
                    if uid in seen_uids:
                        continue
                    typ, msg_data = mail.fetch(uid, "(RFC822)")
                    msg = email.message_from_bytes(msg_data[0][1])

                    subj_raw = msg.get("Subject", "")
                    subject = str(email.header.make_header(email.header.decode_header(subj_raw)))

                    body = ""
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            payload = part.get_payload(decode=True) or b""
                            body = payload.decode(errors="ignore")
                            break

                    if any(s in subject for s in SEARCH_STRINGS) or any(s in body for s in SEARCH_STRINGS):
                        txn_id = extract_txn_id(body)
                        seen_uids.add(uid)
                        debug(f"Email UID {uid.decode()} matched → {txn_id}")
                        return txn_id
        except Exception as exc:
            debug(f"Gmail Error: {exc}")
    return None

# ───────────────────────────── PROCESSOR THREAD ─────────────────────────

def processor() -> None:
    global last_processed
    while True:
        if queue:
            now = time.time()
            if now - last_processed >= COOLDOWN_SECONDS:
                txn_id = queue.popleft()
                status[txn_id] = "Processing"
                debug(f"⚙ Processing {txn_id}")

                success = send_mqtt()
                status[txn_id] = "Completed" if success else "Failed"
                debug(("✔" if success else "❌") + f" Completed {txn_id}")
                last_processed = time.time()
            else:
                remaining = int(COOLDOWN_SECONDS - (now - last_processed))
                debug(f"Cool‑down {remaining}s")
        time.sleep(1)

# ──────────────────────────────── FLASK APP ─────────────────────────────
app = Flask(__name__)

@app.route("/")
def root():
    return (
        "<h3>Zenorc Payment Listener</h3>"
        f"<p>Status: running</p>"
        f"<p>Queue length: {len(queue)}</p>"
    )

@app.route("/health")
def health():
    return jsonify(ok=True)

# ────────────────────────────────── MAIN ─────────────────────────────────

def main_loop() -> None:
    while True:
        debug("\nScanning inbox for payments…")
        txn_id = poll_email()
        if txn_id and txn_id not in status:
            queue.append(txn_id)
            status[txn_id] = "Queued"
            log_payment(txn_id)
            debug(f"Queued {txn_id}")
        time.sleep(3)

if __name__ == "__main__":
    threading.Thread(target=processor, daemon=True).start()
    threading.Thread(target=main_loop, daemon=True).start()

    # Flask/Gunicorn will pass $PORT on Render
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
