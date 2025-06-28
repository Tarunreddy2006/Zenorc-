from __future__ import annotations

import email, imaplib, os, re, threading, time, uuid
from collections import deque
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import gspread, paho.mqtt.client as mqtt
from dotenv import load_dotenv
from flask import Flask, jsonify
from oauth2client.service_account import ServiceAccountCredentials

# ───────────────────── ENV ─────────────────────
load_dotenv()

EMAIL_ID       = os.getenv("EMAIL_ID")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

MQTT_BROKER   = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT     = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_TOPIC    = os.getenv("MQTT_TOPIC", "Zenorc")
CLIENT_ID     = f"zenorc-{uuid.uuid4().hex[:8]}"

GSHEET_URL        = os.getenv("GSHEET_URL")
GSHEET_CREDS_PATH = os.getenv("GSHEET_CREDS_PATH", "/etc/secrets/Zenorc.json")

SEARCH_STRINGS   = tuple(s.strip() for s in os.getenv("SEARCH_STRINGS", "₹5,Rs 5").split(","))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "40"))

# ─────────────────── STATE ───────────────────
seen_uids: set[bytes]   = set()    # Gmail UIDs already handled
seen_txn_ids: set[str]  = set()    # Reference numbers already logged
queue: deque[str]       = deque()  # Waiting payments
status: dict[str, str]  = {}       # txn_id → Queued / Processing / Completed / Failed
last_processed: float   = 0.0      # epoch time of last MQTT send
imap_lock               = threading.Lock()

# ────────────────── UTILS ──────────────────
def debug(msg: str) -> None:
    print(msg, flush=True)

def tz_mumbai() -> ZoneInfo:
    try:
        return ZoneInfo("Asia/Mumbai")
    except (ZoneInfoNotFoundError, KeyError):
        return ZoneInfo("UTC")

# ───────────── Google Sheets ─────────────
def _get_sheet():
    if not GSHEET_URL:
        raise RuntimeError("GSHEET_URL env var missing")
    if not os.path.isfile(GSHEET_CREDS_PATH):
        raise FileNotFoundError(f"Credentials not found: {GSHEET_CREDS_PATH}")

    scope  = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_name(GSHEET_CREDS_PATH, scope)
    client = gspread.authorize(creds)
    return client.open_by_url(GSHEET_URL).sheet1

def _bootstrap_txns() -> set[str]:
    try:
        return set(_get_sheet().col_values(1))
    except Exception as exc:
        debug(f"Sheets bootstrap warning: {exc}")
        return set()

seen_txn_ids = _bootstrap_txns()

def log_payment(txn_id: str, amount: str = "5") -> None:
    try:
        sheet = _get_sheet()
        now   = datetime.now(tz_mumbai())
        sheet.append_row([txn_id, amount,
                          now.strftime("%Y-%m-%d"),
                          now.strftime("%H:%M:%S")])
        seen_txn_ids.add(txn_id)
        debug(f"Logged {txn_id} to Google Sheets")
    except Exception as exc:
        debug(f"Sheets Error: {exc}")

# ───────────── MQTT PUBLISH ─────────────
def send_mqtt(max_retries: int = 3, retry_delay: int = 5) -> bool:
    for attempt in range(1, max_retries + 1):
        try:
            connected = threading.Event()

            client = mqtt.Client(client_id=CLIENT_ID,
                                 protocol=mqtt.MQTTv311,
                                 callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

            if MQTT_USERNAME:
                client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

            def on_connect(c, _u, _f, rc, _p=None):
                debug(f"   ↳ MQTT connect rc={rc}")
                if rc == 0:
                    connected.set()

            client.on_connect = on_connect
            client.tls_set()
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=20)
            client.loop_start()

            if not connected.wait(timeout=5):
                raise TimeoutError("MQTT connect timeout")

            res, mid = client.publish(MQTT_TOPIC, "paid", qos=1)
            debug(f"   ↳ MQTT publish result = {mqtt.error_string(res)} (mid={mid})")

            client.loop_stop(); client.disconnect()
            return res == mqtt.MQTT_ERR_SUCCESS

        except Exception as exc:
            debug(f"MQTT attempt {attempt}/{max_retries} failed: {exc}")
            if attempt < max_retries:
                time.sleep(retry_delay)
    return False

# ───────────── Gmail Polling ─────────────
def _imap_login() -> imaplib.IMAP4_SSL:
    if not (EMAIL_ID and EMAIL_PASSWORD):
        raise RuntimeError("EMAIL_ID and/or EMAIL_PASSWORD missing")
    imap = imaplib.IMAP4_SSL("imap.gmail.com")
    imap.login(EMAIL_ID, EMAIL_PASSWORD)
    return imap

def _extract_txn_id(body: str) -> str:
    m = re.search(r"Reference\s*No\.?[:\s]*(\d+)", body)
    return m.group(1) if m else f"TXN{int(time.time())}"

def poll_email() -> Optional[str]:
    with imap_lock:
        try:
            with _imap_login() as mail:
                mail.select("inbox")
                _, data = mail.search(None, "(UNSEEN)")
                for uid in (data[0] or b"").split()[-30:][::-1]:
                    if uid in seen_uids:
                        continue
                    _, msg_data = mail.fetch(uid, "(RFC822)")
                    msg = email.message_from_bytes(msg_data[0][1])

                    subject = str(email.header.make_header(
                        email.header.decode_header(msg.get("Subject", ""))))
                    body = ""
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body = (part.get_payload(decode=True) or b"").decode(errors="ignore")
                            break

                    if any(s in subject for s in SEARCH_STRINGS) or any(s in body for s in SEARCH_STRINGS):
                        txn_id = _extract_txn_id(body)
                        seen_uids.add(uid)
                        mail.store(uid, "+FLAGS", "\\Seen")
                        debug(f"UID {uid.decode()} → {txn_id}")
                        return txn_id
        except Exception as exc:
            debug(f"Gmail Error: {exc}")
    return None

# ───────────── Payment Processor ─────────────
def processor():
    global last_processed
    while True:
        if queue:
            now = time.time()
            if now - last_processed >= COOLDOWN_SECONDS:
                txn_id = queue.popleft()
                status[txn_id] = "Processing"
                debug(f"⚙ Processing {txn_id}")

                ok = send_mqtt()
                status[txn_id] = "Completed" if ok else "Failed"
                debug(("✔" if ok else "❌") + f" Completed {txn_id}")
                last_processed = time.time()
            else:
                debug(f"Cool‑down {int(COOLDOWN_SECONDS - (time.time() - last_processed))} s")
        time.sleep(1)

# ───────────── Flask App ─────────────
app = Flask(__name__)

@app.route("/")
def root():
    return (
        "<h3>Zenorc Payment Processor</h3>"
        f"<p>Status: running</p>"
        f"<p>Queue length: {len(queue)}</p>"
    )

@app.route("/health")
def health():
    return jsonify(ok=True)

# ───────────── Main Loop ─────────────
def main_loop():
    debug("Scanning inbox for payments…")  # printed once at start
    while True:
        txn_id = poll_email()
        if txn_id and txn_id not in status and txn_id not in seen_txn_ids:
            status[txn_id] = "Queued"
            log_payment(txn_id)
            queue.append(txn_id)
            debug(f"Queued {txn_id}")
            debug("Scanning inbox for payments…")  # re‑print after each new payment
        time.sleep(3)

if __name__ == "__main__":
    threading.Thread(target=processor,  daemon=True).start()
    threading.Thread(target=main_loop, daemon=True).start()

    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
