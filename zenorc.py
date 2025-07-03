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

# ╭─ ENV ──────────────────────────────────────────────────────────╮
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

SEARCH_STRINGS = tuple(s.strip().lower() for s in os.getenv("SEARCH_STRINGS", "₹5,Rs 5,INR 5").split(","))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "40"))
# ╰────────────────────────────────────────────────────────────────╯

# ╭─ STATE ───────────────────────────────────────────────────────╮
seen_uids: set[bytes]  = set()
seen_txn_ids: set[str] = set()
queue: deque[str]      = deque()
status: dict[str, str] = {}
last_processed         = 0.0
imap_lock              = threading.Lock()
# ╰────────────────────────────────────────────────────────────────╯

# ╭─ UTILS ───────────────────────────────────────────────────────╮
def log(msg: str, level: str = "INFO"):
    print(f"{level} {msg}", flush=True)

def tz_mumbai():
    try:
        return ZoneInfo("Asia/Mumbai")
    except:
        return ZoneInfo("UTC")
# ╰────────────────────────────────────────────────────────────────╯

# ╭─ SHEETS ──────────────────────────────────────────────────────╮
def _sheet() -> gspread.Worksheet:
    if not GSHEET_URL:
        raise RuntimeError("GSHEET_URL env var missing")
    if not os.path.isfile(GSHEET_CREDS_PATH):
        raise FileNotFoundError(f"Credentials not found: {GSHEET_CREDS_PATH}")

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GSHEET_CREDS_PATH, scope)
    client = gspread.authorize(creds)
    return client.open_by_url(GSHEET_URL).sheet1

def _bootstrap_txns():
    try:
        return set(_sheet().col_values(1))
    except Exception as e:
        log(f"Sheets bootstrap failed: {e}", "WARN")
        return set()

seen_txn_ids = _bootstrap_txns()

def log_payment(txn_id: str, amount: str = "5"):
    try:
        sheet = _sheet()
        now = datetime.now(tz_mumbai())
        sheet.append_row([txn_id, amount, now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S")])
        seen_txn_ids.add(txn_id)
        log(f"Logged {txn_id} to Google Sheets")
    except Exception as e:
        log(f"Sheets Error: {e}", "ERROR")
# ╰────────────────────────────────────────────────────────────────╯

# ╭─ MQTT ────────────────────────────────────────────────────────╮
def send_mqtt(max_retries: int = 3, retry_delay: int = 5):
    for attempt in range(1, max_retries + 1):
        try:
            connected = threading.Event()
            client = mqtt.Client(
                client_id=CLIENT_ID,
                protocol=mqtt.MQTTv311,
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2
            )
            if MQTT_USERNAME:
                client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

            def on_connect(c, *_):
                log("↳ MQTT connected rc=0")
                connected.set()

            client.on_connect = on_connect
            client.tls_set()
            client.connect(MQTT_BROKER, MQTT_PORT)
            client.loop_start()

            if not connected.wait(timeout=5):
                raise TimeoutError("MQTT connection timeout")

            rc, mid = client.publish(MQTT_TOPIC, "paid", qos=1)
            log(f"↳ MQTT publish result = {mqtt.error_string(rc)} (mid={mid})", "INFO" if rc == 0 else "ERROR")

            client.loop_stop()
            client.disconnect()
            return rc == mqtt.MQTT_ERR_SUCCESS
        except Exception as e:
            log(f"MQTT attempt {attempt} failed: {e}", "WARN")
            if attempt < max_retries:
                time.sleep(retry_delay)
    return False
# ╰────────────────────────────────────────────────────────────────╯

# ╭─ EMAIL HANDLER ───────────────────────────────────────────────╮
def _imap_login() -> imaplib.IMAP4_SSL:
    if not (EMAIL_ID and EMAIL_PASSWORD):
        raise RuntimeError("EMAIL_ID / EMAIL_PASSWORD missing")
    imap = imaplib.IMAP4_SSL("imap.gmail.com")
    imap.login(EMAIL_ID, EMAIL_PASSWORD)
    return imap


def _extract_txn_id(body: str) -> str:
    """
    Extracts transaction ID from both formats:
    1. Reference No.: 845009839012
    2. UPI transaction reference number is 845009839012
    """
    patterns = [
        r"Reference\s*(?:No\.?|number)?\s*[:\-]?\s*(\d{8,})",  # Format 1
        r"transaction reference number\s*(?:is)?\s*[:\-]?\s*(\d{8,})",  # Format 2
    ]
    for pat in patterns:
        m = re.search(pat, body, re.I)
        if m:
            return m.group(1)
    return f"TXN{int(time.time())}"


def _looks_like_credit(body: str) -> bool:
    """
    Returns True only for valid credit messages (avoiding debits).
    """
    body_l = body.lower()
    return (
        "credited" in body_l
        and "debited" not in body_l
        and (
            "successfully credited" in body_l
            or "has been credited" in body_l
        )
    )


def poll_email() -> Optional[str]:
    """
    Poll Gmail inbox for unseen ₹5 credit emails. Returns txn_id if new valid payment found.
    """
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

                    # Extract plain text body
                    body = ""
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body = (part.get_payload(decode=True) or b"").decode(errors="ignore")
                            break

                    # Check if it's a valid credit message
                    if _looks_like_credit(body):
                        txn_id = _extract_txn_id(body)
                        seen_uids.add(uid)
                        mail.store(uid, "+FLAGS", "\\Seen")
                        log(f"UID {uid.decode()} → {txn_id}")
                        return txn_id

        except Exception as e:
            log(f"Gmail Error: {e}", "ERROR")
    return None
#╰────────────────────────────────────────────────────────────────╯

# ╭─ PROCESSOR ───────────────────────────────────────────────────╮
def processor():
    global last_processed
    while True:
        if queue:
            now = time.time()
            if now - last_processed >= COOLDOWN_SECONDS:
                txn_id = queue.popleft()
                status[txn_id] = "Processing"
                log(f"⚙ Processing {txn_id}")
                ok = send_mqtt()
                status[txn_id] = "Completed" if ok else "Failed"
                log(("✔" if ok else "❌") + f" Completed {txn_id}")
                last_processed = time.time()
            else:
                remain = int(COOLDOWN_SECONDS - (time.time() - last_processed))
                log(f"Cooldown: {remain}s")
        time.sleep(1)
# ╰────────────────────────────────────────────────────────────────╯

# ╭─ FLASK ───────────────────────────────────────────────────────╮
app = Flask(__name__)

@app.route("/")
def root():
    return (
        "<h3>Zenorc Payment Processor</h3>"
        f"<p>Status: running</p><p>Queue length: {len(queue)}</p>"
    )

@app.route("/health")
def health():
    return jsonify(ok=True)
# ╰────────────────────────────────────────────────────────────────╯

# ╭─ MAIN LOOP ───────────────────────────────────────────────────╮
def main_loop():
    log("Scanning inbox for payments…")
    while True:
        txn_id = poll_email()
        if txn_id and txn_id not in status and txn_id not in seen_txn_ids:
            status[txn_id] = "Queued"
            log_payment(txn_id)
            queue.append(txn_id)
            log(f"Queued {txn_id}")
            log("Scanning inbox for payments…")
        time.sleep(3)
# ╰────────────────────────────────────────────────────────────────╯

if __name__ == "__main__":
    threading.Thread(target=processor, daemon=True).start()
    threading.Thread(target=main_loop, daemon=True).start()

    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
