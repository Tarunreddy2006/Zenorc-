from __future__ import annotations

import email
import imaplib
import os
import re
import uuid
import threading
import time
from collections import deque
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import gspread
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from flask import Flask, jsonify
from oauth2client.service_account import ServiceAccountCredentials

# ────────────────────────── ENV ──────────────────────────
load_dotenv()

EMAIL_ID            = os.getenv("EMAIL_ID")
EMAIL_PASSWORD      = os.getenv("EMAIL_PASSWORD")

MQTT_BROKER         = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT           = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME       = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD       = os.getenv("MQTT_PASSWORD")
MQTT_TOPIC          = os.getenv("MQTT_TOPIC", "Zenorc")
client_id           = f"zenorc-{uuid.uuid4().hex[:8]}"

GSHEET_URL          = os.getenv("GSHEET_URL")
GSHEET_CREDS_PATH   = os.getenv("SHEET_CREDS_PATH", "/etc/secrets/Zenorc.json")

SEARCH_STRINGS      = tuple(s.strip() for s in os.getenv("SEARCH_STRINGS", "₹5,Rs 5").split(","))
COOLDOWN_SECONDS    = int(os.getenv("COOLDOWN_SECONDS", "40"))

# ───────────────────────── STATE ─────────────────────────
seen_uids: set[bytes]  = set()         # Gmail UIDs handled in this session
seen_txn_ids: set[str] = set()         # Reference Numbers already logged
queue: deque[str]      = deque()       # FIFO queue of txns awaiting MQTT
status: dict[str, str] = {}            # txn_id → Queued / Processing / Completed / Failed
last_processed: float  = 0.0           # time.time() of last successful MQTT publish
imap_lock              = threading.Lock()

# ──────────────────────── UTILITIES ──────────────────────
def debug(msg: str) -> None:
    """Print instantly‑flushed log line (handy on Render)."""
    print(msg, flush=True)

# ──────────────── GOOGLE SHEETS HANDLERS ────────────────
def _get_sheet() -> gspread.Worksheet:
    if not GSHEET_URL:
        raise RuntimeError("GSHEET_URL env var missing")
    if not os.path.isfile(SHEET_CREDS_PATH):
        raise FileNotFoundError(f"Credentials not found: {SHEET_CREDS_PATH}")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = ServiceAccountCredentials.from_json_keyfile_name(SHEET_CREDS_PATH, scope)
    client = gspread.authorize(creds)
    return client.open_by_url(GSHEET_URL).sheet1

def _load_existing_txns() -> set[str]:
    """Populate in‑memory set with already‑logged Reference Numbers."""
    try:
        sheet = _get_sheet()
        return set(sheet.col_values(1))  # first column = txn_id
    except Exception as exc:
        debug(f"Sheets init warning: {exc}")
        return set()

seen_txn_ids = _load_existing_txns()

def log_payment(txn_id: str, amount: str = "5") -> None:
    """Append txn→sheet with IST timestamp."""
    try:
        sheet = _get_sheet()
        now   = datetime.now(ZoneInfo("Asia/Mumbai"))
        sheet.append_row([
            txn_id,
            amount,
            now.strftime("%Y-%m-%d"),
            now.strftime("%H:%M:%S"),
        ])
        seen_txn_ids.add(txn_id)
        debug(f"✔ Logged {txn_id} to Google Sheets")
    except Exception as exc:
        debug(f"Sheets Error: {exc}")

# ──────────────────────── MQTT PUBLISH ───────────────────────
def send_mqtt(max_retries: int = 3, retry_delay: int = 5) -> bool:
    """Publish 'paid' once to MQTT_TOPIC. Return True on success."""
    for attempt in range(1, max_retries + 1):
        try:
            client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
     
            if MQTT_USERNAME:
                client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

            auth_failed = False

            def on_connect(c, userdata, flags, rc, properties=None):
                nonlocal auth_failed
                debug(f"   ↳ MQTT connect rc={rc}")
                if rc == mqtt.CONNACK_REFUSED_NOT_AUTHORIZED:
                    debug("MQTT NOT_AUTHORIZED – check credentials/ACLs")
                    auth_failed = True

            client.on_connect = on_connect
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=20)
            client.loop_start()
            time.sleep(1)  # wait for connect callback

            if auth_failed:
                client.loop_stop()
                client.disconnect()
                return False

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

# ─────────────────────── EMAIL POLLING ───────────────────────
def _imap_login() -> imaplib.IMAP4_SSL:
    if not (EMAIL_ID and EMAIL_PASSWORD):
        raise RuntimeError("EMAIL_ID / EMAIL_PASSWORD env vars missing")
    imap = imaplib.IMAP4_SSL("imap.gmail.com")
    imap.login(EMAIL_ID, EMAIL_PASSWORD)
    return imap

def _extract_txn_id(body: str) -> str:
    m = re.search(r"Reference\s*No\.?[:\s]*(\d+)", body)
    return m.group(1) if m else f"TXN{int(time.time())}"

def poll_email() -> Optional[str]:
    """Return a new txn_id or None."""
    with imap_lock:
        try:
            with _imap_login() as mail:
                mail.select("inbox")
                _, data = mail.search(None, "(UNSEEN)")
                for uid in (data[0] or b"").split()[-30:][::-1]:  # latest 30, newest first
                    if uid in seen_uids:
                        continue

                    _, msg_data = mail.fetch(uid, "(RFC822)")
                    msg = email.message_from_bytes(msg_data[0][1])

                    subject_raw = msg.get("Subject", "")
                    subject     = str(email.header.make_header(email.header.decode_header(subject_raw)))

                    body = ""
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            payload = part.get_payload(decode=True) or b""
                            body = payload.decode(errors="ignore")
                            break

                    if any(s in subject for s in SEARCH_STRINGS) or any(s in body for s in SEARCH_STRINGS):
                        txn_id = _extract_txn_id(body)
                        seen_uids.add(uid)
                        mail.store(uid, "+FLAGS", "\\Seen")  # mark as read to avoid re-scan
                        debug(f"UID {uid.decode()} matched → {txn_id}")
                        return txn_id
        except Exception as exc:
            debug(f"Gmail Error: {exc}")
    return None

# ────────────────────── PROCESSOR THREAD ──────────────────────
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

# ─────────────────────────── FLASK APP ───────────────────────────
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

# ───────────────────────────── MAIN LOOP ─────────────────────────────
def main_loop() -> None:
    while True:
        debug("\nScanning inbox for payments…")
        txn_id = poll_email()
        if (
            txn_id
            and txn_id not in status
            and txn_id not in seen_txn_ids      # ensures no duplicates
        ):
            status[txn_id] = "Queued"
            log_payment(txn_id)
            queue.append(txn_id)
            debug(f"Queued {txn_id}")
        time.sleep(3)

if __name__ == "__main__":
    threading.Thread(target=processor,  daemon=True).start()
    threading.Thread(target=main_loop, daemon=True).start()

    port = int(os.environ.get("PORT", "5000"))  # Render passes $PORT
    app.run(host="0.0.0.0", port=port, debug=False)