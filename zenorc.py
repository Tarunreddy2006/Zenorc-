from __future__ import annotations

import os
import re
import json
import time
import uuid
import email
import imaplib
import threading
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import certifi
import gspread
import paho.mqtt.client as mqtt
from dotenv import load_dotenv, dotenv_values
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, jsonify, request
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

try:
    import schedule
except Exception:
    schedule = None

try:
    from waitress import serve
except Exception:
    serve = None

# -------------------
# Environment loading
# -------------------
# Load default .env and optional Zenorc.env in cwd if present
load_dotenv()
zenorc_env_path = Path("Zenorc.env")
if zenorc_env_path.exists():
    load_dotenv(zenorc_env_path)

# -------------------
# Global configuration
# -------------------
GSHEET_URL = os.getenv("GSHEET_URL", "")
GSHEET_CREDS_PATH = os.getenv("GSHEET_CREDS_PATH", "Zenorc.json")

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "Zenorc")

COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "40"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1.0"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "10"))
MAX_BATCH = int(os.getenv("MAX_BATCH", "500"))
DEFAULT_AMOUNT = os.getenv("AMOUNT", "5")

# Configs directory (per-client envs)
CONFIGS_DIR = Path(os.getenv("CONFIGS_DIR", "/workspace/configs"))

# Backup file
LOCAL_BACKUP_FILE = os.getenv("LOCAL_BACKUP_FILE", "backups.json")

# Allowed notification senders
ALLOWED_SENDERS: set[str] = {
    "alerts@hdfcbank.com",
    "donotreply@hdfcbank.com",
    "hdfc.alerts@hdfcbank.net",
    "instaalerts@hdfcbank.com",
    "alerts@icicibank.com",
    "donotreply@icicibank.com",
    "icici.alerts@icicibank.com",
    "alerts@axisbank.com",
    "alerts@yesbank.in",
    "noreply@yesbank.in",
    "alerts@sbi.co.in",
    "noreply@sbi.co.in",
    "ealerts@kotak.com",
    "alerts@kotak.com",
    "alerts@idfcfirstbank.com",
    "donotreply@idfcfirstbank.com",
    "donotreply@aubank.in",
    "alerts@aubank.in",
    "alerts@canarabank.com",
    "alerts@pnb.co.in",
    "alerts@federalbank.co.in",
    "tradertarun38@gmail.com",
    "tharun.aiml24@cmrit.ac.in",
}

# -------------------
# Shared state
# -------------------
seen_txn_ids: set[str] = set()  # global set of transaction ids already in sheet

# items: {"machine_id","txn_id","remarks","amount", "mqtt_topic"}
global_queue: deque[dict] = deque()

# status by txn_id: "Queued" | "Processing" | "Completed" | "Failed"
global_status: dict[str, str] = {}

# user flags written by webhook (active/inactive subscription)
user_flags: dict[str, bool] = {}

# When we last processed an item (for cooldown)
last_processed = 0.0

# Shared buffer for sheet rows
log_buffer: list[list[str]] = []
buffer_lock = threading.Lock()

# Backup logging throttling
backup_logged_once = False

# -------------------
# Utils
# -------------------
def log(msg: str, level: str = "INFO"):
    print(f"{level} {msg}", flush=True)


def tz_mumbai():
    try:
        if ZoneInfo:
            return ZoneInfo("Asia/Kolkata")
    except Exception:
        pass
    return datetime.now().astimezone().tzinfo


def col_idx_to_letter(idx: int) -> str:
    """0-based index -> Excel column letters (0 -> A, 26 -> AA)."""
    idx += 1
    letters = ""
    while idx > 0:
        idx, remainder = divmod(idx - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters

# -------------------
# Backup handling
# -------------------

def save_backup():
    global backup_logged_once
    with buffer_lock:
        if not log_buffer:
            return
        try:
            existing = []
            if os.path.exists(LOCAL_BACKUP_FILE):
                try:
                    with open(LOCAL_BACKUP_FILE, "r", encoding="utf-8") as f:
                        content = f.read().strip()
                        if content:
                            existing = json.loads(content)
                except Exception:
                    existing = []
            existing.extend(log_buffer)
            with open(LOCAL_BACKUP_FILE, "w", encoding="utf-8") as f:
                json.dump(existing, f)
            if not backup_logged_once:
                log(f"Saved {len(log_buffer)} rows to local backup", "INFO")
                backup_logged_once = True
        except Exception as e:
            log(f"save_backup error: {e}", "ERROR")


def restore_backup():
    global log_buffer
    if not os.path.exists(LOCAL_BACKUP_FILE):
        try:
            with open(LOCAL_BACKUP_FILE, "w", encoding="utf-8") as f:
                json.dump([], f)
            log("Created new backup.json file (empty).", "INFO")
        except Exception as e:
            log(f"Failed creating backup file: {e}", "ERROR")
        return
    try:
        with open(LOCAL_BACKUP_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                log("Backup file empty, nothing to restore.", "INFO")
                return
            data = json.loads(content)
        restored = 0
        skipped = 0
        with buffer_lock:
            for row in data:
                if not row:
                    continue
                txn_id = str(row[1]) if len(row) > 1 else ""
                if txn_id and txn_id in seen_txn_ids:
                    skipped += 1
                    continue
                log_buffer.append(row)
                restored += 1
        if restored or skipped:
            log(f"Restored {restored} logs from backup (skipped {skipped})", "INFO")
    except Exception as e:
        log(f"Backup restore failed: {e}", "ERROR")

# -------------------
# Sheet client
# -------------------

class SheetClient:
    """
    Writes rows of format:
    [Machine ID, Transaction ID, Amount, Date, Time, Status, Remarks]
    """

    def __init__(self, url: str, creds_path: str):
        self.url = url
        self.creds_path = creds_path
        self._client = None
        self._sheet = None
        self._lock = threading.Lock()
        self.txn_col_index: Optional[int] = None

    def _authorize(self):
        if self._client:
            return
        if not self.creds_path or not os.path.isfile(self.creds_path):
            raise FileNotFoundError(f"Credentials not found: {self.creds_path}")
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.creds_path, scope
        )
        self._client = gspread.authorize(creds)

    def sheet(self):
        with self._lock:
            if self._sheet:
                return self._sheet
            self._authorize()
            if not self.url:
                raise RuntimeError("GSHEET_URL env var missing")
            self._sheet = self._client.open_by_url(self.url).sheet1
            return self._sheet

    def bootstrap_txn_ids(self) -> set[str]:
        """Read sheet header and collect existing transaction IDs (robust to header layout)."""
        try:
            sh = self.sheet()
            rows = sh.get_all_values()
            if not rows:
                return set()
            header = [h.strip().lower() for h in rows[0]]
            txn_idx = None
            for i, h in enumerate(header):
                if ("transaction" in h and "id" in h) or "txn" in h:
                    txn_idx = i
                    break
            if txn_idx is None:
                for i, h in enumerate(header):
                    if (
                        "transaction" in h
                        or "txn" in h
                        or "reference" in h
                    ):
                        txn_idx = i
                        break
            if txn_idx is None:
                txn_idx = 1 if len(header) > 1 else 0
            self.txn_col_index = txn_idx
            vals = []
            for row in rows[1:]:
                if len(row) > txn_idx and row[txn_idx].strip():
                    vals.append(row[txn_idx].strip())
            return set(vals)
        except Exception as e:
            log(f"Sheets bootstrap failed: {e}", "WARN")
            return set()

    def append_rows(self, rows: list[list], retries: int = 3) -> bool:
        if not rows:
            return True
        attempt = 0
        while attempt < retries:
            attempt += 1
            try:
                sh = self.sheet()
                sh.append_rows(rows, value_input_option="RAW")
                return True
            except Exception as e:
                log(f"Sheet append attempt {attempt} failed: {e}", "WARN")
                time.sleep(1 + attempt)
        return False


SHEET_CLIENT = SheetClient(GSHEET_URL, GSHEET_CREDS_PATH)

# bootstrap existing txn ids
try:
    seen_txn_ids = SHEET_CLIENT.bootstrap_txn_ids()
    log(f"Bootstrapped {len(seen_txn_ids)} existing txn ids from sheet", "INFO")
except Exception as e:
    log(f"Bootstrap exception: {e}", "WARN")
    seen_txn_ids = set()

# -------------------
# MQTT persistent client (global)
# -------------------

class PersistentMQTT:
    def __init__(
        self,
        broker: str,
        port: int,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: Optional[str] = None,
    ):
        self.broker = broker
        self.port = port
        self.username = username
        self.password = password
        self.client_id = client_id or f"zenorc-{uuid.uuid4().hex[:8]}"
        try:
            self._client = mqtt.Client(
                client_id=self.client_id,
                protocol=mqtt.MQTTv311,
                callback_api_version=getattr(
                    mqtt, "CallbackAPIVersion", None
                ).VERSION2
                if hasattr(mqtt, "CallbackAPIVersion")
                else None,
            )
        except TypeError:
            # Older paho versions do not support callback_api_version
            self._client = mqtt.Client(client_id=self.client_id, protocol=mqtt.MQTTv311)

        if self.username:
            self._client.username_pw_set(self.username, self.password)
        self._connected = threading.Event()
        self._client.on_connect = self._on_connect
        try:
            # Best-effort TLS setup; fall back if CA unavailable
            try:
                self._client.tls_set(ca_certs=certifi.where())
            except Exception as e:
                log(f"MQTT tls_set warning: {e}", "WARN")
            self._client.connect(self.broker, self.port)
            self._client.loop_start()
            self._connected.wait(timeout=5)
            if not self._connected.is_set():
                log("MQTT persistent client did not connect within timeout", "WARN")
        except Exception as e:
            log(f"MQTT init failed: {e}", "ERROR")

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self._connected.set()
            log("Persistent MQTT connected (rc=0)", "INFO")
        else:
            log(f"MQTT connect rc={rc}", "WARN")

    def publish(self, topic: str, payload: str = "paid", qos: int = 1) -> bool:
        try:
            rc, mid = self._client.publish(topic, payload, qos=qos)
            ok = rc == 0 or rc == getattr(mqtt, "MQTT_ERR_SUCCESS", 0)
            log(f"↳ MQTT publish to {topic} rc={rc} mid={mid}")
            return ok
        except Exception as e:
            log(f"MQTT publish error: {e}", "ERROR")
            return False

    def stop(self):
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:
            pass


MQTT_CLIENT = PersistentMQTT(MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD)


def send_mqtt(
    remarks: str = "",
    base_topic: str = MQTT_TOPIC,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> bool:
    topic = base_topic
    if remarks:
        t = remarks.strip()
        topic = f"{base_topic}/{t}"
    for attempt in range(1, max_retries + 1):
        ok = MQTT_CLIENT.publish(topic, payload="paid", qos=1)
        if ok:
            return True
        log(f"MQTT attempt {attempt} to {topic} failed", "WARN")
        time.sleep(retry_delay)
    return False

# -------------------
# Buffer/flush helpers
# -------------------

def log_payment_buffered(
    machine_id: str,
    txn_id: str,
    amount: str = DEFAULT_AMOUNT,
    remarks: str = "",
):
    now = datetime.now(tz_mumbai())
    txn_status = global_status.get(txn_id, "Queued")
    row = [
        machine_id,
        txn_id,
        amount,
        now.strftime("%Y-%m-%d"),
        now.strftime("%H:%M:%S"),
        txn_status,
        remarks,
    ]
    with buffer_lock:
        log_buffer.append(row)
        if len(log_buffer) >= MAX_BATCH:
            threading.Thread(target=flush_logs, daemon=True).start()


def flush_logs():
    global log_buffer
    with buffer_lock:
        if not log_buffer:
            return
        rows_to_flush = list(log_buffer)
    ok = SHEET_CLIENT.append_rows(rows_to_flush)
    if ok:
        with buffer_lock:
            log(f"Logged {len(rows_to_flush)} rows to Google Sheets (batch)", "INFO")
            if os.path.exists(LOCAL_BACKUP_FILE):
                try:
                    with open(LOCAL_BACKUP_FILE, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                except Exception:
                    existing = []
                remaining = (
                    existing[len(rows_to_flush) :]
                    if len(existing) >= len(rows_to_flush)
                    else []
                )
                try:
                    with open(LOCAL_BACKUP_FILE, "w", encoding="utf-8") as f:
                        json.dump(remaining, f)
                except Exception as e:
                    log(f"Error trimming backup file: {e}", "WARN")
            log_buffer.clear()
    else:
        log("Sheets append failed; saving to local backup.", "ERROR")
        save_backup()


def periodic_flusher():
    while True:
        try:
            flush_logs()
        except Exception as e:
            log(f"periodic_flusher error: {e}", "ERROR")
        time.sleep(FLUSH_INTERVAL)

# -------------------
# Email parsing helpers
# -------------------

def _extract_txn_id(body: str) -> str:
    for pat in (
        r"Reference\s*(?:No\.?|number)?\s*[:\-]?\s*(\d{6,})",
        r"transaction reference number\s*(?:is)?\s*[:\-]?\s*(\d{6,})",
    ):
        m = re.search(pat, body, re.I)
        if m:
            return m.group(1)
    return f"TXN{int(time.time()*1000)}"


def _looks_like_credit(body_lc: str, amount: str) -> bool:
    if "credited" not in body_lc or "debited" in body_lc:
        return False
    # Match INR formats like ₹ 5, Rs 5, INR 5, with optional .00
    amt_re = re.compile(
        rf"(?:₹|rs\.?|inr)\s*[, ]*\s*{re.escape(str(amount))}(?:[.,]00)?\b",
        re.I,
    )
    wanted_phrase = (
        "successfully credited" in body_lc
        or "has been credited" in body_lc
        or "credited to your account" in body_lc
    )
    return wanted_phrase and bool(amt_re.search(body_lc))

# -------------------
# UserWorker: polls a single mailbox (per-client)
# -------------------

class UserWorker:
    def __init__(self, machine_id: str, cfg: dict):
        self.machine_id = machine_id
        self.cfg = cfg
        self.email_id = cfg.get("EMAIL_ID")
        self.email_pw = cfg.get("EMAIL_PASSWORD")
        self.amount = str(cfg.get("AMOUNT", DEFAULT_AMOUNT))
        self.mqtt_topic = cfg.get("MQTT_TOPIC", MQTT_TOPIC)
        self.seen_uids: set[bytes] = set()
        self.seen_txns: set[str] = set()
        self.imap_lock = threading.Lock()

    def _imap_login(self) -> imaplib.IMAP4_SSL:
        if not self.email_id or not self.email_pw:
            raise RuntimeError(f"{self.machine_id}: EMAIL_ID or EMAIL_PASSWORD missing")
        imap = imaplib.IMAP4_SSL("imap.gmail.com")
        imap.login(self.email_id, self.email_pw)
        return imap

    def poll_loop(self):
        while True:
            try:
                with self.imap_lock:
                    with self._imap_login() as mail:
                        mail.select("inbox")
                        _, data = mail.search(None, "(UNSEEN)")
                        uids = (data[0] or b"").split()
                        for uid in uids[-100:][::-1]:
                            if uid in self.seen_uids:
                                continue
                            _, msg_data = mail.fetch(uid, "(RFC822)")
                            if not msg_data or not msg_data[0]:
                                continue
                            msg = email.message_from_bytes(msg_data[0][1])
                            from_field = msg.get("From", "")
                            mclean = re.search(r"<(.+?)>", from_field)
                            sender = (
                                mclean.group(1).lower() if mclean else from_field.strip().lower()
                            )
                            if sender not in ALLOWED_SENDERS:
                                continue
                            body = ""
                            for part in msg.walk():
                                ctype = part.get_content_type()
                                if ctype == "text/plain":
                                    body = (
                                        part.get_payload(decode=True) or b""
                                    ).decode(errors="ignore")
                                    break
                            if not body:
                                for part in msg.walk():
                                    if part.get_content_type() == "text/html":
                                        body = (
                                            part.get_payload(decode=True) or b""
                                        ).decode(errors="ignore")
                                        break
                            if not body:
                                continue
                            body_lc = body.lower()
                            if not _looks_like_credit(body_lc, self.amount):
                                continue
                            txn_id = _extract_txn_id(body)
                            if txn_id in seen_txn_ids or txn_id in self.seen_txns:
                                self.seen_uids.add(uid)
                                try:
                                    mail.store(uid, "+FLAGS", "\\Seen")
                                except Exception:
                                    pass
                                continue
                            remarks = ""
                            try:
                                match = re.search(r"\b[Mm]\d+\b", body)
                                if match:
                                    remarks = match.group(0)
                            except Exception as e:
                                log(f"[{self.machine_id}] remark parse failed: {e}", "WARN")
                            self.seen_uids.add(uid)
                            self.seen_txns.add(txn_id)
                            try:
                                mail.store(uid, "+FLAGS", "\\Seen")
                            except Exception:
                                pass
                            item = {
                                "machine_id": self.machine_id,
                                "txn_id": txn_id,
                                "remarks": remarks,
                                "amount": self.amount,
                                "mqtt_topic": self.mqtt_topic,
                            }
                            global_queue.append(item)
                            global_status[txn_id] = "Queued"
                            log(f"[{self.machine_id}] Queued {txn_id} remarks={remarks}", "INFO")
            except Exception as e:
                log(f"[{self.machine_id}] Poll error: {e}", "ERROR")
            time.sleep(POLL_INTERVAL)

# -------------------
# Processor: central processor that pulls from global_queue
# -------------------

def processor_loop():
    global last_processed
    log("Processor started", "INFO")
    while True:
        if global_queue:
            now = time.time()
            if now - last_processed >= COOLDOWN_SECONDS:
                item = global_queue.popleft()
                txn_id = item["txn_id"]
                remarks = item.get("remarks", "")
                machine_id = item.get("machine_id", "UNKNOWN")
                amount = item.get("amount", DEFAULT_AMOUNT)
                mqtt_topic = item.get("mqtt_topic", MQTT_TOPIC)
                global_status[txn_id] = "Processing"
                log(f"⚙ Processing [{machine_id}] {txn_id}", "INFO")
                ok = send_mqtt(remarks=remarks, base_topic=mqtt_topic)
                global_status[txn_id] = "Completed" if ok else "Failed"
                log(("✔" if ok else "❌") + f" Completed [{machine_id}] {txn_id}", "INFO" if ok else "ERROR")
                threading.Thread(
                    target=log_payment_buffered,
                    args=(machine_id, txn_id, amount, remarks),
                    daemon=True,
                ).start()
                seen_txn_ids.add(txn_id)
                last_processed = time.time()
            else:
                time.sleep(0.2)
        else:
            time.sleep(0.5)

# -------------------
# Load all client configs from CONFIGS_DIR/*.env
# -------------------

def load_clients_from_configs(dir_path: Path) -> dict[str, dict]:
    clients: dict[str, dict] = {}
    if not dir_path.exists() or not dir_path.is_dir():
        log(f"Configs directory {dir_path} missing", "WARN")
        return clients
    for p in sorted(dir_path.iterdir()):
        if p.is_file() and p.suffix == ".env":
            machine_id = p.stem
            cfg = dotenv_values(p)
            if "EMAIL_ID" not in cfg or "EMAIL_PASSWORD" not in cfg:
                log(f"{p} missing EMAIL_ID/EMAIL_PASSWORD — skipping", "WARN")
                continue
            machine_id = cfg.get("MACHINE_ID", machine_id)
            clients[machine_id] = cfg
    return clients

# -------------------
# Flask endpoints
# -------------------

app = Flask(__name__)


@app.route("/health")
def health():
    return jsonify(ok=True)


@app.route("/")
def root():
    return (
        f"<h3>Zenorc Payment Processor</h3>"
        f"<p>Queue length: {len(global_queue)}</p>"
        f"<p>Buffered rows: {len(log_buffer)}</p>"
    )


@app.route("/flush", methods=["POST"])
def manual_flush():
    try:
        flush_logs()
        return "Flush triggered", 200
    except Exception as e:
        return f"Flush error: {e}", 500


@app.route("/paypal-webhook", methods=["POST"])
def paypal_webhook():
    try:
        data = request.get_json(force=True)
        event = data.get("event_type")
        resource = data.get("resource", {})

        subscription_id = resource.get("billing_agreement_id")
        payer_info = resource.get("payer", {})
        payer_email = payer_info.get("email_address")

        sh = SHEET_CLIENT.sheet()
        rows = sh.get_all_values()
        if not rows:
            log("Webhook: sheet empty", "ERROR")
            return "Sheet empty", 500

        headers = [h.strip().lower() for h in rows[0]]

        def safe_idx(name: str) -> int:
            return headers.index(name) if name in headers else -1

        mid_idx = safe_idx("machine id")
        email_idx = safe_idx("email")
        subid_idx = safe_idx("subscription id")
        status_idx = safe_idx("status")
        pay_idx = safe_idx("payment received")
        exp_idx = safe_idx("expiry")

        if mid_idx == -1:
            log("Webhook: 'machine id' column not found", "ERROR")
            return "Column machine id missing", 500

        machine_id = None
        row_index = -1
        for i, row in enumerate(rows[1:], start=2):
            matches = False
            if (
                email_idx != -1
                and len(row) > email_idx
                and payer_email
                and row[email_idx] == payer_email
            ):
                matches = True
            if (
                not matches
                and subid_idx != -1
                and len(row) > subid_idx
                and subscription_id
                and row[subid_idx] == subscription_id
            ):
                matches = True
            if matches:
                machine_id = row[mid_idx] if len(row) > mid_idx else None
                row_index = i
                break

        if not machine_id:
            log("Webhook: Unable to match subscription/email to machine", "ERROR")
            return "Machine ID not found", 404

        if event == "PAYMENT.SALE.COMPLETED":
            now = datetime.now(tz_mumbai())
            new_payment = now.strftime("%Y-%m-%d")
            new_expiry = (
                now.replace(hour=0, minute=0, second=0, microsecond=0)
                + timedelta(days=30)
            ).strftime("%Y-%m-%d")
            if status_idx != -1 and pay_idx != -1 and exp_idx != -1:
                row_values = rows[row_index - 1]
                max_needed = max(status_idx, pay_idx, exp_idx) + 1
                while len(row_values) < max_needed:
                    row_values.append("")
                row_values[status_idx] = "active"
                row_values[pay_idx] = new_payment
                row_values[exp_idx] = new_expiry
                start_col = "A"
                end_col = col_idx_to_letter(max_needed - 1)
                sh.update(f"{start_col}{row_index}:{end_col}{row_index}", [row_values[:max_needed]])
            else:
                if status_idx != -1:
                    sh.update_cell(row_index, status_idx + 1, "active")
                if pay_idx != -1:
                    sh.update_cell(row_index, pay_idx + 1, new_payment)
                if exp_idx != -1:
                    sh.update_cell(row_index, exp_idx + 1, new_expiry)
            user_flags[machine_id] = True
            log(f"[{machine_id}] Webhook: PAYMENT.SALE.COMPLETED → ACTIVE", "INFO")
        elif event in [
            "PAYMENT.SALE.DENIED",
            "BILLING.SUBSCRIPTION.CANCELLED",
            "BILLING.SUBSCRIPTION.SUSPENDED",
        ]:
            if status_idx != -1:
                sh.update_cell(row_index, status_idx + 1, "inactive")
            user_flags[machine_id] = False
            log(f"[{machine_id}] Webhook: {event} → INACTIVE", "WARNING")

        return "Webhook processed", 200

    except Exception as e:
        log(f"Webhook error: {e}", "ERROR")
        return "Internal error", 500

# -------------------
# Start-up helpers
# -------------------

def send_daily_summary():
    received = [tid for tid, s in global_status.items() if s == "Completed"]
    failed = [tid for tid, s in global_status.items() if s == "Failed"]
    pending = [tid for tid, s in global_status.items() if s in {"Queued", "Processing"}]
    if not (received or failed or pending):
        log("No new transactions for summary email.")
        return

    def build_section(title: str, items: list[str]) -> str:
        return (
            f"<h4>{title} ({len(items)}):</h4><ul>"
            + "".join(f"<li>{i}</li>" for i in items)
            + "</ul>"
        )

    html_body = (
        "<h3>Zenorc Daily Payment Summary</h3>"
        + build_section("Received", received)
        + build_section("Pending", pending)
        + build_section("Failed", failed)
    )
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Zenorc Daily Payment Summary"
    msg["From"] = os.getenv("SMTP_EMAIL", os.getenv("EMAIL_ID", ""))
    msg["To"] = os.getenv("SMTP_EMAIL", os.getenv("EMAIL_ID", ""))
    msg.attach(MIMEText(html_body, "html"))
    try:
        with smtplib.SMTP(
            os.getenv("SMTP_SERVER", "smtp.gmail.com"),
            int(os.getenv("SMTP_PORT", 587)),
        ) as server:
            server.starttls()
            server.login(os.getenv("SMTP_EMAIL"), os.getenv("SMTP_PASSWORD"))
            server.sendmail(msg["From"], msg["To"].split(","), msg.as_string())
            log("Daily summary email sent.")
    except Exception as e:
        log(f"Email send error: {e}", "ERROR")


def scheduler_loop():
    if schedule is None:
        log("schedule library not installed; skipping daily summary scheduler", "WARN")
        return
    schedule.every().day.at("23:59").do(send_daily_summary)
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            log(f"scheduler_loop error: {e}", "ERROR")
        time.sleep(60)


def main():
    restore_backup()
    clients = load_clients_from_configs(CONFIGS_DIR)
    if not clients:
        log(
            "No valid client configs loaded. Put MCH-xxx.env in configs/ and restart.",
            "ERROR",
        )
    for mid, cfg in clients.items():
        worker = UserWorker(mid, cfg)
        t = threading.Thread(target=worker.poll_loop, daemon=True)
        t.start()
    threading.Thread(target=processor_loop, daemon=True).start()
    threading.Thread(target=periodic_flusher, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    log("All workers started", "INFO")

    port = int(os.getenv("PORT", "5000"))
    if serve is not None:
        serve(app, host="0.0.0.0", port=port)
    else:
        app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    main()
