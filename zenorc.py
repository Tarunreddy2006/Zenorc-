import imaplib
import email
import time
import gspread
import paho.mqtt.client as mqtt
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from gtts import gTTS
import pygame
import os
import re
from collections import deque
import threading
from dotenv import load_dotenv

# ------------------------------
# 🔧 LOAD ENVIRONMENT VARIABLES
# ------------------------------
load_dotenv()

EMAIL_ID = os.getenv("EMAIL_ID")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # Gmail App Password

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = os.getenv("MQTT_PORT")
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD ")
MQTT_TOPIC = os.getenv("MQTT_TOPIC")

GOOGLE_CREDS = os.getenv("GOOGLE_CREDS")
SHEET_URL = os.getenv("SHEET_URL")

SEARCH_STRINGS = ('₹5', 'Rs 5')

seen_uids = set()
payment_queue = deque()
payment_status = {}

COOLDOWN_SECONDS = 40
last_processed_time = 0

# ------------------------------
# 🔊 TEXT-TO-SPEECH
# ------------------------------
def play_tts(message):
    try:
        tts = gTTS(message, lang='en')
        fname = 'tts.mp3'
        tts.save(fname)
        pygame.mixer.init()
        pygame.mixer.music.load(fname)
        pygame.mixer.music.play()
        while pygame.mixer.music.get_busy():
            time.sleep(0.1)
        pygame.mixer.quit()
        os.remove(fname)
    except Exception as err:
        print('TTS Error:', err)

# ------------------------------
# 📧 CHECK GMAIL FOR ₹5 PAYMENTS
# ------------------------------
def check_for_payment():
    try:
        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login(EMAIL_ID, EMAIL_PASSWORD)
        mail.select('inbox')

        result, data = mail.search(None, '(UNSEEN)')
        uid_list = data[0].split()[-20:][::-1]

        for uid in uid_list:
            if uid in seen_uids:
                continue

            result, msg_data = mail.fetch(uid, '(RFC822)')
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            raw_subject = msg['Subject'] or ''
            subject = str(email.header.make_header(email.header.decode_header(raw_subject)))

            body_text = ''
            for part in msg.walk():
                if part.get_content_type() == 'text/plain':
                    body_text = part.get_payload(decode=True).decode(errors='ignore')
                    break

            if any(s in subject for s in SEARCH_STRINGS) or any(s in body_text for s in SEARCH_STRINGS):
                seen_uids.add(uid)
                ref_match = re.search(r'Reference\s*No\.?[:\s]*(\d+)', body_text)
                txn_id = ref_match.group(1) if ref_match else f'TXN{int(time.time())}'
                print(f'Payment email found (UID {uid.decode()}): {txn_id}')
                return txn_id
        return None
    except Exception as err:
        print('Gmail Error:', err)
        return None

# ------------------------------
# 📄 LOG PAYMENT TO SHEET
# ------------------------------
def log_payment(txn_id: str, amount: str):
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS, scope)
        sheet = gspread.authorize(creds).open_by_url(SHEET_URL).sheet1
        now = datetime.now()
        sheet.append_row([txn_id, amount, now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S')])
        print(f'Logged → Sheet: {txn_id} ₹{amount}')
    except Exception as err:
        print('Sheets Error:', err)

# ------------------------------
# 📡 MQTT PUBLISH WITH RETRIES
# ------------------------------
def send_mqtt(max_retries=3, retry_delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            client = mqtt.Client()
            client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
            client.tls_set()
            client.connect(MQTT_BROKER, MQTT_PORT)
            client.publish(MQTT_TOPIC, "paid")
            client.disconnect()
            print('MQTT sent successfully')
            return True
        except Exception as err:
            print(f'MQTT Error (Attempt {attempt}): {err}')
            if attempt < max_retries:
                time.sleep(retry_delay)
    print('MQTT failed after all retries')
    return False

# ------------------------------
# 🔁 BACKGROUND THREAD: PROCESS PAYMENTS
# ------------------------------
def process_payments():
    global last_processed_time
    while True:
        if payment_queue:
            current_time = time.time()
            if current_time - last_processed_time >= COOLDOWN_SECONDS:
                txn_id = payment_queue.popleft()
                payment_status[txn_id] = 'Processing'
                print(f'⚙ Processing: {txn_id} [Status: {payment_status[txn_id]}]')

                if send_mqtt():
                    play_tts('Payment of five rupees received')
                    payment_status[txn_id] = 'Completed'
                    print(f'Completed: {txn_id}')
                else:
                    play_tts('Payment failed to reach the system. Please contact support.')
                    payment_status[txn_id] = 'Failed'

                last_processed_time = time.time()
            else:
                print('Please wait, transaction is processing...')
                play_tts('Please wait. Transaction is processing.')
        time.sleep(1)

# ------------------------------
# 🚀 MAIN LOOP
# ------------------------------
if __name__ == "__main__":
    threading.Thread(target=process_payments, daemon=True).start()

    while True:
        print('\nScanning inbox for payments...')
        txn_id = check_for_payment()

        if txn_id and txn_id not in payment_status:
            log_payment(txn_id, '5')
            payment_queue.append(txn_id)
            payment_status[txn_id] = 'Queued'
            print(f'Added to queue: {txn_id} [Status: {payment_status[txn_id]}]')

        time.sleep(1)