import os, re, json, time, sys, io
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from typing import List, Tuple

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ---------- Config ----------
TZ = ZoneInfo("Europe/Rome")
WINDOW_START = dtime(9, 0)
WINDOW_END   = dtime(18, 0)

YEAR = int(os.getenv("YEAR", "2025"))
BASE = f"https://www.regione.piemonte.it/governo/bollettino/abbonati/{YEAR}"
CORRENTE_SISTE = f"{BASE}/corrente/siste/index.htm"

PAGES = ["siste", "suppo1", "suppo2", "suppo3"]  # suppo3 è “eventuale”
HEADERS = {"User-Agent": "Mozilla/5.0 (BUR monitor)"}

DRIVE_FOLDER_ID = os.environ["DRIVE_FOLDER_ID"]
SERVICE_ACCOUNT_JSON = os.environ["DRIVE_SERVICE_ACCOUNT_JSON"]

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")

# Opzione B (SMTP diretto via App Password Gmail)
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
MAIL_TO   = [x.strip() for x in os.getenv("MAIL_TO", "").split(",") if x.strip()]

STATE_FILE = "state.json"

# ---------- HTTP session (anti-504) ----------
def http_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1.5, status_forcelist=(500,502,503,504))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

def now_in_window() -> bool:
    t = datetime.now(TZ).time()
    return WINDOW_START <= t <= WINDOW_END

# ---------- Lettura numero corrente ----------
def parse_bur_number(html: str) -> Tuple[int, str]:
    # es. "Bollettino Ufficiale n. 35 del 28 agosto 2025"
    m = re.search(r"Bollettino\s+Ufficiale\s+n\.\s*(\d+)\s+del\s+(.+?20\d{2})", html, re.IGNORECASE)
    if not m:
        # fallback: "Bollettino n° 35 del ..."
        m = re.search(r"Bollettino\s*n[°o]\s*(\d+)\s*del\s+(.+?20\d{2})", html, re.IGNORECASE)
    if m:
        return int(m.group(1)), m.group(2).strip()
    raise RuntimeError("Impossibile estrarre il numero BUR.")

def get_current_bur_number(s: requests.Session) -> Tuple[int, str]:
    r = s.get(CORRENTE_SISTE, timeout=25)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return parse_bur_number(r.text)

def url_for(issue:int, page:str) -> str:
    return f"{BASE}/{issue}/{page}/index.htm"

def url_exists(url:str, s:requests.Session) -> bool:
    try:
        r = s.get(url, timeout=20)
        if r.status_code == 200:
            return True
        return False
    except Exception:
        return False

# ---------- PDF rendering con Playwright ----------
async def render_pdf_async(url:str, out_path:str):
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(args=["--no-sandbox"])
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(url, wait_until="networkidle", timeout=60000)
        # piccolo delay per elementi dinamici
        await page.wait_for_timeout(500)
        await page.pdf(path=out_path, print_background=True, margin={"top":"10mm","bottom":"10mm","left":"8mm","right":"8mm"})
        await browser.close()

def render_pdf(url:str, out_path:str):
    import asyncio
    asyncio.run(render_pdf_async(url, out_path))

# ---------- Upload su Drive ----------
def drive_client():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(SERVICE_ACCOUNT_JSON),
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def upload_to_drive(drive, file_path:str, name:str) -> dict:
    media = MediaIoBaseUpload(open(file_path,"rb"), mimetype="application/pdf", resumable=True)
    meta = {"name": name, "parents":[DRIVE_FOLDER_ID]}
    f = drive.files().create(body=meta, media_body=media, fields="id,name,webViewLink,webContentLink").execute()
    return f

def share_with(drive, file_id:str, emails:List[str]):
    # Condivide in sola lettura con una o più mail (opzionale)
    for e in emails:
        drive.permissions().create(fileId=file_id, body={"type":"user","role":"reader","emailAddress":e}, sendNotificationEmail=False).execute()

# ---------- Notifiche ----------
def post_webhook(payload:dict):
    if not WEBHOOK_URL: return
    try:
        requests.post(WEBHOOK_URL, json=payload, timeout=15)
    except Exception as e:
        print("Webhook error:", e)

def send_smtp(files:List[str], subject:str, body:str):
    if not (SMTP_USER and SMTP_PASS and MAIL_TO):
        return
    import smtplib, ssl, mimetypes
    from email.message import EmailMessage

    msg = EmailMessage()
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(MAIL_TO)
    msg["Subject"] = subject
    msg.set_content(body)

    for path in files:
        ctype, _ = mimetypes.guess_type(path)
        maintype, subtype = (ctype or "application/pdf").split("/", 1)
        with open(path, "rb") as f:
            msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=os.path.basename(path))

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx) as smtp:
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)

# ---------- State ----------
def load_state():
    try:
        return json.load(open(STATE_FILE,"r",encoding="utf-8"))
    except FileNotFoundError:
        return {"last_number": None, "year": YEAR}

def save_state(state:dict):
    json.dump(state, open(STATE_FILE,"w",encoding="utf-8"))

# ---------- MAIN ----------
def main():
    if not now_in_window():
        print("Fuori fascia 09–18 Europe/Rome → esco.")
        return

    s = http_session()
    st = load_state()
    drive = drive_client()

    try:
        cur_num, cur_date = get_current_bur_number(s)
        print(f"Numero corrente BUR sul sito: n. {cur_num} (del {cur_date})")
    except Exception as e:
        print("Errore nel leggere il numero corrente:", e)
        # fallback: prova il “prossimo” rispetto allo stato
        cur_num = (st.get("last_number") or 0)
        cur_date = ""

    new_issue = (st.get("last_number") != cur_num and cur_num is not None)

    # Se nuovo, prepariamo le 3/4 pagine di quel numero; se non nuovo, usciamo
    if not new_issue:
        print(f"Nessun nuovo BUR (ultimo noto: {st.get('last_number')}).")
        return

    print("Nuovo BUR rilevato! Procedo con PDF e upload.")
    files_local = []
    uploaded = []

    for page in PAGES:
        u = url_for(cur_num, page)
        if url_exists(u, s):
            out_name = f"BUR_{YEAR}_{cur_num}_{page}.pdf"
            out_path = os.path.join(".", out_name)
            try:
                render_pdf(u, out_path)
                files_local.append(out_path)
                meta = upload_to_drive(drive, out_path, out_name)
                # (facoltativo) assicurati accesso a te/colleghi per Make/Drive
                share_with(drive, meta["id"], [SMTP_USER] if SMTP_USER else [])
                uploaded.append(meta)
                # cortesia per il server
                time.sleep(0.5)
            except Exception as e:
                print(f"Errore su {page}:", e)
        else:
            print(f"Pagina assente (ok): {u}")

    # Notifica webhook (per Make.com → invio email Gmail con allegati presi da Drive)
    payload = {
        "bur_number": cur_num,
        "date": cur_date,
        "year": YEAR,
        "files": uploaded,   # [{id,name,webViewLink,webContentLink}]
    }
    post_webhook(payload)

    # In alternativa invio diretto via SMTP (Gmail App Password)
    if files_local and SMTP_USER and SMTP_PASS and MAIL_TO:
        subj = f"BUR Piemonte n. {cur_num} — pubblicazione"
        body = f"In allegato i PDF delle pagine (siste/supplementi) del BUR n. {cur_num} ({cur_date})."
        send_smtp(files_local, subj, body)

    # Aggiorna stato
    st["last_number"] = cur_num
    st["year"] = YEAR
    save_state(st)
    print("Completato.")

if __name__ == "__main__":
    main()
