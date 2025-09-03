import os, re, json, time, io
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from typing import List, Tuple
from pathlib import Path
from html import escape

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import pdfkit  # conversione HTML -> PDF offline

# ============ Config ============
TZ = ZoneInfo("Europe/Rome")
WINDOW_START = dtime(9, 0)
WINDOW_END   = dtime(18, 0)

YEAR = int(os.getenv("YEAR", "2025"))
BASE = f"https://www.regione.piemonte.it/governo/bollettino/abbonati/{YEAR}"
CORRENTE_SISTE = f"{BASE}/corrente/siste/index.htm"

PAGES = ["siste", "suppo1", "suppo2", "suppo3"]  # suppo3 è “eventuale”
HEADERS = {"User-Agent": "Mozilla/5.0 (BUR monitor)"}

# --- Drive (DISATTIVO di default) ---
ENABLE_DRIVE = os.getenv("ENABLE_DRIVE", "0") == "1"
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "")
SERVICE_ACCOUNT_JSON = os.getenv("DRIVE_SERVICE_ACCOUNT_JSON", "")

# Webhook (facoltativo, non usato qui)
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")

# SMTP diretto via App Password Gmail
SEND_EMAIL = os.getenv("SEND_EMAIL", "1") == "1"
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT") or "465")
MAIL_TO   = [x.strip() for x in os.getenv("MAIL_TO", "").split(",") if x.strip()]

STATE_FILE = "state.json"  # lo salvi nel repo con lo step GitHub Actions

# ============ HTTP session (anti-504) ============
def http_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1.5, status_forcelist=(500, 502, 503, 504))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

def now_in_window() -> bool:
    t = datetime.now(TZ).time()
    return WINDOW_START <= t <= WINDOW_END

# ============ Lettura numero corrente ============
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

def url_for(issue: int, page: str) -> str:
    return f"{BASE}/{issue}/{page}/index.htm"

def url_exists(url: str, s: requests.Session) -> bool:
    try:
        r = s.get(url, timeout=20, allow_redirects=True)
        print(f"[DBG] URL check {r.status_code}: {url}")
        return 200 <= r.status_code < 400
    except Exception as e:
        print(f"[DBG] URL check exception for {url}: {e}")
        return False

# ============ HTML -> PDF (wkhtmltopdf/pdfkit) ============
def render_pdf_offline(url: str, out_path: str, session: requests.Session):
    """
    Scarica l'HTML con requests e lo converte in PDF offline con wkhtmltopdf (via pdfkit).
    Evita i blocchi del WAF perché non usa un browser headless.
    """
    r = session.get(url, timeout=30)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    html = r.text

    title = f"Istantanea pagina: {url}"
    safe_url = escape(url)
    wrapped = f"""<!doctype html><html lang="it"><head>
      <meta charset="utf-8">
      <title>{escape(title)}</title>
      <style>
        body {{ font-family: Arial, Helvetica, sans-serif; font-size: 12px; line-height: 1.35; }}
        h1,h2,h3 {{ margin-top: 12px; }}
        a {{ text-decoration: none; word-break: break-word; }}
        .src {{ font-size: 10px; color: #555; margin-bottom: 8px; }}
        table {{ border-collapse: collapse; width: 100%; }}
        td, th {{ border: 1px solid #ddd; padding: 4px; vertical-align: top; }}
      </style>
    </head><body>
      <div class="src">Fonte: {safe_url}</div>
      {html}
    </body></html>"""

    tmp_html = Path(out_path).with_suffix(".tmp.html")
    tmp_html.write_text(wrapped, encoding="utf-8")

    options = {
        "--quiet": None,
        "--enable-local-file-access": None,
        "--print-media-type": None,
        "--margin-top": "10mm",
        "--margin-bottom": "10mm",
        "--margin-left": "8mm",
        "--margin-right": "8mm",
        "--encoding": "UTF-8",
    }
    try:
        pdfkit.from_file(str(tmp_html), out_path, options=options)
    finally:
        try:
            tmp_html.unlink()
        except FileNotFoundError:
            pass

    try:
        sz = os.path.getsize(out_path)
        print(f"[DBG] Creato PDF: {out_path} ({sz} bytes)")
    except Exception as e:
        print(f"[DBG] Stat PDF fallita per {out_path}: {e}")

# ============ Drive (opzionale; DISATTIVO di default) ============
def drive_client():
    """
    Client Drive via Service Account. ATTENZIONE:
    - Le SA NON hanno quota su 'Il mio Drive' (403).
    - Qui manteniamo il supporto solo se abiliti esplicitamente ENABLE_DRIVE=1
      e carichi su un Drive Condiviso o usi un bridge esterno.
    """
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_info(
        json.loads(SERVICE_ACCOUNT_JSON),
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_check_folder(drive) -> bool:
    from googleapiclient.errors import HttpError
    try:
        meta = drive.files().get(
            fileId=DRIVE_FOLDER_ID,
            fields="id,name,driveId,parents,mimeType",
            supportsAllDrives=True,
        ).execute()
        print(f"[DBG] Drive folder OK: name={meta.get('name')} id={meta.get('id')} driveId={meta.get('driveId')}")
        return True
    except HttpError as e:
        print("[WARN] Cartella Drive non accessibile (upload disabilitato). Dettagli:", e)
        return False
    except Exception as e:
        print("[WARN] Errore generico accesso Drive:", e)
        return False

def upload_to_drive(drive, file_path: str, name: str) -> dict | None:
    """
    Prova a caricare su Drive; se fallisce (es. 403 quota SA), non blocca il job.
    """
    from googleapiclient.http import MediaIoBaseUpload
    try:
        media = MediaIoBaseUpload(open(file_path, "rb"), mimetype="application/pdf", resumable=True)
        meta = {"name": name, "parents": [DRIVE_FOLDER_ID]}
        f = drive.files().create(
            body=meta, media_body=media,
            fields="id,name,webViewLink,webContentLink,parents",
            supportsAllDrives=True
        ).execute()
        print(f"[DBG] Caricato su Drive: {f.get('name')} (id={f.get('id')})")
        return f
    except Exception as ex:
        msg = (getattr(ex, "content", b"") or b"")
        try:
            msg = msg.decode("utf-8", "ignore")
        except Exception:
            msg = str(ex)
        if "Service Accounts do not have storage quota" in msg:
            print("[WARN] SA senza quota: upload su Drive saltato (non blocco il job).")
            return None
        print(f"[WARN] Upload su Drive fallito per {name}: {ex}")
        return None

# ============ Notifiche / Email ============
def send_smtp(files: List[str], subject: str, body: str):
    if not (SEND_EMAIL and SMTP_USER and SMTP_PASS and MAIL_TO):
        print("[DBG] SMTP disabilitato o incompleto — salto invio.")
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
            msg.add_attachment(
                f.read(),
                maintype=maintype, subtype=subtype,
                filename=os.path.basename(path)
            )

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx) as smtp:
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)

# ============ State ============
def load_state():
    try:
        return json.load(open(STATE_FILE, "r", encoding="utf-8"))
    except FileNotFoundError:
        return {"last_number": None, "year": YEAR}

def save_state(state: dict):
    json.dump(state, open(STATE_FILE, "w", encoding="utf-8"))

# ============ MAIN ============
def main():
    force_run = os.getenv("FORCE_RUN") == "1"
    force_send = os.getenv("FORCE_SEND") == "1"

    if not now_in_window() and not force_run:
        print("Fuori fascia 09–18 Europe/Rome → esco.")
        return

    s = http_session()
    st = load_state()

    try:
        cur_num, cur_date = get_current_bur_number(s)
        print(f"Numero corrente BUR sul sito: n. {cur_num} (del {cur_date})")
    except Exception as e:
        print("Errore nel leggere il numero corrente:", e)
        cur_num = (st.get("last_number") or 0)
        cur_date = ""

    new_issue = (st.get("last_number") != cur_num and cur_num is not None)

    if not new_issue and not force_send:
        print(f"Nessun nuovo BUR (ultimo noto: {st.get('last_number')}).")
        return

    print("Nuovo BUR rilevato o invio forzato! Procedo con PDF.")
    files_local: List[str] = []
    uploaded: List[dict] = []

    for page in PAGES:
        u = url_for(cur_num, page)
        if url_exists(u, s):
            out_name = f"BUR_{YEAR}_{cur_num}_{page}.pdf"
            out_path = os.path.join(".", out_name)
            try:
                render_pdf_offline(u, out_path, s)
                files_local.append(out_path)

                # Upload opzionale (di default disattivato)
                if ENABLE_DRIVE and DRIVE_FOLDER_ID and SERVICE_ACCOUNT_JSON:
                    try:
                        drive = getattr(main, "_drive", None)
                        if drive is None:
                            drive = drive_client()
                            if drive_check_folder(drive):
                                setattr(main, "_drive", drive)
                            else:
                                setattr(main, "_drive", False)
                                drive = False
                        if drive:
                            meta = upload_to_drive(drive, out_path, out_name)
                            if meta:
                                uploaded.append(meta)
                    except Exception as e:
                        print("[WARN] Upload Drive non riuscito:", e)

                time.sleep(0.3)
            except Exception as e:
                print(f"Errore su {page}:", e)
        else:
            print(f"Pagina assente (ok): {u}")

    # Email (se abilitata)
    print(f"[DBG] files_local={len(files_local)}  MAIL_TO={len(MAIL_TO)}  SMTP_USER_set={bool(SMTP_USER)}  SMTP_PASS_set={bool(SMTP_PASS)}  SMTP_PORT={SMTP_PORT}  SEND_EMAIL={SEND_EMAIL}")
    try:
        if files_local:
            subj = f"BUR Piemonte n. {cur_num} — pubblicazione"
            body = f"In allegato i PDF (siste/supplementi) del BUR n. {cur_num} ({cur_date})."
            send_smtp(files_local, subj, body)
            print("[DBG] Email inviata (se abilitata).")
        else:
            print("[DBG] Nessun file locale da allegare.")
    except Exception as e:
        print("[ERR] SMTP:", repr(e))

    # Aggiorna stato (evita doppie email ai prossimi run)
    if new_issue:
        st["last_number"] = cur_num
        st["year"] = YEAR
        save_state(st)
    print("Completato.")

if __name__ == "__main__":
    main()
