import os, re, json, time, sys, io
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from typing import List, Tuple
from pathlib import Path
from html import escape

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import pdfkit  # <-- nuovo: conversione HTML -> PDF offline

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

# SMTP diretto via App Password Gmail (opzionale)
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT") or "465")
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
        return r.status_code == 200
    except Exception:
        return False

# ---------- Rendering PDF offline (wkhtmltopdf/pdfkit) ----------
def render_pdf_offline(url: str, out_path: str, session: requests.Session):
    """
    Scarica l'HTML con requests (aggira i blocchi del WAF verso browser headless)
    e lo converte in PDF offline con wkhtmltopdf (via pdfkit).
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

    tmp_html = Path(ou_
