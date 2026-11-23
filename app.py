import asyncio
import csv
import io
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="Monitor Disponibilità Lucania Rent")
scheduler = AsyncIOScheduler()

INTERVAL_MINUTES = 30

STATE: Dict[str, Any] = {
    "sheet_url": "",
    "site_url": "",
    "running": False,
    "scanning": False,
    "last_run": None,
    "next_run": None,
    "status_message": "In attesa di configurazione",
    "sheet_count": 0,
    "site_count": 0,
    "alerts": [],
    "items": [],
}


class ConfigPayload(BaseModel):
    sheet_url: str
    site_url: str


def clean_pasted_url(raw: str) -> str:
    s = (raw or "").strip()
    md_match = re.search(r"\]\((https?://[^\s\)]+)\)", s)
    if md_match:
        s = md_match.group(1)
    s = s.strip("[]()<>\"' ")
    if " " in s:
        s = s.split(" ")[0]
    return s


def normalize_code(val: Optional[str]) -> str:
    if not val:
        return ""
    s = str(val).strip().upper()
    if s.endswith(",00") or s.endswith(".00"):
        s = s[:-3]
    return re.sub(r"[^A-Z0-9]", "", s)


def to_export_csv_url(sheet_url: str) -> str:
    url = clean_pasted_url(sheet_url)
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not match:
        return url
    sheet_id = match.group(1)
    gid_match = re.search(r"[?&]gid=([0-9]+)", url)
    if gid_match and gid_match.group(1) != "0":
        return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid_match.group(1)}"
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"


def parse_stock(val: Optional[str]) -> int:
    if val is None:
        return 0
    cleaned = str(val).strip().replace(",", ".")
    if not cleaned:
        return 0
    try:
        return int(float(cleaned))
    except ValueError:
        return 0


def is_valid_http_url(url: str) -> bool:
    if not url:
        return False
    u = url.strip()
    if "[" in u or "]" in u or " " in u:
        return False
    try:
        parsed = urlparse(u)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc) and "." in parsed.netloc
    except Exception:
        return False


async def fetch_sheet_products(client: httpx.AsyncClient, sheet_url: str) -> List[Dict[str, Any]]:
    csv_url = to_export_csv_url(sheet_url)
    resp = await client.get(csv_url, follow_redirects=True, timeout=25.0)
    resp.raise_for_status()

    text = resp.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    products: List[Dict[str, Any]] = []
    for row in reader:
        norm_row = {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}
        codice = norm_row.get("codice", "")
        cod_forn = norm_row.get("codartforn", "")
        ean = norm_row.get("ean_13", "") or norm_row.get("ean", "")
        descrizione = norm_row.get("descrizione", "") or norm_row.get("nome", "") or codice
        stock_raw = norm_row.get("stock", "0")
        stock = parse_stock(stock_raw)

        keys = [k for k in {
            normalize_code(codice),
            normalize_code(cod_forn),
            normalize_code(ean),
        } if k]

        if not (codice or cod_forn or ean or descrizione):
            continue

        products.append({
            "codice": codice,
            "cod_forn": cod_forn,
            "ean": ean,
            "descrizione": descrizione,
            "stock": stock,
            "available": stock > 0,
            "keys": keys,
        })
    return products


def extract_urls_from_xml(xml_text: str) -> List[str]:
    soup = BeautifulSoup(xml_text, "xml")
    return [
        loc.get_text(strip=True)
        for loc in soup.find_all("loc")
        if is_valid_http_url(loc.get_text(strip=True))
    ]


def is_product_url(url: str, origin: str) -> bool:
    if not is_valid_http_url(url):
        return False
    if url.rstrip("/") == origin.rstrip("/"):
        return False
    p = urlparse(url)
    path = p.path.lower()
    low = url.lower()

    if any(ext in path for ext in [
        ".jpg", ".jpeg", ".png", ".webp", ".gif", ".svg", ".ico",
        ".pdf", ".css", ".js", ".xml", ".zip", ".mp4", ".mp3"
    ]):
        return False

    if any(skip in path for skip in [
        "/wp-login", "/wp-admin", "/wp-content", "/wp-includes",
        "/cart", "/carrello", "/checkout", "/account", "/my-account",
        "/login", "/register", "/privacy", "/cookie", "/termini", "/contatti",
        "/chi-siamo", "/blog", "/news", "/tag/", "/author/"
    ]):
        return False

    if any(seg in low for seg in [
        "/prodotto/", "/product/", "/item/", "/p/", "/noleggio/",
        "/attrezzatur", "/macchin", "/scheda", "sku=", "codice=", "id_prodotto="
    ]):
        return True

    parts = [seg for seg in p.path.strip("/").split("/") if seg]
    return len(parts) >= 2


async def discover_all_site_product_urls(client: httpx.AsyncClient, site_url: str, max_crawl_pages: int = 250) -> List[str]:
    base = clean_pasted_url(site_url)
    if not base.startswith(("http://", "https://")):
        base = "https://" + base
    if not is_valid_http_url(base):
        return []

    parsed_base = urlparse(base)
    origin = f"{parsed_base.scheme}://{parsed_base.netloc}"

    candidate_urls: List[str] = []
    sitemap_queue: List[str] = []
    seen_sitemaps = set()

    for sm_path in ["/sitemap_index.xml", "/sitemap.xml", "/product-sitemap.xml", "/wp-sitemap.xml"]:
        sm_url = urljoin(origin, sm_path)
        if is_valid_http_url(sm_url):
            sitemap_queue.append(sm_url)

    while sitemap_queue and len(seen_sitemaps) < 30:
        sm_url = sitemap_queue.pop(0)
        if sm_url in seen_sitemaps:
            continue
        seen_sitemaps.add(sm_url)
        try:
            r = await client.get(sm_url, follow_redirects=True, timeout=8.0)
            if r.status_code == 200 and ("<urlset" in r.text or "<sitemapindex" in r.text or "<?xml" in r.text):
                locs = extract_urls_from_xml(r.text)
                for loc in locs:
                    if loc.endswith(".xml") and loc not in seen_sitemaps:
                        sitemap_queue.append(loc)
                    else:
                        candidate_urls.append(loc)
        except Exception:
            pass

    to_visit = [base]
    visited = set()

    while to_visit and len(visited) < max_crawl_pages:
        current = to_visit.pop(0)
        if current in visited or not is_valid_http_url(current):
            continue
        visited.add(current)
        try:
            r = await client.get(current, follow_redirects=True, timeout=8.0)
            if r.status_code != 200 or "text/html" not in r.headers.get("content-type", ""):
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                full_url = urljoin(current, a["href"]).split("#")[0]
                if not is_valid_http_url(full_url):
                    continue
                p = urlparse(full_url)
                if p.netloc != parsed_base.netloc:
                    continue
                candidate_urls.append(full_url)

                low = full_url.lower()
                if any(kw in low for kw in [
                    "shop", "catalog", "catalogo", "prodott", "categoria",
                    "category", "collection", "noleggio", "macchin", "attrezzatur",
                    "page=", "/page/"
                ]):
                    if full_url not in visited and full_url not in to_visit and len(to_visit) < max_crawl_pages * 2:
                        to_visit.append(full_url)
        except Exception:
            pass

    all_products: List[str] = []
    seen_products = set()

    for u in candidate_urls:
        if u in seen_products:
            continue
        seen_products.add(u)
        if is_product_url(u, origin):
            all_products.append(u)

    return all_products


def detect_site_availability(html: str, title: str) -> Tuple[bool, str, List[str]]:
    soup = BeautifulSoup(html, "html.parser")
    text_blob = soup.get_text(" ", strip=True).lower()

    found_codes = set()
    for m in re.findall(r"\b[A-Z0-9]{5,18}\b", f"{title} {soup.get_text(' ', strip=True)}"):
        found_codes.add(normalize_code(m))

    out_of_stock_phrases = [
        "esaurito",
        "non disponibile",
        "out of stock",
        "sold out",
        "temporaneamente non disponibile",
        "attualmente non disponibile",
        "non acquistabile",
    ]
    in_stock_phrases = [
        "disponibile",
        "aggiungi al carrello",
        "acquista ora",
        "in stock",
    ]

    has_oos = any(p in text_blob for p in out_of_stock_phrases)
    has_add_to_cart = bool(
        soup.find(["button", "a", "input"], string=re.compile(r"aggiungi al carrello|acquista", re.I))
        or soup.find(["button", "input"], attrs={"name": re.compile(r"add-to-cart", re.I)})
    )

    if has_oos and not has_add_to_cart:
        return False, "Non disponibile / Esaurito sul sito", list(found_codes)
    if has_add_to_cart or any(p in text_blob for p in in_stock_phrases):
        return True, "Disponibile sul sito", list(found_codes)
    if has_oos:
        return False, "Segnalato non disponibile sul sito", list(found_codes)

    return True, "Stato da verificare (attivo su pagina)", list(found_codes)


async def fetch_single_product(client: httpx.AsyncClient, url: str) -> Optional[Dict[str, Any]]:
    if not is_valid_http_url(url):
        return None
    try:
        r = await client.get(url, follow_redirects=True, timeout=7.0)
        if r.status_code != 200:
            return {
                "nome": url.rstrip("/").split("/")[-1],
                "url": url,
                "available": False,
                "stato_sito": f"Non raggiungibile (HTTP {r.status_code})",
                "codes": [],
            }

        soup = BeautifulSoup(r.text, "html.parser")
        h1 = soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else (soup.title.get_text(" ", strip=True) if soup.title else url)
        avail, stato_sito, codes = detect_site_availability(r.text, title)
        return {
            "nome": title,
            "url": url,
            "available": avail,
            "stato_sito": stato_sito,
            "codes": codes,
        }
    except Exception as exc:
        return {
            "nome": url.rstrip("/").split("/")[-1],
            "url": url,
            "available": False,
            "stato_sito": f"Errore lettura: {exc}",
            "codes": [],
        }


def compare_single_item(
    idx: int,
    wp: Dict[str, Any],
    key_to_sheet: Dict[str, Dict[str, Any]],
    sheet_items: List[Dict[str, Any]],
    now_str: str,
) -> Dict[str, Any]:
    matched_sheet = None
    for c in wp["codes"]:
        if c in key_to_sheet:
            matched_sheet = key_to_sheet[c]
            break

    if not matched_sheet:
        wp_words = {w.lower() for w in re.findall(r"[A-Za-z0-9]{3,}", wp["nome"])}
        best_score = 0
        for sp in sheet_items:
            sp_words = {w.lower() for w in re.findall(r"[A-Za-z0-9]{3,}", sp["descrizione"])}
            if not sp_words:
                continue
            overlap = len(wp_words & sp_words)
            if overlap > best_score and overlap >= 2:
                best_score = overlap
                matched_sheet = sp

    sheet_stock = matched_sheet["stock"] if matched_sheet else None
    sheet_avail = (sheet_stock is not None and sheet_stock > 0)
    site_avail = bool(wp["available"])

    is_alert = False
    alert_type = "OK"
    alert_reason = "Allineato"

    if not site_avail:
        is_alert = True
        alert_type = "NON DISPONIBILE"
        alert_reason = wp["stato_sito"]
    elif matched_sheet is not None and not sheet_avail:
        is_alert = True
        alert_type = "ALERT STOCK 0"
        alert_reason = f"Attivo sul sito ma STOCK = {sheet_stock} nel foglio"
    elif matched_sheet is None:
        is_alert = False
        alert_type = "NON ABBINATO"
        alert_reason = "Nessun codice/descrizione corrispondente nel foglio"

    return {
        "id": idx,
        "nome": wp["nome"],
        "url": wp["url"],
        "codice": matched_sheet["codice"] if matched_sheet else "-",
        "stock_foglio": sheet_stock if sheet_stock is not None else "-",
        "disponibile_sito": "Sì" if site_avail else "No",
        "stato_sito": wp["stato_sito"],
        "alert": is_alert,
        "alert_type": alert_type,
        "motivo": alert_reason,
        "controllato_alle": now_str,
    }


async def run_monitoring_cycle() -> None:
    if STATE.get("scanning"):
        return

    sheet_url = clean_pasted_url(STATE.get("sheet_url", ""))
    site_url = clean_pasted_url(STATE.get("site_url", ""))
    if not sheet_url or not site_url:
        return

    STATE["scanning"] = True
    STATE["items"] = []
    STATE["alerts"] = []
    STATE["site_count"] = 0

    now = datetime.now(timezone.utc)
    now_str = now.strftime("%H:%M:%S")
    STATE["status_message"] = "Lettura Google Sheet e ricerca di tutti i prodotti sul sito..."

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
    try:
        async with httpx.AsyncClient(headers=headers) as client:
            sheet_items = await fetch_sheet_products(client, sheet_url)
            STATE["sheet_count"] = len(sheet_items)
            STATE["status_message"] = f"Caricati {len(sheet_items)} articoli dal Sheet — ricerca prodotti sul sito..."

            key_to_sheet: Dict[str, Dict[str, Any]] = {}
            for sp in sheet_items:
                for k in sp["keys"]:
                    if k and k not in key_to_sheet:
                        key_to_sheet[k] = sp

            urls = await discover_all_site_product_urls(client, site_url, max_crawl_pages=250)
            total_urls = len(urls)
            STATE["status_message"] = f"Trovati {total_urls} URL prodotto — analisi in corso: 0 / {total_urls}..."

            compared: List[Dict[str, Any]] = []
            alerts: List[Dict[str, Any]] = []
            batch_size = 8

            for i in range(0, total_urls, batch_size):
                chunk = urls[i:i + batch_size]
                fetched = await asyncio.gather(*(fetch_single_product(client, u) for u in chunk))
                for wp in fetched:
                    if not wp:
                        continue
                    row_obj = compare_single_item(
                        len(compared) + 1,
                        wp,
                        key_to_sheet,
                        sheet_items,
                        now_str,
                    )
                    compared.append(row_obj)
                    if row_obj["alert"]:
                        alerts.append(row_obj)

                STATE["site_count"] = len(compared)
                STATE["items"] = list(compared)
                STATE["alerts"] = list(alerts)
                STATE["status_message"] = f"Analisi in corso: {len(compared)} / {total_urls} prodotti — {len(alerts)} alert"
                await asyncio.sleep(0.1)

        STATE["last_run"] = now.isoformat()
        STATE["next_run"] = (now + timedelta(minutes=INTERVAL_MINUTES)).isoformat()
        STATE["status_message"] = f"Completato — {len(STATE['site_count'] if isinstance(STATE['site_count'], int) else len(STATE['items']))} prodotti analizzati, {len(STATE['alerts'])} alert trovati"
    except Exception as exc:
        STATE["status_message"] = f"Errore durante il controllo: {exc}"
        STATE["last_run"] = now.isoformat()
        STATE["next_run"] = (now + timedelta(minutes=INTERVAL_MINUTES)).isoformat()
    finally:
        STATE["scanning"] = False


def ensure_job_scheduled() -> None:
    if not scheduler.running:
        scheduler.start()
    job = scheduler.get_job("stock_monitor_30m")
    if job:
        scheduler.remove_job("stock_monitor_30m")
    scheduler.add_job(
        run_monitoring_cycle,
        "interval",
        minutes=INTERVAL_MINUTES,
        id="stock_monitor_30m",
        replace_existing=True,
    )


@app.on_event("startup")
async def on_startup() -> None:
    if not scheduler.running:
        scheduler.start()


@app.post("/api/start")
async def start_monitor(payload: ConfigPayload) -> Dict[str, Any]:
    STATE["sheet_url"] = clean_pasted_url(payload.sheet_url)
    STATE["site_url"] = clean_pasted_url(payload.site_url)
    STATE["running"] = True
    ensure_job_scheduled()
    asyncio.create_task(run_monitoring_cycle())
    return {"ok": True, "message": "Monitoraggio avviato (ogni 30 minuti)"}


@app.post("/api/scan-now")
async def scan_now() -> Dict[str, Any]:
    if not STATE.get("sheet_url") or not STATE.get("site_url"):
        return {"ok": False, "message": "Inserisci prima entrambi i link"}
    ensure_job_scheduled()
    asyncio.create_task(run_monitoring_cycle())
    return {"ok": True, "message": "Scansione avviata"}


@app.get("/api/status")
async def get_status() -> Dict[str, Any]:
    return STATE


@app.get("/", response_class=HTMLResponse)
async def home() -> str:
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()
