import base64
import html
import hashlib
import hmac
import ipaddress
import json
import mimetypes
import os
import re
import shutil
import socket
import sqlite3
import threading
import time
import uuid
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from io import BytesIO
from html.parser import HTMLParser
from pathlib import Path
from socketserver import ThreadingMixIn
from urllib.parse import quote, urljoin, urlparse, urlunparse
from wsgiref.simple_server import WSGIServer, make_server

try:
    import truststore
except ImportError:
    truststore = None
if truststore:
    truststore.inject_into_ssl()

import requests
from bs4 import BeautifulSoup
from PIL import Image, ImageOps

PILImage = Image

ROOT = Path(__file__).parent
PORT = int(os.environ.get("PORT", "8080"))
DATA_ROOT = Path(os.environ.get("DATA_ROOT", str(ROOT / "data")))
PACKAGES_ROOT = DATA_ROOT / "packages"
DB_PATH = DATA_ROOT / "block3.sqlite3"
ALLOWED_ROOT_DOMAINS = ("olx.ua", "rieltor.ua")
TELEGRAPH_API = "https://api.telegra.ph"
ACCESS_TOKEN = os.environ.get("TELEGRAPH_ACCESS_TOKEN")
MEDIA_GITHUB_REPO = os.environ.get("KYIV_ESTATE_MEDIA_GITHUB_REPO", "").strip()
MEDIA_GITHUB_BRANCH = os.environ.get("KYIV_ESTATE_MEDIA_GITHUB_BRANCH", "media").strip() or "media"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
MEDIA_UPLOAD_LOCK = threading.Lock()
PUBLICATION_LOCK = threading.RLock()
LOGO_URL = os.environ.get("KYIV_ESTATE_LOGO_URL", "").strip()
LOGO_PATH = Path(os.environ.get("KYIV_ESTATE_LOGO_PATH", str(ROOT / "assets" / "kyiv-estate-logo.jpg"))).expanduser()
BUILTIN_LOGO_URL = "https://raw.githubusercontent.com/realestateplatformapi-lang/listing-telegraph/main/assets/kyiv-estate-logo.jpg"
PUBLIC_BASE_URL = os.environ.get("KYIV_ESTATE_PUBLIC_BASE_URL", "").strip().rstrip("/")
if not PUBLIC_BASE_URL and os.environ.get("RAILWAY_PUBLIC_DOMAIN"):
    PUBLIC_BASE_URL = "https://" + os.environ["RAILWAY_PUBLIC_DOMAIN"].strip().strip("/")
CONTACT_PHONE = os.environ.get("KYIV_ESTATE_PHONE", "+380 98 155 9900").strip()
CONTACT_URL = os.environ.get("KYIV_ESTATE_URL", "https://kyiv.estate/").strip()
CONTACT_LINKS = {
    "Instagram": os.environ.get("KYIV_ESTATE_INSTAGRAM", "").strip(),
    "Telegram": os.environ.get("KYIV_ESTATE_TELEGRAM", "https://t.me/Real_Estate_Agency_premium").strip(),
    "WhatsApp": os.environ.get("KYIV_ESTATE_WHATSAPP", "https://api.whatsapp.com/send/?phone=380981559900&text&type=phone_number&app_absent=0").strip(),
    "Facebook": os.environ.get("KYIV_ESTATE_FACEBOOK", "").strip(),
    "Email": os.environ.get("KYIV_ESTATE_EMAIL", "info@kyiv.estate").strip(),
}
AI_ENDPOINT = os.environ.get("KYIV_ESTATE_AI_ENDPOINT", "").strip().rstrip("/")
AI_PACKAGES_ROOT = Path(os.environ.get("KYIV_ESTATE_AI_PACKAGES_ROOT", "")) if os.environ.get("KYIV_ESTATE_AI_PACKAGES_ROOT") else None
AI_MODE = os.environ.get("KYIV_ESTATE_AI_MODE", "browser").strip().lower() or "browser"
AI_TOKEN = os.environ.get("KYIV_ESTATE_AI_TOKEN", "").strip()
AI_BRIDGE_ENABLED = os.environ.get("KYIV_ESTATE_AI_BRIDGE_ENABLED", "false").lower() == "true"
SOURCE_LISTINGS_ROOT = Path(os.environ.get("KYIV_ESTATE_SOURCE_LISTINGS_ROOT", "")) if os.environ.get("KYIV_ESTATE_SOURCE_LISTINGS_ROOT") else None
AI_REQUIRED = os.environ.get("KYIV_ESTATE_AI_REQUIRED", "false").lower() == "true"
AI_TIMEOUT_SECONDS = max(60, int(os.environ.get("KYIV_ESTATE_AI_TIMEOUT_SECONDS", "1800")))
MAX_AI_PHOTOS = max(1, min(100, int(os.environ.get("KYIV_ESTATE_MAX_AI_PHOTOS", "100"))))
SHEETS_WEBHOOK_URL = os.environ.get(
    "KYIV_ESTATE_SHEETS_WEBHOOK_URL",
    "https://script.google.com/macros/s/AKfycbwKC9uWanGXZVgQPDwlEwj9gYdxL65A6IyVRHxxLWQP4BVGDRRc1WcwPzqgmZfXRS8H/exec",
).strip()
SHEETS_WEBHOOK_SECRET = os.environ.get("KYIV_ESTATE_SHEETS_WEBHOOK_SECRET", "").strip()
SHEETS_OUTBOX_ROOT = DATA_ROOT / "sheets_outbox"
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.9,ru;q=0.8,en;q=0.7",
}
LISTING_CACHE = {}
CACHE_TTL_SECONDS = 60 * 60 * 6
RATE_CACHE = {"at": 0, "value": None}
BANNED_PUBLIC_PHRASES = (
    "olx", "rieltor", "рієлтор", "риелтор", "коміс", "власник",
    "агентств", "зателефон", "дзвон", "зустріч", "internal review draft",
)


@contextmanager
def database():
    connection = sqlite3.connect(DB_PATH, timeout=30)
    try:
        yield connection
        connection.commit()
    except Exception as error:
        print(f"Database transaction error: {type(error).__name__}: {error}", flush=True)
        connection.rollback()
        raise
    finally:
        connection.close()


def init_storage():
    PACKAGES_ROOT.mkdir(parents=True, exist_ok=True)
    with database() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                input_value TEXT NOT NULL,
                phase TEXT NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                snapshot_json TEXT,
                package_path TEXT,
                uk_url TEXT,
                en_url TEXT,
                error TEXT,
                retries INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS uploaded_assets (
                sha256 TEXT PRIMARY KEY,
                local_path TEXT NOT NULL,
                public_url TEXT NOT NULL,
                uploaded_at TEXT NOT NULL
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS ai_bridge_jobs (
                id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, status TEXT NOT NULL,
                error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS publication_state (
                identity_key TEXT PRIMARY KEY,
                internal_id TEXT NOT NULL,
                source_url TEXT NOT NULL,
                uk_url TEXT,
                en_url TEXT,
                uk_hash TEXT,
                en_hash TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)


def reclaim_package_space(keep_id=""):
    if not PACKAGES_ROOT.exists():
        return
    target_free = max(128, int(os.environ.get("KYIV_ESTATE_MIN_FREE_MB", "512"))) * 1024 * 1024
    try:
        if shutil.disk_usage(PACKAGES_ROOT).free >= target_free:
            return
    except OSError:
        return
    keep_count = max(2, int(os.environ.get("KYIV_ESTATE_PACKAGE_CACHE_KEEP", "6")))
    for cache_root in (PACKAGES_ROOT, DATA_ROOT / "listings"):
        if not cache_root.exists():
            continue
        candidates = [path for path in cache_root.iterdir() if path.is_dir() and path.name != keep_id]
        candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
        removable = candidates[keep_count:] or (candidates[2:] if len(candidates) > 2 else [])
        for cache_path in reversed(removable):
            try:
                shutil.rmtree(cache_path)
                print(f"Pruned stale package cache: {cache_path.name}", flush=True)
            except OSError as cleanup_error:
                print(f"Could not prune package cache {cache_path.name}: {cleanup_error}", flush=True)
                continue
            try:
                if shutil.disk_usage(PACKAGES_ROOT).free >= target_free:
                    return
            except OSError:
                return


def update_job(job_id, input_value, phase, progress=0, snapshot=None, package_path=None, uk_url=None, en_url=None, error=None):
    try:
        with database() as db:
            db.execute("""
                INSERT INTO jobs (id, input_value, phase, progress, snapshot_json, package_path, uk_url, en_url, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    phase=excluded.phase, progress=excluded.progress,
                    snapshot_json=COALESCE(excluded.snapshot_json, jobs.snapshot_json),
                    package_path=COALESCE(excluded.package_path, jobs.package_path),
                    uk_url=COALESCE(excluded.uk_url, jobs.uk_url),
                    en_url=COALESCE(excluded.en_url, jobs.en_url),
                    error=excluded.error, updated_at=CURRENT_TIMESTAMP
            """, (job_id, input_value, phase, progress, json.dumps(snapshot, ensure_ascii=False) if snapshot else None, package_path, uk_url, en_url, error))
    except sqlite3.OperationalError as storage_error:
        if "full" not in str(storage_error).lower():
            raise
        print(f"Storage full: skipped non-critical job metadata for {job_id}", flush=True)


def normalize_listing_input(raw):
    value = str(raw or "").strip()
    if re.fullmatch(r"\d{5,12}", value):
        return f"https://rieltor.ua/flats-sale/view/{value}/"
    return value


def canonical_listing_url(raw):
    value = normalize_listing_input(raw)
    parsed = urlparse(value)
    host = (parsed.hostname or "").lower()
    if not host:
        return value.strip()
    port = f":{parsed.port}" if parsed.port and parsed.port != 443 else ""
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if host.endswith("rieltor.ua"):
        path = path.rstrip("/") + "/"
    elif host.endswith("olx.ua"):
        path = path.rstrip("/")
    return urlunparse((parsed.scheme.lower() or "https", host + port, path, "", "", ""))


def external_listing_id(source_url):
    canonical = canonical_listing_url(source_url)
    parsed = urlparse(canonical)
    host = (parsed.hostname or "").lower()
    if host.endswith("rieltor.ua"):
        found = re.search(r"/view/(\d+)(?:/|$)", parsed.path, re.IGNORECASE)
        if found:
            return "rieltor:" + found.group(1)
    if host.endswith("olx.ua"):
        found = re.search(r"-ID([A-Za-z0-9]+)\.html(?:/|$)", parsed.path, re.IGNORECASE)
        if found:
            return "olx:id" + found.group(1).casefold()
    digest = hashlib.blake2s(canonical.casefold().encode(), digest_size=12).hexdigest()
    return "url:" + digest


def listing_id(source_url):
    digest = hashlib.blake2s(external_listing_id(source_url).encode(), digest_size=8).digest()
    return "kyivestate-" + str(10_000_000 + int.from_bytes(digest, "big") % 90_000_000)


class ListingParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.meta, self.json_ld, self._script = {}, [], False
        self._script_text = []
        self.title, self._in_title = "", False
        self._text_tag, self._text_parts = None, []
        self.text_blocks, self.images = [], []
        self._description_depth, self._description_parts = 0, []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "meta":
            key = attrs.get("property") or attrs.get("name")
            value = attrs.get("content")
            if key and value:
                self.meta.setdefault(key.lower(), []).append(value)
        if tag == "script" and attrs.get("type", "").lower() == "application/ld+json":
            self._script, self._script_text = True, []
        if tag == "title":
            self._in_title = True
        if tag in {"h1", "h2", "p", "li"}:
            self._text_tag, self._text_parts = tag, []
        if tag == "img":
            for key in ("src", "data-src", "data-lazy-src", "data-original"):
                if attrs.get(key):
                    self.images.append(attrs[key])
                    break
        classes = set(attrs.get("class", "").split())
        if tag == "div" and "offer-view-section-text" in classes:
            self._description_depth, self._description_parts = 1, []
        elif self._description_depth:
            self._description_depth += 1

    def handle_endtag(self, tag):
        if tag == "script" and self._script:
            self._script = False
            try:
                self.json_ld.append(json.loads("".join(self._script_text)))
            except json.JSONDecodeError:
                pass
        if tag == "title":
            self._in_title = False
        if tag == self._text_tag:
            text = " ".join(self._text_parts).strip()
            if text and len(text) > 15:
                self.text_blocks.append(text)
            self._text_tag, self._text_parts = None, []
        if self._description_depth:
            self._description_depth -= 1
            if self._description_depth == 0:
                text = " ".join(self._description_parts).strip()
                if text:
                    self.text_blocks.insert(0, text)

    def handle_data(self, data):
        if self._script:
            self._script_text.append(data)
        if self._in_title:
            self.title += data
        if self._text_tag:
            self._text_parts.append(data.strip())
        if self._description_depth:
            self._description_parts.append(data.strip())


def reject_unsafe_url(raw):
    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower()
    valid_host = any(host == domain or host.endswith("." + domain) for domain in ALLOWED_ROOT_DOMAINS)
    if parsed.scheme != "https" or not valid_host:
        raise ValueError("Підтримуються лише HTTPS-посилання на OLX.ua та Rieltor.ua.")
    for result in socket.getaddrinfo(parsed.hostname, 443, type=socket.SOCK_STREAM):
        ip = ipaddress.ip_address(result[4][0])
        if not ip.is_global:
            raise ValueError("Небезпечна адреса сайту.")


def safe_remote_url(raw):
    parsed = urlparse(str(raw))
    if parsed.scheme != "https" or not parsed.hostname:
        return False
    try:
        return all(ipaddress.ip_address(item[4][0]).is_global for item in socket.getaddrinfo(parsed.hostname, 443, type=socket.SOCK_STREAM))
    except socket.gaierror:
        return False


def first(data, *keys):
    for key in keys:
        values = data.get(key.lower(), [])
        if values:
            return values[0]
    return ""


def flatten_jsonld(value):
    if isinstance(value, list):
        for item in value:
            yield from flatten_jsonld(item)
    elif isinstance(value, dict):
        yield value
        if "@graph" in value:
            yield from flatten_jsonld(value["@graph"])


def listing_photo_urls(images, page_url):
    page_host = (urlparse(page_url).hostname or "").lower()
    accepted = []
    for image in images:
        parsed = urlparse(image)
        host, path = (parsed.hostname or "").lower(), parsed.path.lower()
        keep = True
        if page_host.endswith("rieltor.ua"):
            keep = (
                host in {"market-images.lunstatic.net", "rieltor-images.lunstatic.net"}
                and ("/images/offers/" in path or "/offers/" in path)
                and "/310/310/" not in path
            )
        elif page_host.endswith("olx.ua"):
            keep = host.endswith("apollo.olxcdn.com") and "/v1/files/" in path
        if keep and image not in accepted:
            accepted.append(image)
    return accepted


def extract_listing(source_url):
    source_url = canonical_listing_url(source_url)
    reject_unsafe_url(source_url)
    job_id = listing_id(source_url)
    update_job(job_id, source_url, "resolve", 10)
    cache_key = source_url.rstrip("/")
    cached = LISTING_CACHE.get(cache_key)
    if cached and time.time() - cached[0] < CACHE_TTL_SECONDS:
        return cached[1]
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)
    response = None
    for attempt in range(2):
        response = session.get(source_url, timeout=20)
        if response.status_code != 429:
            break
        try:
            delay = min(int(response.headers.get("Retry-After", "3")), 10)
        except ValueError:
            delay = 3
        if attempt == 0:
            try:
                session.get("https://rieltor.ua/", timeout=15)
            except requests.RequestException:
                pass
            time.sleep(delay)
    if response.status_code == 429:
        update_job(job_id, source_url, "error", 10, error="Rieltor rate limit")
        raise ValueError("Rieltor тимчасово обмежив запити. Зачекайте 1–2 хвилини й повторіть спробу.")
    response.raise_for_status()
    canonical_source = canonical_listing_url(response.url)
    job_id = listing_id(canonical_source)
    update_job(job_id, source_url, "ingest", 30)
    parser = ListingParser()
    parser.feed(response.text)
    title = first(parser.meta, "og:title", "twitter:title") or parser.title.strip()
    description = first(parser.meta, "og:description", "description", "twitter:description")
    page_text = "\n\n".join(parser.text_blocks)[:20_000]
    if (urlparse(response.url).hostname or "").endswith("rieltor.ua"):
        description_node = BeautifulSoup(response.text, "html.parser").select_one(".offer-view-section-text")
        description = description_node.get_text("\n", strip=True) if description_node else page_text
    else:
        description = description or page_text
    images = parser.meta.get("og:image", []) + parser.meta.get("twitter:image", []) + parser.images
    if (urlparse(response.url).hostname or "").endswith("rieltor.ua"):
        images.extend(re.findall(
            r"https://(?:market-images|rieltor-images)\.lunstatic\.net/[^\"'<>\s]*/offers/[^\"'<>\s]+?\.(?:jpe?g|png)(?:\?[^\"'<>\s]*)?",
            response.text,
            re.IGNORECASE,
        ))
    for item in flatten_jsonld(parser.json_ld):
        title = title or str(item.get("name", ""))
        candidate_description = str(item.get("description", "")).strip()
        if len(candidate_description) > len(description or ""):
            description = candidate_description
        image = item.get("image", [])
        images.extend(image if isinstance(image, list) else [image])
    clean_images = []
    for image in images:
        if isinstance(image, dict): image = image.get("url", "")
        if isinstance(image, str):
            absolute = urljoin(response.url, image)
            if absolute.startswith("https://") and absolute not in clean_images:
                clean_images.append(absolute)
    blocked_markers = ("just a moment", "access denied", "checking your browser", "captcha")
    if not title or any(marker in title.lower() for marker in blocked_markers):
        raise ValueError("Не вдалося прочитати оголошення. Можливо, сайт запросив перевірку або змінив розмітку.")
    page_plain = html.unescape(re.sub(r"<[^>]*>", " ", response.text))
    page_plain = re.sub(r"\s+", " ", page_plain)
    clean_title = sanitize_title(html.unescape(title).strip())
    original_description = strip_listing_references(html.unescape(description)).strip()[:20_000]
    clean_description = sanitize_public_text(original_description)[:20_000]
    detail_text = " ".join(parser.meta.get("og:description", [])) + " " + page_plain + " " + original_description
    details = extract_details(detail_text, clean_title)
    details.update(extract_contact_details(page_plain))
    details["address"] = extract_address(detail_text, response.url, html.unescape(title).strip())
    prices = convert_prices(details.get("price"), details.get("currency"))
    discovered_photo_count = len(clean_images)
    listing_images = listing_photo_urls(clean_images, canonical_source)
    clean_images = visually_unique_preview_urls(listing_images)
    listing = {
        "internal_id": job_id,
        "external_id": external_listing_id(canonical_source),
        "title": clean_title,
        "description": clean_description,
        "original_description": original_description,
        "ai_description": editorial_ai_text(original_description, clean_title),
        "images": clean_images,
        "source": canonical_source,
        "details": details,
        "prices": prices,
        "media_audit": {
            "discovered": discovered_photo_count,
            "listing_only": len(listing_images),
            "unique_urls": len(clean_images),
            "unique_visuals": len(clean_images),
            "duplicates_removed": max(0, len(listing_images) - len(clean_images)),
            "method": "source-url-normalization",
        },
        "phase": "ready",
    }
    LISTING_CACHE[cache_key] = (time.time(), listing)
    update_job(job_id, source_url, "ready", 100, snapshot=listing)
    return listing


def extract_contact_details(page_text):
    text_value = html.unescape(page_text or "")
    lower = text_value.lower()
    if re.search(r"\b(?:ріелтор\w*|риелтор\w*|realtor\w*|агент\w*|broker\w*)\b", lower):
        agent_type = "realtor"
    elif re.search(r"\b(?:власник\w*|владелец\w*|owner\w*)\b", lower):
        agent_type = "owner"
    else:
        agent_type = ""
    phone_match = re.search(r"(?<!\d)(?:\+?38)?[\s\-()]*0\d{2}[\s\-()]*\d{3}[\s\-()]*\d{2}[\s\-()]*\d{2}(?!\d)", text_value)
    phone = re.sub(r"\s+", " ", phone_match.group(0)).strip() if phone_match else ""
    name = ""
    role_pattern = r"(?:ріелтор\w*|риелтор\w*|realtor\w*|агент\w*|broker\w*)\s*[:\-–]?\s*([A-ZА-ЯІЇЄ][A-Za-zА-Яа-яІіЇїЄє'’\-]+(?:\s+[A-ZА-ЯІЇЄ][A-Za-zА-Яа-яІіЇїЄє'’\-]+){0,2})"
    name_match = re.search(role_pattern, text_value)
    if name_match:
        candidate = name_match.group(1).strip()
        if not re.search(r"\b(?:тел|phone|зателефону|contact)\b", candidate, re.IGNORECASE):
            name = candidate
    return {"agent_type": agent_type, "agent_name": name, "agent_phone": phone}


def extract_details(page_text, classification_text=""):
    def match(pattern):
        found = re.search(pattern, page_text, re.IGNORECASE)
        return found.groups() if found else ()
    price = match(r"(?<!\d)(\d[\d\s\u00a0]*(?:[.,]\d+)?)\s*(USD|EUR|грн\.?|\$|€|₴)(?!\w)")
    price_per_m2 = match(r"(?<!\d)(\d[\d\s\u00a0]*(?:[.,]\d+)?)\s*(USD|EUR|грн\.?|\$|€|₴)\s*/\s*(?:кв\.?\s*м\.?|м[²2]|m[²2])")
    area = match(r"(\d[\d\s\u00a0]*(?:[.,]\d+)?)\s*/\s*\d+(?:[.,]\d+)?\s*/\s*\d+(?:[.,]\d+)?\s*(?:м²|м2|m²|m2)") or match(r"(\d[\d\s\u00a0]*(?:[.,]\d+)?)\s*(?:м²|м2|m²|m2)")
    floor = match(r"(?:поверх|пов\.)\s*(\d+)\s*(?:з|/)\s*(\d+)") or match(r"(\d+)\s*(?:поверх|пов\.)\s*(?:з|/)\s*(\d+)") or match(r"(\d+)\s*поверх\s*(\d+)\s*-?\s*пов") or match(r"(?:поверх|пов\.)\s*(\d+)")
    storeys = match(r"\b(\d+)\s*[-–]?\s*(?:х|x)?\s*поверх\w*") or match(r"(?:поверховість|поверхів|этажность|этажей)\s*[:№-]?\s*(\d+)")
    room_values = []
    for pattern in (
        r"(?:кількість\s+)(?:кімнат|комнат|rooms?)\s*[:№-]?\s*(\d{1,2})(?!\d)",
        r"\b(\d{1,2})\s*[-–]?\s*(?:кімнат\w*|комнат\w*|rooms?)\b",
        r"(?:кімнат|комнат|rooms?)\s*[:№-]?\s*(\d{1,2})(?!\s*(?:кв|м[²2]))",
    ):
        room_values = [int(value) for value in re.findall(pattern, page_text, re.IGNORECASE) if 0 < int(value) <= 20]
        if room_values:
            break
    lowered = (classification_text or page_text).casefold()
    property_type = "house" if re.search(r"\b(?:будин\w*|дом\w*|котедж\w*|house\w*|townhouse|таунхаус\w*)\b", lowered) else "commercial" if re.search(r"\b(?:комерц\w*|склад\w*|офіс\w*|магазин\w*|warehouse|office|retail)\b", lowered) else "apartment"
    land = match(r"(?:(?:площа\s+)?(?:земельн\w*\s+)?(?:ділян\w*|участ\w*|land\s+(?:area|plot)?))\s*[:№-]?\s*(\d+(?:[.,]\d+)?)\s*(сот(?:\.?|ок|ки)?|га|гектар\w*|hectares?)")
    if property_type == "house" and not land:
        land = match(r"\b(\d+(?:[.,]\d+)?)\s*(сот(?:\.?|ок|ки)?|га|гектар\w*|hectares?)\b")
    complex_name = ""
    complex_match = re.search(
        r"\b(?:ЖК|житлов(?:ий|ого)\s+комплекс)\s*[«\"“]([^»\"”\n]{2,80})[»\"”]",
        page_text,
        re.IGNORECASE,
    )
    if complex_match:
        candidate = re.sub(r"\s+", " ", complex_match.group(1)).strip(" ,.;:-")
        if not re.search(r"\b(?:продаж|оренд|квартир|будинок|комерц|вул(?:иця)?|район)\b", candidate, re.IGNORECASE):
            complex_name = candidate
    currency = {"$": "USD", "USD": "USD", "€": "EUR", "EUR": "EUR", "₴": "UAH", "грн": "UAH", "грн.": "UAH"}
    house_storeys = storeys[0] if property_type == "house" and storeys else (floor[0] if property_type == "house" and len(floor) == 1 else "")
    land_area = ""
    if land:
        land_unit = "соток" if land[1].casefold().startswith("сот") else "га" if land[1].casefold().startswith(("га", "гект")) else land[1]
        land_area = f"{land[0]} {land_unit}"
    def normalized_number(value):
        value = re.sub(r"[\s\u00a0]+", "", str(value or "")).replace(",", ".")
        return value if re.fullmatch(r"\d+(?:\.\d+)?", value) else ""
    normalized_price = normalized_number(price[0]) if price else ""
    normalized_price_per_m2 = normalized_number(price_per_m2[0]) if price_per_m2 else ""
    normalized_area = normalized_number(area[0]) if area else ""
    return {
        "price": normalized_price,
        "currency": currency.get(price[1].upper(), "UAH") if price else "UAH",
        "source_price_raw": " ".join(price) if price else "",
        "price_amount": normalized_price,
        "price_currency": currency.get(price[1].upper(), "UAH") if price else "",
        "price_per_m2": normalized_price_per_m2,
        "price_per_m2_currency": currency.get(price_per_m2[1].upper(), "UAH") if price_per_m2 else "",
        "area": normalized_area,
        "area_total_m2": normalized_area,
        "floor": floor[0] if floor else "",
        "total_floors": floor[1] if len(floor) > 1 else house_storeys,
        "rooms": str(room_values[0]) if room_values else "",
        "land_area": land_area,
        "property_type": property_type,
        "residential_complex": complex_name,
    }


def extract_address(page_text, source_url, title=""):
    combined = "\n".join((str(title or ""), str(page_text or "")))
    exact = re.search(
        r"(?<!\w)(?:(?:вул(?:иця)?|ул(?:ица)?|просп(?:ект)?|пр-т|пров(?:улок)?|пер(?:еулок)?|наб(?:ережна)?|бульвар|узвіз)\.?\s+[^,;\n]{1,70}?(?:,\s*)?(?:будинок|буд\.|д\.)?\s*\d{1,4}[а-яіїєґa-z0-9/-]*)",
        combined,
        re.IGNORECASE,
    )
    if exact and "rieltor.ua" not in source_url.lower():
        return re.sub(r"\s+", " ", exact.group(0)).strip(" ,·-")
    if "olx.ua" in source_url.lower():
        location = re.search(r'\\"location\\":\{([^{}]{0,3000})\}', combined)
        if location:
            values = {}
            for key in ("cityName", "districtName", "streetName", "street", "houseNumber", "buildingNumber"):
                match = re.search(r'\\"' + key + r'\\":\\"([^\\"]*)\\"', location.group(1))
                if match:
                    try:
                        values[key] = json.loads('"' + match.group(1) + '"')
                    except json.JSONDecodeError:
                        values[key] = match.group(1)
            street = values.get("streetName") or values.get("street")
            number = values.get("houseNumber") or values.get("buildingNumber")
            city = values.get("cityName")
            district = values.get("districtName")
            if street and number:
                return ", ".join(item for item in (city, f"{street}, {number}") if item)
            if city and district:
                return f"{city}, {district}"
            if city:
                return city
    if "rieltor.ua" not in source_url.lower():
        return ""
    from_title = re.search(r":\s*(.+?)\s*[-–]\s*(?:Оголошення|Announcement)\b", title, re.IGNORECASE)
    if from_title:
        return re.sub(r"\s+", " ", from_title.group(1)).strip(" ,·-")
    patterns = (
        r"(?:Київ|Киев)\s*[,·-]?\s*[^,]{0,55}?(?:вул\.|вулиця|проспект|пр-т|наб\.|пров\.)\s*[^,]{2,70}",
        r"(?:вул\.|вулиця|проспект|пр-т|наб\.|пров\.)\s*[^,]{2,70}",
    )
    for pattern in patterns:
        found = re.search(pattern, page_text, re.IGNORECASE)
        if found:
            return re.sub(r"\s+", " ", found.group(0)).strip(" ,·-")
    return ""


def strip_listing_references(value):
    return re.sub(r"\s*[-–—|·]?\s*(?:оголошення|advertisement|announcement)\s*#?\s*№?\s*\d+\b\s*[:|·—-]*", "", str(value or ""), flags=re.IGNORECASE)


def sanitize_title(value):
    value = strip_listing_references(value)
    value = re.sub(r"^\s*(?:advertisement|оголошення)\s*#?\s*\d*\s*[:|·—-]*\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"(?:Ресурс|Resource)\s*\d+.*$", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s*(?:[-|·]\s*)?(?:RIELTOR\.UA|OLX(?:\.UA)?)\s*$", "", value, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", value).strip(" \"'—-")


def sanitize_public_text(value):
    value = strip_listing_references(html.unescape(re.sub(r"<[^>]+>", " ", value)))
    value = re.sub(r"[\t\r ]+", " ", value).replace(" ,", ",").replace(" .", ".")
    kept = []
    seen_sentences = set()
    for paragraph in re.split(r"\n+", value):
        sentences = re.split(r"(?<=[.!?])\s+", paragraph.strip())
        safe = []
        for sentence in sentences:
            sentence = sentence.strip(" \"'")
            key = re.sub(r"\s+", " ", sentence).casefold()
            prohibited = (
                r"\b(?:без|є|есть|no|without|with)\s+(?:коміс\w*|комисс\w*|commission\w*)\b",
                r"\b(?:ріелтор\w*|риелтор\w*|realtor\w*|агент\w*|broker\w*)\b",
                r"\b(?:власник\w*|владелец\w*|owner\w*)\b",
                r"\b(?:дзвон\w*|звон\w*|телефону\w*|phone|contact\s+me)\b",
                r"\b(?:оригінал\s+оголошення|оригинал\s+объявления|original\s+listing)\b",
                r"^\s*(?:оновлено|обновлено|updated)\s*:",
                r"\b(?:торг\w*|торгів\w*|bargain\w*|negotiable)\b",
            )
            if not sentence or key in seen_sentences or any(phrase in key for phrase in BANNED_PUBLIC_PHRASES) or any(re.search(pattern, key, re.IGNORECASE) for pattern in prohibited):
                continue
            seen_sentences.add(key)
            safe.append(sentence)
        if safe:
            kept.append(" ".join(safe))
    return "\n\n".join(kept).strip()


def editorial_ai_text(value, title=""):
    clean = sanitize_public_text(value)
    if title:
        clean = re.sub(re.escape(sanitize_title(title)), "", clean, flags=re.IGNORECASE)
        clean = re.sub(r"^[\s:—-]+", "", clean)
    sentences = [item.strip() for item in re.split(r"(?<=[.!?])\s+", clean.replace("\n", " ")) if item.strip()]
    paragraphs = []
    for start in range(0, len(sentences), 3):
        paragraph = " ".join(sentences[start:start + 3])
        if paragraph:
            paragraphs.append(paragraph[0].upper() + paragraph[1:] if len(paragraph) > 1 else paragraph.upper())
    polished_title = sanitize_title(title)
    if polished_title:
        intro = f"Представляємо ретельно відібрану пропозицію: {polished_title}. Об’єкт поєднує виразні характеристики та потенціал, який варто оцінити особисто."
    else:
        intro = "Представляємо ретельно відібрану пропозицію нерухомості з характеристиками, що заслуговують на увагу."
    closing = "Запрошуємо ознайомитися з деталями та оцінити всі переваги об’єкта особисто."
    candidate = "\n\n".join([intro, *paragraphs, closing]).strip()
    original_key = re.sub(r"\W+", "", clean).casefold()
    candidate_key = re.sub(r"\W+", "", candidate).casefold()
    if clean and (candidate_key == original_key or candidate_key.startswith(original_key)):
        candidate = f"{intro}\n\n{clean}\n\n{closing}"
    return candidate[:20_000]


def clean_image_urls(image_urls):
    cleaned = []
    seen = set()
    for value in image_urls or []:
        if not isinstance(value, str) or not safe_remote_url(value):
            continue
        parsed = urlparse(value.strip())
        olx_file = re.search(r"/v1/files/([^/;?]+)/image", parsed.path, re.IGNORECASE)
        normalized = ("olx:" + olx_file.group(1).lower()) if olx_file and parsed.netloc.lower().endswith("olxcdn.com") else urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, "", "", ""))
        if normalized in seen:
            continue
        seen.add(normalized)
        cleaned.append(value.strip())
    return cleaned


def nbu_rates():
    if RATE_CACHE["value"] and time.time() - RATE_CACHE["at"] < 60 * 60 * 6:
        return RATE_CACHE["value"]
    response = requests.get("https://bank.gov.ua/NBUStatService/v1/statdirectory/exchangenew", params={"json": ""}, timeout=15)
    response.raise_for_status()
    by_code = {row.get("cc"): row for row in response.json()}
    if "USD" not in by_code or "EUR" not in by_code:
        raise RuntimeError("НБУ не повернув курси USD та EUR.")
    value = {
        "USD": float(by_code["USD"]["rate"]),
        "EUR": float(by_code["EUR"]["rate"]),
        "date": by_code["USD"].get("exchangedate", ""),
        "source": "National Bank of Ukraine",
    }
    RATE_CACHE.update({"at": time.time(), "value": value})
    return value


def parse_number(value):
    cleaned = re.sub(r"[^\d,.-]", "", str(value or "")).replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def convert_prices(value, currency):
    amount = parse_number(value)
    if amount is None:
        return {"UAH": "", "USD": "", "EUR": "", "rate_date": "", "rate_source": ""}
    try:
        rates = nbu_rates()
    except (requests.RequestException, RuntimeError, ValueError):
        return {"UAH": str(value).strip(), "USD": "", "EUR": "", "rate_date": "", "rate_source": "unavailable"} if currency == "UAH" else {"UAH": "", "USD": str(value).strip() if currency == "USD" else "", "EUR": str(value).strip() if currency == "EUR" else "", "rate_date": "", "rate_source": "unavailable"}
    uah = amount if currency == "UAH" else amount * rates[currency]
    result = {
        "UAH": str(round(uah)),
        "USD": str(round(uah / rates["USD"])),
        "EUR": str(round(uah / rates["EUR"])),
        "rate_date": rates["date"],
        "rate_source": rates["source"],
    }
    return result


def translate_to_english(text):
    if not text.strip():
        return ""
    translated = []
    for part in text.splitlines() or [text]:
        if not part.strip():
            translated.append("")
            continue
        response = requests.get("https://translate.googleapis.com/translate_a/single", params={"client": "gtx", "sl": "auto", "tl": "en", "dt": "t", "q": part[:4500]}, timeout=15)
        response.raise_for_status()
        translated.append("".join(segment[0] for segment in response.json()[0] if segment[0]))
    return "\n".join(translated)


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def visual_fingerprint(path):
    with Image.open(path) as source:
        image = ImageOps.exif_transpose(source).convert("L")
        width, height = image.size
        pixels = list(image.resize((17, 16), Image.Resampling.LANCZOS).getdata())
    value = 0
    for row in range(16):
        start = row * 17
        for column in range(16):
            value = (value << 1) | int(pixels[start + column] > pixels[start + column + 1])
    return width, height, value


def visual_fingerprint_bytes(content):
    with Image.open(BytesIO(content)) as source:
        source.thumbnail((512, 512), Image.Resampling.LANCZOS)
        image = ImageOps.exif_transpose(source).convert("L")
        width, height = image.size
        pixels = list(image.resize((17, 16), Image.Resampling.LANCZOS).getdata())
    value = 0
    for row in range(16):
        start = row * 17
        for column in range(16):
            value = (value << 1) | int(pixels[start + column] > pixels[start + column + 1])
    return width, height, value


def same_visual_photo(candidate, fingerprints):
    width, height, signature = visual_fingerprint(candidate)
    for other_width, other_height, other_signature in fingerprints:
        if abs(width / max(height, 1) - other_width / max(other_height, 1)) < 0.012 and (signature ^ other_signature).bit_count() <= 12:
            return True
    return False


def visually_unique_preview_urls(urls):
    # Do not fetch source photos merely to deduplicate them.  OLX file tokens
    # and normalized source URLs are stable enough for deterministic dedupe and
    # keep the server free of photo downloads/caches during extraction.
    return clean_image_urls(urls)


def display_price(details, prices):
    symbols = {"UAH": "грн", "USD": "$", "EUR": "€"}
    def format_amount(value):
        number = parse_number(value)
        if number is not None and number.is_integer():
            return f"{number:,.0f}".replace(",", " ")
        return str(value).strip().replace("\u00a0", " ")

    primary_amount = parse_number(details.get("price"))
    unit_amount = parse_number(details.get("price_per_m2"))
    per_m2 = primary_amount is not None and unit_amount is not None and abs(primary_amount - unit_amount) < 0.0001 and str(details.get("currency", "")).upper() == str(details.get("price_per_m2_currency", "")).upper()
    suffix = "/м²" if per_m2 else ""
    converted = []
    for code in ("UAH", "USD", "EUR"):
        value = prices.get(code) if isinstance(prices, dict) else ""
        if value:
            converted.append(f"{format_amount(value)} {symbols[code]}{suffix}")
    if len(converted) >= 2:
        return " • ".join(converted)
    value = str(details.get("price", "")).strip()
    currency = str(details.get("currency", "")).upper()
    if value and currency:
        return f"{format_amount(value)} {symbols.get(currency, currency)}{suffix}"
    for code in ("USD", "EUR", "UAH"):
        if prices.get(code):
            return f"{format_amount(prices[code])} {symbols[code]}{suffix}"
    return ""


def property_detail_rows(details, language):
    uk = language == "uk"
    labels = {
        "price": "Ціна" if uk else "Price",
        "area": "Загальна площа" if uk else "Total area",
        "floor": "Поверх" if uk else "Floor",
        "total_floors": "Поверховість" if uk else "Total floors",
        "storeys": "Поверховість" if uk else "Total floors",
        "rooms": "Кількість кімнат" if uk else "Rooms",
        "land": "Площа ділянки" if uk else "Land area",
        "address": "Адреса" if uk else "Address",
    }
    area_value = str(details.get("area", "")).strip()
    area_number = parse_number(area_value)
    if area_number is not None and area_number.is_integer():
        area_value = f"{area_number:,.0f}".replace(",", " ")
    area = f"{area_value} м²" if area_value else ""
    rooms = f"{details.get('rooms')} {'кімнати' if uk else 'rooms'}" if details.get("rooms") else ""
    kind = details.get("property_type", "apartment")
    rows = [(labels["area"], area)]
    if kind == "house":
        rows.extend([(labels["rooms"], rooms), (labels["land"], str(details.get("land_area", ""))), (labels["total_floors"], str(details.get("total_floors", "")))])
    elif kind == "commercial":
        rows.append((labels["floor"], str(details.get("floor", ""))))
    else:  # apartment: floor and total floors are apartment-only facts
        floor = str(details.get("floor", ""))
        total_floors = str(details.get("total_floors", ""))
        floor_value = "/".join(value for value in (floor, total_floors) if value)
        rows.extend([(labels["floor"], floor_value), (labels["rooms"], rooms)])
    address = details.get("address" if uk else "address_en") or details.get("address", "")
    rows.append((labels["address"], str(address)))
    return [(label, value) for label, value in rows if value]


def ensure_local_logo():
    target = DATA_ROOT / "assets" / "kyiv-estate-logo.jpg"
    target.parent.mkdir(parents=True, exist_ok=True)
    if LOGO_PATH and LOGO_PATH.is_file():
        if not target.exists() or file_sha256(target) != file_sha256(LOGO_PATH):
            shutil.copy2(LOGO_PATH, target)
        return target
    if target.is_file():
        return target
    if LOGO_URL.startswith("https://") and safe_remote_url(LOGO_URL):
        response = requests.get(LOGO_URL, headers=REQUEST_HEADERS, timeout=20)
        response.raise_for_status()
        if len(response.content) < 1024:
            raise RuntimeError("Логотип KYIV ESTATE пошкоджений або порожній.")
        target.write_bytes(response.content)
        return target
    raise RuntimeError("Налаштуйте KYIV_ESTATE_LOGO_PATH або KYIV_ESTATE_LOGO_URL.")


def telegraph_image(path):
    path = Path(path)
    digest = file_sha256(path)
    cached = None
    try:
        init_storage()
        with database() as db:
            cached = db.execute("SELECT public_url FROM uploaded_assets WHERE sha256=?", (digest,)).fetchone()
    except sqlite3.OperationalError as storage_error:
        if "full" not in str(storage_error).lower():
            raise
    if cached:
        return cached[0]
    with Image.open(path) as source:
        image = ImageOps.exif_transpose(source).convert("RGB")
        if max(image.size) > 2560:
            ratio = 2560 / max(image.size)
            image = image.resize((round(image.width * ratio), round(image.height * ratio)), Image.Resampling.LANCZOS)
        encoded = BytesIO()
        quality = 90
        image.save(encoded, format="JPEG", quality=quality, optimize=True, progressive=True)
        while encoded.tell() > 4_500_000 and quality > 68:
            quality -= 6
            encoded = BytesIO()
            image.save(encoded, format="JPEG", quality=quality, optimize=True, progressive=True)
    response = requests.post(
        "https://telegra.ph/upload",
        files={"file": (path.stem + ".jpg", encoded.getvalue(), "image/jpeg")},
        headers={"User-Agent": "KYIV-ESTATE/1.0"},
        timeout=90,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not payload or not payload[0].get("src"):
        raise RuntimeError("Telegraph не зберіг зображення.")
    public_url = urljoin("https://telegra.ph", str(payload[0]["src"]))
    try:
        with database() as db:
            db.execute(
                "INSERT OR REPLACE INTO uploaded_assets (sha256,local_path,public_url,uploaded_at) VALUES (?,?,?,?)",
                (digest, str(path), public_url, datetime.now(timezone.utc).isoformat()),
            )
    except sqlite3.OperationalError as storage_error:
        if "full" not in str(storage_error).lower():
            raise
    return public_url


def github_api(method, path, **kwargs):
    if not MEDIA_GITHUB_REPO or not GITHUB_TOKEN:
        raise RuntimeError("GitHub media storage is not configured.")
    headers = dict(kwargs.pop("headers", {}))
    headers.update({
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "KYIV-ESTATE/1.0",
    })
    return requests.request(method, f"https://api.github.com/repos/{MEDIA_GITHUB_REPO}{path}", headers=headers, timeout=90, **kwargs)


def ensure_media_branch():
    branch_path = quote(MEDIA_GITHUB_BRANCH, safe="")
    response = github_api("GET", f"/git/ref/heads/{branch_path}")
    if response.status_code == 200:
        return response.json()["object"]["sha"]
    if response.status_code != 404:
        response.raise_for_status()
    repository = github_api("GET", "")
    repository.raise_for_status()
    default_branch = repository.json()["default_branch"]
    base = github_api("GET", f"/git/ref/heads/{quote(default_branch, safe='')}")
    base.raise_for_status()
    base_sha = base.json()["object"]["sha"]
    created = github_api("POST", "/git/refs", json={"ref": f"refs/heads/{MEDIA_GITHUB_BRANCH}", "sha": base_sha})
    if created.status_code not in (201, 422):
        created.raise_for_status()
    if created.status_code == 422:
        existing = github_api("GET", f"/git/ref/heads/{branch_path}")
        existing.raise_for_status()
        return existing.json()["object"]["sha"]
    return created.json()["object"]["sha"]


def github_media_images(paths, folder):
    paths = [Path(path) for path in paths]
    head_sha = ensure_media_branch()
    commit = github_api("GET", f"/git/commits/{head_sha}")
    commit.raise_for_status()
    safe_folder = re.sub(r"[^a-zA-Z0-9_-]", "", str(folder)) or "listing"
    entries, repo_paths = [], []
    for path in paths:
        digest = file_sha256(path)
        suffix = path.suffix.lower() if path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"} else ".jpg"
        repo_path = f"media/{safe_folder}/{digest[:20]}{suffix}"
        blob = github_api("POST", "/git/blobs", json={"content": base64.b64encode(path.read_bytes()).decode("ascii"), "encoding": "base64"})
        blob.raise_for_status()
        entries.append({"path": repo_path, "mode": "100644", "type": "blob", "sha": blob.json()["sha"]})
        repo_paths.append(repo_path)
    tree = github_api("POST", "/git/trees", json={"base_tree": commit.json()["tree"]["sha"], "tree": entries})
    tree.raise_for_status()
    new_commit = github_api("POST", "/git/commits", json={"message": f"Add media for listing {safe_folder}", "tree": tree.json()["sha"], "parents": [head_sha]})
    new_commit.raise_for_status()
    updated = github_api("PATCH", f"/git/refs/heads/{quote(MEDIA_GITHUB_BRANCH, safe='')}", json={"sha": new_commit.json()["sha"], "force": False})
    updated.raise_for_status()
    repo = "/".join(quote(part, safe="") for part in MEDIA_GITHUB_REPO.split("/"))
    branch = quote(MEDIA_GITHUB_BRANCH, safe="")
    return [f"https://raw.githubusercontent.com/{repo}/{branch}/" + "/".join(quote(part, safe="") for part in repo_path.split("/")) for repo_path in repo_paths]


def durable_image_urls(paths, folder):
    paths = [Path(path) for path in paths]
    cached_urls, missing = {}, []
    try:
        init_storage()
        with database() as db:
            for path in paths:
                digest = file_sha256(path)
                row = db.execute("SELECT public_url FROM uploaded_assets WHERE sha256=?", (digest,)).fetchone()
                if row:
                    cached_urls[digest] = row[0]
                else:
                    missing.append(path)
    except sqlite3.OperationalError as storage_error:
        if "full" not in str(storage_error).lower():
            raise
        missing = list(paths)
    if missing:
        if MEDIA_GITHUB_REPO and GITHUB_TOKEN:
            with MEDIA_UPLOAD_LOCK:
                uploaded = github_media_images(missing, folder)
        else:
            uploaded = [telegraph_image(path) for path in missing]
        now = datetime.now(timezone.utc).isoformat()
        for path, public_url in zip(missing, uploaded):
            cached_urls[file_sha256(path)] = public_url
        try:
            with database() as db:
                for path, public_url in zip(missing, uploaded):
                    db.execute(
                        "INSERT OR REPLACE INTO uploaded_assets (sha256,local_path,public_url,uploaded_at) VALUES (?,?,?,?)",
                        (file_sha256(path), str(path), public_url, now),
                    )
        except sqlite3.OperationalError as storage_error:
            if "full" not in str(storage_error).lower():
                raise
    return [cached_urls[file_sha256(path)] for path in paths]


def token():
    global ACCESS_TOKEN
    if ACCESS_TOKEN:
        return ACCESS_TOKEN
    token_file = DATA_ROOT / "telegraph-account.json"
    if token_file.is_file():
        stored = json.loads(token_file.read_text(encoding="utf-8")).get("access_token", "")
        if stored:
            ACCESS_TOKEN = str(stored)
            return ACCESS_TOKEN
    result = requests.post(
        f"{TELEGRAPH_API}/createAccount",
        json={"short_name": "KYIVESTATE", "author_name": "KYIV ESTATE", "author_url": CONTACT_URL if CONTACT_URL.startswith("https://") else ""},
        timeout=15,
    ).json()
    if not result.get("ok"):
        raise RuntimeError("Telegraph не надав токен.")
    ACCESS_TOKEN = result["result"]["access_token"]
    token_file.parent.mkdir(parents=True, exist_ok=True)
    token_file.write_text(json.dumps({"access_token": ACCESS_TOKEN}), encoding="utf-8")
    return ACCESS_TOKEN


def agency_links():
    links = []
    if CONTACT_PHONE:
        links.append(("Phone", "tel:" + re.sub(r"[^0-9+]", "", CONTACT_PHONE)))
    if CONTACT_URL.startswith("https://"):
        links.append(("KYIV ESTATE", CONTACT_URL))
    for label, url in CONTACT_LINKS.items():
        if url.startswith("https://") or (label == "Email" and re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", url)):
            links.append((label, "mailto:" + url if label == "Email" else url))
    return links


def telegraph_content(payload, language, text, images=None, logo_url=None):
    details = payload.get("details", {})
    prices = payload.get("prices", {})
    images = clean_image_urls(images if images is not None else payload.get("images", []))
    price_label, contacts_label = ("Ціна", "Контакти") if language == "uk" else ("Price", "Contacts")
    content = []
    if images:
        content.append({"tag": "img", "attrs": {"src": images[0]}})
    logo_url = logo_url or (LOGO_URL if LOGO_URL.startswith("https://") else "")
    if logo_url:
        content.append({"tag": "img", "attrs": {"src": logo_url}})
    if CONTACT_PHONE:
        phone_digits = re.sub(r"[^0-9]", "", CONTACT_PHONE)
        phone_url = CONTACT_LINKS.get("WhatsApp") or CONTACT_URL or f"https://wa.me/{phone_digits}"
        content.append({"tag": "p", "children": [
            {"tag": "b", "children": ["Контакти: " if language == "uk" else "Contacts: "]},
            {"tag": "a", "attrs": {"href": phone_url}, "children": [CONTACT_PHONE]},
        ]})
    price = display_price(details, prices)
    if price:
        content.append({"tag": "p", "children": [{"tag": "b", "children": [f"{price_label}: "]}, price]})
    for label, value in property_detail_rows(details, language):
        if value:
            content.append({"tag": "p", "children": [{"tag": "b", "children": [f"{label}: "]}, str(value)]})
    public_text = sanitize_public_text(text)
    if public_text:
        content.append({"tag": "h3", "children": ["📋 Опис" if language == "uk" else "📋 Description"]})
    for paragraph in public_text.splitlines():
        if paragraph.strip():
            content.append({"tag": "p", "children": [paragraph.strip()]})
    if len(images) > 1:
        content.append({"tag": "h3", "children": ["📸 Фотографії" if language == "uk" else "📸 Photos"]})
    for image in images[1:]:
        content.append({"tag": "img", "attrs": {"src": image}})
    contacts = []
    for label, url in agency_links():
        if label == "Phone":
            continue
        if contacts:
            contacts.append(" · ")
        display = CONTACT_PHONE if label == "Phone" else label
        contacts.append({"tag": "a", "attrs": {"href": url}, "children": [display]})
    if contacts:
        content.append({"tag": "p", "children": [{"tag": "b", "children": [f"{contacts_label}: "]}, *contacts]})
    slogan = "🏛 Kyiv.Estate — Агентство нерухомості №1 в Києві." if language == "uk" else "🏛 Kyiv.Estate — Kyiv’s No. 1 Real Estate Agency."
    content.append({"tag": "p", "children": [{"tag": "b", "children": [slogan]}]})
    return content


def publish_page(title, content):
    payload = {"access_token": token(), "title": title[:256], "author_name": "KYIV ESTATE", "author_url": CONTACT_URL if CONTACT_URL.startswith("https://") else "", "content": json.dumps(content, ensure_ascii=False), "return_content": False}
    last_error = ""
    for attempt in range(3):
        try:
            response = requests.post(f"{TELEGRAPH_API}/createPage", json=payload, timeout=35)
            result = response.json()
            if result.get("ok"):
                return result["result"]["url"]
            last_error = str(result.get("error", response.text[:300]))
        except (requests.RequestException, ValueError) as error:
            last_error = str(error)
        if attempt < 2:
            time.sleep(1.5 * (2 ** attempt))
    raise RuntimeError(last_error or "Telegraph could not create the page.")


def edit_page(page_url, title, content):
    page_path = urlparse(page_url).path.strip("/")
    if not page_path:
        raise ValueError("Invalid Telegraph page URL.")
    last_error = ""
    for attempt in range(3):
        try:
            response = requests.post(
                f"{TELEGRAPH_API}/editPage/{page_path}",
                json={"access_token": token(), "title": title[:256], "author_name": "KYIV ESTATE", "author_url": CONTACT_URL if CONTACT_URL.startswith("https://") else "", "content": json.dumps(content, ensure_ascii=False), "return_content": False},
                timeout=35,
            )
            result = response.json()
            if result.get("ok"):
                return result["result"]["url"]
            last_error = str(result.get("error", response.text[:300]))
        except (requests.RequestException, ValueError) as error:
            last_error = str(error)
        if attempt < 2:
            time.sleep(1.5 * (2 ** attempt))
    raise RuntimeError(last_error or "Telegraph could not update the page.")


def publication_identity(payload):
    source = canonical_listing_url(payload.get("source", ""))
    if not source.startswith("https://"):
        raise ValueError("A source listing URL is required for Telegraph.")
    return external_listing_id(source), listing_id(source), source


def publication_state(identity_key, internal_id="", source_url=""):
    init_storage()
    with database() as db:
        row = db.execute(
            "SELECT internal_id,source_url,uk_url,en_url,uk_hash,en_hash FROM publication_state WHERE identity_key=?",
            (identity_key,),
        ).fetchone()
        if row:
            return dict(zip(("internal_id", "source_url", "uk_url", "en_url", "uk_hash", "en_hash"), row))
        job = db.execute(
            "SELECT uk_url,en_url FROM jobs WHERE id=? OR input_value=? ORDER BY updated_at DESC LIMIT 1",
            (internal_id, source_url),
        ).fetchone() if internal_id else None
    state = {"internal_id": internal_id, "source_url": source_url, "uk_url": job[0] if job else "", "en_url": job[1] if job else "", "uk_hash": "", "en_hash": ""}
    if not state["uk_url"] and not state["en_url"] and PACKAGES_ROOT.is_dir():
        for manifest_path in PACKAGES_ROOT.glob("*/manifest.json"):
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest_source = str(manifest.get("source", ""))
                if not manifest_source or external_listing_id(manifest_source) != identity_key:
                    continue
                previous = manifest.get("telegraph", {})
                for language in ("uk", "en"):
                    value = str(previous.get(language, ""))
                    if value.startswith("https://telegra.ph/"):
                        state[language + "_url"] = value
                break
            except (OSError, ValueError, json.JSONDecodeError):
                continue
    return state


def save_publication_state(identity_key, state):
    with database() as db:
        db.execute("""
            INSERT INTO publication_state(identity_key,internal_id,source_url,uk_url,en_url,uk_hash,en_hash)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(identity_key) DO UPDATE SET
                internal_id=excluded.internal_id, source_url=excluded.source_url,
                uk_url=excluded.uk_url, en_url=excluded.en_url,
                uk_hash=excluded.uk_hash, en_hash=excluded.en_hash,
                updated_at=CURRENT_TIMESTAMP
        """, (identity_key, state["internal_id"], state["source_url"], state.get("uk_url", ""), state.get("en_url", ""), state.get("uk_hash", ""), state.get("en_hash", "")))


def seeded_telegraph_url(payload, language):
    candidates = payload.get("telegraph_urls", {}) if isinstance(payload.get("telegraph_urls"), dict) else {}
    key = "telegraph_url_en" if language == "en" else "telegraph_url"
    value = candidates.get(language) or payload.get(key) or payload.get("details", {}).get(key)
    value = str(value or "").strip()
    return value if value.startswith("https://telegra.ph/") else ""


def publication_content_hash(title, content):
    value = json.dumps({"title": title, "content": content}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(value.encode()).hexdigest()


def _publish_single_language_unlocked(payload, language):
    if language not in {"uk", "en"}:
        raise ValueError("Unsupported publication language.")
    translations = payload.get("translations", {})
    version = translations.get(language, {})
    if not str(version.get("title", "")).strip() or not str(version.get("text", "")).strip():
        raise ValueError("The requested language version is not ready.")
    identity_key, job_id, source = publication_identity(payload)
    image_urls = clean_image_urls(payload.get("images", []))
    if not image_urls:
        raise ValueError("At least one direct source photograph is required for Telegraph.")
    state = publication_state(identity_key, job_id, source)
    state.update({"internal_id": job_id, "source_url": source})
    for item in ("uk", "en"):
        if not str(state.get(item + "_url", "")).startswith("https://telegra.ph/"):
            state[item + "_url"] = seeded_telegraph_url(payload, item)
    update_job(job_id, source, "publishing", 90)
    show_title = bool(payload.get("include_title"))
    logo_url = LOGO_URL if LOGO_URL.startswith("https://") else BUILTIN_LOGO_URL

    def render(item):
        item_version = translations.get(item, {})
        item_title = f"{job_id} · {item_version.get('title', '')}" if show_title else job_id
        item_content = telegraph_content(payload, item, str(item_version.get("text", "")), image_urls, logo_url)
        other = "en" if item == "uk" else "uk"
        other_url = str(state.get(other + "_url", ""))
        if other_url.startswith("https://telegra.ph/"):
            label = "🌐 English" if item == "uk" else "🌐 Українська"
            item_content.insert(0, {"tag": "p", "children": [{"tag": "a", "attrs": {"href": other_url}, "children": [label]}]})
        return item_title, item_content, publication_content_hash(item_title, item_content)

    title, content, digest = render(language)
    previous_url = str(state.get(language + "_url", ""))
    unchanged = previous_url.startswith("https://telegra.ph/") and state.get(language + "_hash") == digest
    if unchanged:
        page_url = previous_url
    elif previous_url.startswith("https://telegra.ph/"):
        page_url = edit_page(previous_url, title, content)
    else:
        page_url = publish_page(title, content)
    state[language + "_url"] = page_url
    state[language + "_hash"] = digest
    save_publication_state(identity_key, state)

    other = "en" if language == "uk" else "uk"
    other_version = translations.get(other, {})
    other_url = str(state.get(other + "_url", ""))
    if other_url.startswith("https://telegra.ph/") and str(other_version.get("title", "")).strip() and str(other_version.get("text", "")).strip():
        other_title, other_content, other_digest = render(other)
        if state.get(other + "_hash") != other_digest:
            edit_page(other_url, other_title, other_content)
            state[other + "_hash"] = other_digest
            save_publication_state(identity_key, state)

    update_job(job_id, source, "published", 100, uk_url=state.get("uk_url") or None, en_url=state.get("en_url") or None)
    payload["internal_id"] = job_id
    payload["external_id"] = identity_key
    payload["source"] = source
    payload["telegraph_urls"] = {"uk": state.get("uk_url", ""), "en": state.get("en_url", "")}
    sync_sheet_record(payload, "telegraph")
    return {"language": language, "url": page_url, "urls": payload["telegraph_urls"], "unchanged": unchanged}


def publish_single_language(payload, language):
    with PUBLICATION_LOCK:
        return _publish_single_language_unlocked(payload, language)


def publish_bilingual(payload):
    translations = payload.get("translations", {})
    if any(not translations.get(item, {}).get(field) for item in ("uk", "en") for field in ("title", "text")):
        raise ValueError("Потрібні готові українська й англійська версії.")
    publish_single_language(payload, "uk")
    result = publish_single_language(payload, "en")
    return result["urls"]


def ai_package_photos(payload):
    if not AI_ENDPOINT:
        if AI_BRIDGE_ENABLED:
            return bridge_ai_package_photos(payload)
        if AI_REQUIRED:
            raise RuntimeError("AI-обробка обов’язкова, але KYIV_ESTATE_AI_ENDPOINT не налаштований.")
        return []
    endpoint = urlparse(AI_ENDPOINT)
    if endpoint.hostname in {"127.0.0.1", "localhost"} and endpoint.port == PORT:
        raise RuntimeError("AI endpoint не може використовувати той самий порт, що й вебзастосунок.")
    request_payload = {
        "mode": AI_MODE,
        "value": payload.get("internal_id") or payload.get("source"),
        "url": payload.get("source"),
        "title": payload.get("translations", {}).get("uk", {}).get("title", ""),
        "description": payload.get("translations", {}).get("uk", {}).get("text", ""),
        "photo_urls": payload.get("images", []),
    }
    headers = {"X-Block3-Token": AI_TOKEN} if AI_TOKEN else {}
    response = requests.post(f"{AI_ENDPOINT}/api/v1/jobs", json=request_payload, headers=headers, timeout=20)
    response.raise_for_status()
    job = response.json()
    deadline = time.time() + AI_TIMEOUT_SECONDS
    while time.time() < deadline:
        try:
            status = requests.get(f"{AI_ENDPOINT}/api/v1/jobs/{job['job_id']}", headers=headers, timeout=20)
            status.raise_for_status()
            job = status.json()
        except requests.RequestException:
            time.sleep(3)
            continue
        if job.get("status") in {"ready", "published"}:
            break
        if job.get("status") == "failed":
            if AI_PACKAGES_ROOT and job.get("internal_id"):
                fallback_root = AI_PACKAGES_ROOT / str(job["internal_id"]) / "photos"
                fallback = sorted(path for path in fallback_root.glob("*") if path.is_file())
                if fallback:
                    return fallback
            raise RuntimeError("AI-обробка фото зупинилася: " + str(job.get("error") or "невідома помилка"))
        time.sleep(3)
    else:
        raise RuntimeError("Перевищено час очікування AI-обробки фотографій.")
    photos = []
    internal_id = str(job.get("internal_id") or "").strip()
    if AI_PACKAGES_ROOT and internal_id:
        photos_root = AI_PACKAGES_ROOT / internal_id / "photos"
        photos = sorted(path for path in photos_root.glob("*") if path.is_file())
    if not photos and internal_id:
        photos = download_remote_ai_photos(internal_id, int(job.get("certified_photos") or 0), headers)
    if not photos:
        raise RuntimeError("AI-конвеєр не повернув сертифікованих фотографій.")
    return photos


def bridge_ai_package_photos(payload):
    init_storage()
    bridge_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc).isoformat()
    request_payload = {
        "mode": "browser", "value": payload.get("internal_id") or payload.get("source"),
        "url": payload.get("source"), "photo_urls": payload.get("images", []),
        "title": payload.get("translations", {}).get("uk", {}).get("title", ""),
        "description": payload.get("translations", {}).get("uk", {}).get("text", ""),
    }
    with database() as db:
        db.execute("INSERT INTO ai_bridge_jobs(id,payload_json,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                   (bridge_id, json.dumps(request_payload, ensure_ascii=False), "queued", now, now))
    deadline = time.time() + AI_TIMEOUT_SECONDS
    while time.time() < deadline:
        with database() as db:
            row = db.execute("SELECT status,error FROM ai_bridge_jobs WHERE id=?", (bridge_id,)).fetchone()
        if row and row[0] == "ready":
            root = DATA_ROOT / "bridge-uploads" / bridge_id
            photos = sorted(path for path in root.glob("*") if path.is_file())
            if photos:
                return photos
            raise RuntimeError("Windows AI completed without certified photos.")
        if row and row[0] == "failed":
            raise RuntimeError("Windows AI processing failed: " + str(row[1] or "unknown error"))
        time.sleep(2)
    raise RuntimeError("Windows AI processing timed out.")


def bridge_authorized(environ):
    return bool(AI_TOKEN and hmac.compare_digest(environ.get("HTTP_X_BLOCK3_TOKEN", ""), AI_TOKEN))


def bridge_reply_job(start_response):
    init_storage()
    now = datetime.now(timezone.utc).isoformat()
    with database() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT id,payload_json FROM ai_bridge_jobs WHERE status='queued' ORDER BY created_at LIMIT 1").fetchone()
        if row:
            db.execute("UPDATE ai_bridge_jobs SET status='claimed',updated_at=? WHERE id=?", (now, row[0]))
    return reply(start_response, "200 OK", {"job_id": row[0], "payload": json.loads(row[1])} if row else {"job_id": None})


def download_remote_ai_photos(internal_id, count, headers=None):
    safe_id = re.sub(r"[^A-Za-z0-9_-]", "", str(internal_id))
    if not safe_id or count < 1 or count > MAX_AI_PHOTOS:
        return []
    destination = DATA_ROOT / "remote-ai" / safe_id
    destination.mkdir(parents=True, exist_ok=True)
    downloaded = []
    for index in range(1, count + 1):
        found = None
        for extension in ("jpg", "jpeg", "png", "webp"):
            url = f"{AI_ENDPOINT}/packages/{quote(safe_id, safe='')}/photos/{index:02d}.{extension}"
            response = requests.get(url, headers=headers or {}, timeout=60)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "").lower()
            if not content_type.startswith("image/") or not 1024 <= len(response.content) <= 30 * 1024 * 1024:
                raise RuntimeError("Windows AI returned an invalid certified photo.")
            found = destination / f"{index:02d}.{extension}"
            found.write_bytes(response.content)
            break
        if not found:
            raise RuntimeError(f"Windows AI package is incomplete: photo {index:02d} is missing.")
        downloaded.append(found)
    return downloaded


def existing_approved_photos(job_id, image_urls, processing_mode="ai"):
    manifest = PACKAGES_ROOT / job_id / "manifest.json"
    if not manifest.is_file():
        return []
    record = json.loads(manifest.read_text(encoding="utf-8"))
    if record.get("selected_source_urls") != list(image_urls[:MAX_AI_PHOTOS]):
        return []
    if record.get("processing_mode", "ai") != processing_mode:
        return []
    if processing_mode == "ai" and (AI_ENDPOINT or AI_BRIDGE_ENABLED) and record.get("ai_processing", {}).get("result") != "ai_clean":
        return []
    result = []
    for item in record.get("photos", []):
        final_path = DATA_ROOT / item["final_path"]
        original_path = DATA_ROOT / item["original_path"] if item.get("original_path") else None
        if not final_path.is_file():
            return []
        if SOURCE_LISTINGS_ROOT and (not original_path or not original_path.is_file()):
            return []
        result.append({**item, "final_path": str(final_path), "original_path": str(original_path) if original_path else ""})
    return result


def save_approved_photos(job_id, image_urls, payload=None):
    image_urls = clean_image_urls(image_urls)
    listing_root = DATA_ROOT / "listings" / job_id
    original_root = listing_root / "original"
    final_root = listing_root / "final"
    original_root.mkdir(parents=True, exist_ok=True)
    final_root.mkdir(parents=True, exist_ok=True)
    processing_mode = str((payload or {}).get("processing_mode") or "ai").lower()
    cached = [] if (payload or {}).get("force_ai") else existing_approved_photos(job_id, image_urls, processing_mode)
    if cached:
        return cached
    saved = []
    saved_hashes = set()
    saved_visual_hashes = []
    for url in image_urls:
        if not safe_remote_url(url):
            continue
        try:
            response = requests.get(url, headers=REQUEST_HEADERS, timeout=15)
            response.raise_for_status()
            if not safe_remote_url(response.url):
                continue
            content_type = response.headers.get("Content-Type", "").lower()
            if not content_type.startswith("image/") or len(response.content) < 1024 or len(response.content) > 30 * 1024 * 1024:
                continue
            extension = ".png" if "png" in content_type else ".webp" if "webp" in content_type else ".jpg"
            digest = hashlib.sha256(response.content).hexdigest()
            if digest in saved_hashes:
                continue
            temporary_path = original_root / f".fingerprint-{uuid.uuid4().hex}{extension}"
            temporary_path.write_bytes(response.content)
            try:
                if same_visual_photo(temporary_path, saved_visual_hashes):
                    continue
                saved_visual_hashes.append(visual_fingerprint(temporary_path))
            finally:
                temporary_path.unlink(missing_ok=True)
            saved_hashes.add(digest)
            index = len(saved) + 1
            name = f"{index:02d}{extension}"
            original_path = original_root / name
            final_path = final_root / name
            if not original_path.exists():
                original_path.write_bytes(response.content)
            shutil.copy2(original_path, final_path)
            saved.append({
                "order": index,
                "source_url": url,
                "selected": "original",
                "approved": True,
                "original_path": str(original_path),
                "final_path": str(final_path),
                "filename": name,
                "sha256": digest,
            })
        except requests.RequestException:
            continue
    if payload and processing_mode == "ai" and (AI_ENDPOINT or AI_BRIDGE_ENABLED):
        processed = ai_package_photos(payload)
        if not saved and SOURCE_LISTINGS_ROOT and SOURCE_LISTINGS_ROOT.is_dir():
            candidates = []
            for provider_root in SOURCE_LISTINGS_ROOT.iterdir():
                candidate = provider_root / job_id / "original"
                if candidate.is_dir():
                    candidates = sorted(path for path in candidate.iterdir() if path.is_file())
                    if candidates:
                        break
            source_hashes = set()
            source_visual_hashes = []
            for source_path in candidates[:MAX_AI_PHOTOS]:
                digest = file_sha256(source_path)
                if digest in source_hashes:
                    continue
                if same_visual_photo(source_path, source_visual_hashes):
                    continue
                source_hashes.add(digest)
                source_visual_hashes.append(visual_fingerprint(source_path))
                index = len(saved) + 1
                extension = source_path.suffix.lower() if source_path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"} else ".jpg"
                name = f"{index:02d}{extension}"
                original_path = original_root / name
                shutil.copy2(source_path, original_path)
                saved.append({
                    "order": index,
                    "source_url": image_urls[index - 1] if index <= len(image_urls) else "",
                    "original_path": str(original_path),
                })
        ai_saved = []
        ai_hashes = set()
        ai_visual_hashes = []
        for source_path in processed:
            digest = file_sha256(source_path)
            if digest in ai_hashes:
                continue
            if same_visual_photo(source_path, ai_visual_hashes):
                continue
            ai_hashes.add(digest)
            ai_visual_hashes.append(visual_fingerprint(source_path))
            index = len(ai_saved) + 1
            extension = source_path.suffix.lower() if source_path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"} else ".jpg"
            name = f"{index:02d}{extension}"
            final_path = final_root / name
            shutil.copy2(source_path, final_path)
            original = saved[index - 1] if index <= len(saved) else None
            ai_saved.append({
                "order": index,
                "source_url": original["source_url"] if original else "",
                "selected": "ai_clean",
                "approved": True,
                "ai_processed": True,
                "original_path": original["original_path"] if original else "",
                "final_path": str(final_path),
                "filename": name,
                "sha256": digest,
            })
        saved = ai_saved
    elif payload and processing_mode == "ai" and AI_REQUIRED:
        raise RuntimeError("AI-обробка обов’язкова, але локальний AI endpoint недоступний.")
    return saved


def render_package_page(language, record, photos):
    translations = record["translations"]
    version = translations[language]
    labels = {"uk": ("Ціна", "English", "Контакти"), "en": ("Price", "Українська", "Contacts")}
    price_label, other_label, contacts_label = labels[language]
    other_file = "en.html" if language == "uk" else "uk.html"
    prices = html.escape(display_price(record["details"], record["prices"]))
    detail_html = "".join(f'<p><b>{html.escape(label)}:</b> {html.escape(str(value))}</p>' for label, value in property_detail_rows(record["details"], language))
    image_tags = [f'<img src="photos/{html.escape(item["filename"])}" alt="KYIV ESTATE property photo {item["order"]}">' for item in photos]
    main_image = image_tags[0] if image_tags else ""
    remaining_images = "".join(image_tags[1:])
    logo = '<img class="logo" src="assets/kyiv-estate-logo.jpg" alt="KYIV ESTATE">' if (PACKAGES_ROOT / record["internal_id"] / "assets" / "kyiv-estate-logo.jpg").exists() else '<div class="brand">KYIV ESTATE</div>'
    contact_parts = []
    for label, url in agency_links():
        display = CONTACT_PHONE if label == "Phone" else label
        contact_parts.append(f'<a href="{html.escape(url, quote=True)}">{html.escape(display)}</a>')
    contacts = " · ".join(contact_parts)
    slogan = "🏛 Kyiv.Estate — Агентство нерухомості №1 в Києві." if language == "uk" else "🏛 Kyiv.Estate — Kyiv’s No. 1 Real Estate Agency."
    return f'''<!doctype html><html lang="{language}"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{html.escape(version["title"])}</title><style>*{{box-sizing:border-box}}body{{max-width:760px;margin:0 auto;padding:42px 22px;color:#111;background:#fff;font-family:-apple-system,BlinkMacSystemFont,"SF Pro Display","SF Pro Text","Helvetica Neue",Arial,sans-serif;line-height:1.55}}h1{{font-size:34px;line-height:1.15}}nav{{margin:14px 0 28px}}a{{color:#111}}img{{display:block;width:100%;height:auto;margin:22px 0}}.logo{{max-width:220px;margin:22px auto}}.brand{{margin:22px 0;padding:24px;text-align:center;border:1px solid #ddd;font-weight:700;letter-spacing:.16em}}.facts{{border-top:1px solid #ddd;border-bottom:1px solid #ddd;padding:16px 0;margin:24px 0}}.facts p{{margin:6px 0}}.description{{white-space:pre-line}}footer{{margin-top:32px;padding-top:18px;border-top:1px solid #ddd}}</style><body><h1>{html.escape(record["internal_id"])} · {html.escape(version["title"])}</h1><nav><a href="{other_file}">{other_label}</a></nav>{main_image}{logo}<section class="facts"><p><b>{price_label}:</b> {prices}</p>{detail_html}</section><div class="description">{html.escape(sanitize_public_text(version["text"]))}</div>{remaining_images}<footer><b>{contacts_label}:</b> {contacts}<p><b>{html.escape(slogan)}</b></p></footer></body></html>'''


def repair_existing_packages_once():
    marker = DATA_ROOT / ".package-repair-v3-visual.complete"
    if marker.exists():
        return
    for package_root in PACKAGES_ROOT.iterdir() if PACKAGES_ROOT.exists() else []:
        manifest_path = package_root / "manifest.json"
        if not manifest_path.is_file():
            continue
        try:
            record = json.loads(manifest_path.read_text(encoding="utf-8"))
            unique, seen, visual_hashes = [], set(), []
            for item in record.get("photos", []):
                image_path = package_root / "photos" / str(item.get("filename", ""))
                if not image_path.is_file():
                    continue
                digest = file_sha256(image_path)
                if digest in seen:
                    image_path.unlink()
                    continue
                if same_visual_photo(image_path, visual_hashes):
                    image_path.unlink()
                    continue
                seen.add(digest)
                visual_hashes.append(visual_fingerprint(image_path))
                unique.append({**item, "order": len(unique) + 1, "sha256": digest})
            if unique:
                record["photos"] = unique
                manifest_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
                (package_root / "uk.html").write_text(render_package_page("uk", record, unique), encoding="utf-8")
                (package_root / "en.html").write_text(render_package_page("en", record, unique), encoding="utf-8")
        except (OSError, ValueError, KeyError, TypeError):
            continue
    marker.write_text(datetime.now(timezone.utc).isoformat(), encoding="utf-8")


def create_package(payload):
    translations = payload.get("translations", {})
    if not translations.get("uk", {}).get("text") or not translations.get("en", {}).get("text"):
        raise ValueError("Для пакета потрібні українська й англійська версії.")
    payload = dict(payload)
    payload["images"] = clean_image_urls(payload.get("images", []))
    job_id = re.sub(r"[^a-zA-Z0-9_-]", "", str(payload.get("internal_id", ""))) or hashlib.sha256(str(payload.get("source", "")).encode()).hexdigest()[:12]
    reclaim_package_space(job_id)
    package_root = PACKAGES_ROOT / job_id
    existing_manifest = package_root / "manifest.json"
    previous_record = {}
    if existing_manifest.is_file():
        try:
            previous_record = json.loads(existing_manifest.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as manifest_error:
            print(f"Discarding incomplete package {job_id}: {manifest_error}", flush=True)
            shutil.rmtree(package_root, ignore_errors=True)
    photos_root = package_root / "photos"
    originals_root = package_root / "originals"
    assets_root = package_root / "assets"
    photos_root.mkdir(parents=True, exist_ok=True)
    originals_root.mkdir(parents=True, exist_ok=True)
    assets_root.mkdir(parents=True, exist_ok=True)
    approved = save_approved_photos(job_id, payload["images"], payload)
    requested_choices = payload.get("media_choices") if str(payload.get("processing_mode") or "ai").lower() == "ai" else None
    if requested_choices is not None:
        requested_order = [int(item["order"]) for item in requested_choices if str(item.get("order", "")).isdigit()]
        approved_by_order = {int(item["order"]): item for item in approved}
        approved = [approved_by_order[order] for order in requested_order if order in approved_by_order]
    if not approved:
        raise ValueError("Жодну фотографію не вдалося зберегти й перевірити.")
    for media_root in (photos_root, originals_root):
        for stale_file in media_root.iterdir():
            if stale_file.is_file():
                stale_file.unlink()
    media_choices = {int(item.get("order")): item.get("kind") for item in (payload.get("media_choices") or []) if str(item.get("order", "")).isdigit()}
    for item in approved:
        choice = media_choices.get(int(item["order"]), "processed")
        selected_path = item.get("original_path") if choice == "original" and item.get("original_path") else item["final_path"]
        shutil.copy2(selected_path, photos_root / item["filename"])
        if item.get("original_path"):
            shutil.copy2(item["original_path"], originals_root / item["filename"])
        item["package_choice"] = "original" if selected_path == item.get("original_path") else "processed"
    shutil.copy2(ensure_local_logo(), assets_root / "kyiv-estate-logo.jpg")
    manifest_photos = []
    for item in approved:
        original_name = Path(item["original_path"]).name if item.get("original_path") else ""
        manifest_photos.append({
            **{key: value for key, value in item.items() if key not in {"original_path", "final_path"}},
            "original_path": f"listings/{job_id}/original/{original_name}" if original_name else "",
            "final_path": f"listings/{job_id}/final/{item['filename']}",
        })
    record = {
        "internal_id": job_id,
        "source": payload.get("source", ""),
        "translations": translations,
        "details": payload.get("details", {}),
        "prices": payload.get("prices", {}),
        "photos": manifest_photos,
        "selected_source_urls": list(payload["images"]),
        "processing_mode": str(payload.get("processing_mode") or "ai").lower(),
        "ai_processing": {
            "enabled": bool(AI_ENDPOINT or AI_BRIDGE_ENABLED) and str(payload.get("processing_mode") or "ai").lower() == "ai",
            "required": AI_REQUIRED and str(payload.get("processing_mode") or "ai").lower() == "ai",
            "result": "ai_clean" if any(item.get("ai_processed") for item in approved) else "original_verified",
        },
        "languages": ["uk", "en"],
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if previous_record.get("telegraph"):
        record["telegraph"] = previous_record["telegraph"]
    (package_root / "manifest.json").write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    (package_root / "uk.html").write_text(render_package_page("uk", record, approved), encoding="utf-8")
    (package_root / "en.html").write_text(render_package_page("en", record, approved), encoding="utf-8")
    (package_root / "index.html").write_text('<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="0; url=uk.html">', encoding="utf-8")
    update_job(job_id, str(payload.get("source", "")), "ready", 100, snapshot=record, package_path=str(package_root))
    return {
        "internal_id": job_id,
        "uk": f"/packages/{job_id}/uk.html",
        "en": f"/packages/{job_id}/en.html",
        "manifest": f"/packages/{job_id}/manifest.json",
        "photo_count": len(approved),
        "processed": [f"/packages/{job_id}/photos/{item['filename']}" for item in approved],
        "originals": [f"/packages/{job_id}/originals/{item['filename']}" if item.get("original_path") else "" for item in approved],
    }


def sheet_tab_for(payload):
    details = payload.get("details", {})
    property_type = str(details.get("property_type") or "apartment").lower()
    text = " ".join(str(payload.get(key, "")) for key in ("source", "title", "text")).lower()
    operation = "rent" if any(marker in text for marker in ("rent", "оренд", "здає", "arenda")) else "sale"
    prefix = "commercial_listings" if property_type == "commercial" else "houses_listings" if property_type == "house" else "active_listings"
    return f"{prefix}_{operation}", operation


def sync_sheet_record(payload, event, pdf_language=""):
    try:
        identity_key, stable_job_id, canonical_source = publication_identity(payload)
    except ValueError:
        return
    job_id = stable_job_id
    package_root = PACKAGES_ROOT / job_id
    manifest_path = package_root / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.is_file() else {}
    except (OSError, json.JSONDecodeError):
        manifest = {}
    tab, operation = sheet_tab_for(payload)
    details, prices = payload.get("details", {}), payload.get("prices", {})
    translations = payload.get("translations", {})
    state = publication_state(identity_key, job_id, canonical_source)
    telegraph = payload.get("telegraph_urls", {}) if isinstance(payload.get("telegraph_urls"), dict) else {}
    telegraph = {"uk": telegraph.get("uk") or state.get("uk_url", ""), "en": telegraph.get("en") or state.get("en_url", "")}
    pdf_root = package_root / "pdf"
    row = {
        "id": job_id, "external_id": identity_key,
        "source": "OLX" if "olx.ua" in canonical_source else "RIELTOR" if "rieltor.ua" in canonical_source else "",
        "operation": operation,
        "property_type": details.get("property_type", "apartment"), "title": translations.get("uk", {}).get("title", payload.get("title", "")),
        "description": payload.get("text", ""), "ai_description": translations.get("uk", {}).get("text", ""),
        "price_uah": prices.get("UAH", ""), "price_usd": prices.get("USD", ""), "price_eur": prices.get("EUR", ""),
        "area": details.get("area", ""), "floor": details.get("floor", ""), "floors_total": details.get("total_floors", ""),
        "rooms": details.get("rooms", ""), "district": details.get("district", ""), "city": details.get("city", ""),
        "street": details.get("address", ""), "residential_complex": details.get("residential_complex", ""),
        "metro_station": details.get("metro_station", ""), "url": canonical_source,
        "photo_url": (payload.get("images") or [""])[0],
        "photo_urls_json": json.dumps(clean_image_urls(payload.get("images", [])), ensure_ascii=False),
        "photo_count": len(clean_image_urls(payload.get("images", []))),
        "agent_type": details.get("agent_type", ""), "agent_name": details.get("agent_name", ""), "agent_phone": details.get("agent_phone", ""),
        "advertiser_type": details.get("agent_type", ""), "contact_name": details.get("agent_name", ""), "phones": details.get("agent_phone", ""),
        "telegraph_url": telegraph.get("uk", ""), "telegraph_url_en": telegraph.get("en", ""),
        "pdf_url": f"{PUBLIC_BASE_URL}/packages/{job_id}/pdf/uk.pdf" if (pdf_root / "uk.pdf").is_file() and PUBLIC_BASE_URL else "",
        "pdf_url_en": f"{PUBLIC_BASE_URL}/packages/{job_id}/pdf/en.pdf" if (pdf_root / "en.pdf").is_file() and PUBLIC_BASE_URL else "",
        "pdf_path": str(pdf_root / "uk.pdf") if (pdf_root / "uk.pdf").is_file() else "",
        "pdf_path_en": str(pdf_root / "en.pdf") if (pdf_root / "en.pdf").is_file() else "",
        "pdf_generated_at": datetime.now(timezone.utc).isoformat() if pdf_language else "",
        "status": "published" if telegraph.get("uk") or telegraph.get("en") else "pdf_generated",
        "created_at": manifest.get("created_at", ""), "updated_at": datetime.now(timezone.utc).isoformat(),
        "parsed_at": manifest.get("created_at", ""), "ai_title": translations.get("uk", {}).get("title", ""),
        "ai_description": translations.get("uk", {}).get("text", ""),
    }
    parsed_fields = (
        "commission", "agent_type", "agent_name", "agent_phone", "commercial_type", "commercial_subtype",
        "price_amount", "price_currency", "price_period", "price_per_m2", "price_per_m2_period",
        "source_price_raw", "vat_included", "opex_amount", "utilities_included", "deposit_months",
        "rent_free_months", "minimum_lease_months", "indexation_terms", "sale_with_tenant", "tenant_name",
        "cap_rate", "area_total_m2", "area_usable_m2", "floor_label", "ceiling_height_m", "layout_type",
        "condition", "fitout", "building_class", "year_built", "separate_entrance", "facade", "showcase_windows",
        "parking_spaces", "loading_dock", "ramp", "freight_elevator", "electric_power_kw", "electricity_backup",
        "generator", "water_supply", "sewerage", "heating_type", "ventilation", "air_conditioning",
        "fire_safety", "security", "internet", "shelter_distance_m", "permitted_use", "full_address",
        "address_confidence", "latitude", "longitude", "advertiser_type", "contact_name", "agency_name",
        "phones", "commission_text", "contact_visibility", "extraction_confidence", "validation_errors",
        "raw_listing_id", "published_at", "last_seen_at", "price_per_m2_uah", "price_per_m2_usd",
        "price_per_m2_eur", "price_per_m2_currency", "data_completeness", "ai_quality_score",
    )
    for field in parsed_fields:
        row[field] = details.get(field, payload.get(field, ""))
    envelope = {
        "spreadsheet_id": "1UmvU7YjDgBpsTBEH4TLIcuVwvkeHzrle6_B6eXUDTU0",
        "sheet": tab,
        "key": identity_key,
        "event": event,
        "write_policy": "preserve_manual_and_nonempty",
        "preserve_fields": ["Коментарі", "comments", "admin_notes"],
        "row": row,
    }
    SHEETS_OUTBOX_ROOT.mkdir(parents=True, exist_ok=True)
    outbox_file = SHEETS_OUTBOX_ROOT / f"{job_id}.json"
    outbox_file.write_text(json.dumps(envelope, ensure_ascii=False, indent=2), encoding="utf-8")
    if SHEETS_WEBHOOK_URL:
        try:
            headers = {"Content-Type": "application/json"}
            if SHEETS_WEBHOOK_SECRET:
                headers["X-Kyiv-Estate-Signature"] = SHEETS_WEBHOOK_SECRET
            request_envelope = {**envelope, "secret": SHEETS_WEBHOOK_SECRET} if SHEETS_WEBHOOK_SECRET else envelope
            last_error = None
            for attempt in range(3):
                try:
                    response = requests.post(SHEETS_WEBHOOK_URL, json=request_envelope, headers=headers, timeout=12)
                    response.raise_for_status()
                    if not response.json().get("ok"):
                        raise RuntimeError("Google Sheets rejected the publication record")
                    last_error = None
                    break
                except (requests.RequestException, RuntimeError, ValueError) as error:
                    last_error = error
                    if attempt < 2:
                        time.sleep(1.5 * (2 ** attempt))
            if last_error:
                raise last_error
        except (requests.RequestException, RuntimeError, ValueError) as error:
            print(json.dumps({"event": "sheets_sync_queued", "listing": identity_key, "error": str(error)[:300]}, ensure_ascii=False), flush=True)


def make_pdf(payload):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import cm
        from reportlab.lib.utils import ImageReader
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except ImportError as error:
        raise RuntimeError("PDF-модуль не встановлено. Перезапустіть застосунок через start.command.") from error
    show_title = bool(payload.get("include_title"))
    title = html.escape(str(payload.get("title", "")).strip()) if show_title else ""
    if show_title and not title:
        raise ValueError("Потрібен заголовок для PDF.")
    language = payload.get("language", "uk")
    price_label, contacts_label = ("Ціна", "Контакти") if language == "uk" else ("Price", "Contacts")
    font_candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/segoeui.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
    ]
    font_path = next((candidate for candidate in font_candidates if os.path.exists(candidate)), "")
    font_name = "Helvetica"
    if font_path:
        pdfmetrics.registerFont(TTFont("KYIVEstateUnicode", font_path))
        font_name = "KYIVEstateUnicode"
    styles = getSampleStyleSheet()
    normal = ParagraphStyle("listing", parent=styles["BodyText"], fontName=font_name, leading=17, spaceAfter=8)
    heading = ParagraphStyle("listing-title", parent=styles["Title"], fontName=font_name, leading=28, textColor=colors.HexColor("#168acd"))
    output = BytesIO()
    document = SimpleDocTemplate(output, pagesize=A4, rightMargin=1.6 * cm, leftMargin=1.6 * cm, topMargin=1.5 * cm, bottomMargin=1.5 * cm)
    story = [Paragraph(title, heading), Spacer(1, 0.3 * cm)] if title else []
    details = payload.get("details", {})
    prices = payload.get("prices", {})
    rows = [(price_label, display_price(details, prices)), *property_detail_rows(details, language)]
    rows = [[Paragraph(html.escape(label), normal), Paragraph(html.escape(value), normal)] for label, value in rows if value]
    if rows:
        table = Table(rows, colWidths=[4 * cm, 12 * cm])
        table.setStyle(TableStyle([("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#eef6fb")), ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#d0d5dd")), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("LEFTPADDING", (0, 0), (-1, -1), 9), ("RIGHTPADDING", (0, 0), (-1, -1), 9)]))
        story.extend([table, Spacer(1, 0.45 * cm)])
    for paragraph in str(payload.get("text", "")).splitlines():
        if paragraph.strip(): story.append(Paragraph(html.escape(paragraph.strip()), normal))
    local_photos = []
    if str(payload.get("processing_mode") or "browser").lower() == "ai":
        job_id = re.sub(r"[^a-zA-Z0-9_-]", "", str(payload.get("internal_id", "")))
        local_root = PACKAGES_ROOT / job_id / "photos"
        if local_root.is_dir():
            manifest_path = PACKAGES_ROOT / job_id / "manifest.json"
            if manifest_path.is_file():
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                local_photos = [local_root / item["filename"] for item in manifest.get("photos", []) if (local_root / item.get("filename", "")).is_file()][:MAX_AI_PHOTOS]
            else:
                local_photos = sorted(path for path in local_root.iterdir() if path.is_file())[:MAX_AI_PHOTOS]
    pdf_image_buffers = []

    def append_pdf_image(source):
        if isinstance(source, (bytes, bytearray)):
            with PILImage.open(BytesIO(source)) as original:
                image = ImageOps.exif_transpose(original).convert("RGB")
                if max(image.size) > 1600:
                    ratio = 1600 / max(image.size)
                    image = image.resize((round(image.width * ratio), round(image.height * ratio)), PILImage.Resampling.LANCZOS)
                encoded = BytesIO()
                image.save(encoded, format="JPEG", quality=82, optimize=True)
            reader_source = image_source = BytesIO(encoded.getvalue())
            pdf_image_buffers.append(image_source)
        else:
            reader_source = image_source = str(source)
        reader = ImageReader(reader_source)
        width, height = reader.getSize()
        ratio = min(16 * cm / width, 11 * cm / height, 1)
        if hasattr(image_source, "seek"):
            image_source.seek(0)
        story.extend([Spacer(1, 0.25 * cm), Image(image_source, width=width * ratio, height=height * ratio)])

    added_photos = 0
    if local_photos:
        photo_sources = local_photos
    else:
        photo_sources = (url for url in clean_image_urls(payload.get("images", [])) if safe_remote_url(url))
    for source in photo_sources:
        try:
            if isinstance(source, str) and source.startswith("https://"):
                response = requests.get(source, headers=REQUEST_HEADERS, timeout=12)
                response.raise_for_status()
                source = response.content
            append_pdf_image(source)
            added_photos += 1
            if added_photos == 1:
                append_pdf_image(ensure_local_logo())
        except Exception as error:
            print(f"Skipping PDF photo: {type(error).__name__}: {error}", flush=True)
            continue
    pdf_contacts = []
    for label, url in agency_links():
        display = CONTACT_PHONE if label == "Phone" else label
        pdf_contacts.append(
            f'<a href="{html.escape(url, quote=True)}">{html.escape(display)}</a>'
        )
    if pdf_contacts:
        story.extend([
            Spacer(1, 0.35 * cm),
            Paragraph(f"{contacts_label}: " + " · ".join(pdf_contacts), normal),
        ])
    document.build(story)
    body = output.getvalue()
    job_id = re.sub(r"[^a-zA-Z0-9_-]", "", str(payload.get("internal_id", "")))
    if job_id:
        pdf_root = PACKAGES_ROOT / job_id / "pdf"
        pdf_root.mkdir(parents=True, exist_ok=True)
        pdf_path = pdf_root / ("en.pdf" if language == "en" else "uk.pdf")
        pdf_path.write_bytes(body)
        sync_sheet_record(payload, "pdf", language)
    return body


def make_photo_zip(payload):
    image_urls = clean_image_urls(payload.get("images", []))
    if not image_urls:
        raise ValueError("No direct source photographs are selected.")
    output = BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for index, url in enumerate(image_urls, 1):
            if not safe_remote_url(url):
                raise ValueError(f"Unsafe photograph URL at position {index}.")
            last_error = None
            for attempt in range(3):
                try:
                    response = requests.get(url, headers=REQUEST_HEADERS, timeout=20)
                    response.raise_for_status()
                    content_type = response.headers.get("Content-Type", "").split(";", 1)[0].lower()
                    if not content_type.startswith("image/"):
                        raise ValueError(f"Source item {index} is not an image.")
                    extension = mimetypes.guess_extension(content_type) or Path(urlparse(url).path).suffix or ".jpg"
                    if extension == ".jpe":
                        extension = ".jpg"
                    archive.writestr(f"{index:03d}{extension}", response.content)
                    last_error = None
                    break
                except (requests.RequestException, ValueError) as error:
                    last_error = error
                    if attempt < 2:
                        time.sleep(1.5 * (2 ** attempt))
            if last_error:
                raise RuntimeError(f"Could not download source photograph {index}: {last_error}")
    return output.getvalue()


def reply(start_response, status, data):
    body = json.dumps(data, ensure_ascii=False).encode()
    start_response(status, [("Content-Type", "application/json; charset=utf-8"), ("Content-Length", str(len(body)))])
    return [body]


def pdf_reply(start_response, body):
    start_response("200 OK", [("Content-Type", "application/pdf"), ("Content-Disposition", "attachment; filename=listing.pdf"), ("Content-Length", str(len(body)))])
    return [body]


def zip_reply(start_response, body, filename="kyiv-estate-photos.zip"):
    start_response("200 OK", [("Content-Type", "application/zip"), ("Content-Disposition", f'attachment; filename="{filename}"'), ("Content-Length", str(len(body))), ("Cache-Control", "no-store")])
    return [body]


def app(environ, start_response):
    path, method = environ["PATH_INFO"], environ["REQUEST_METHOD"]
    try:
        if path == "/health" and method == "GET":
            return reply(start_response, "200 OK", {
                "ok": True,
                "service": "KYIV ESTATE",
                "version": "1.2.0-production",
                "revision": os.environ.get("RAILWAY_GIT_COMMIT_SHA", "ca725db")[:7],
            })
        if path == "/" and method == "GET":
            body = (ROOT / "index.html").read_bytes()
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body))), ("Cache-Control", "no-store, max-age=0")])
            return [body]
        if path == "/api/bridge/jobs/next" and method == "GET":
            if not bridge_authorized(environ):
                return reply(start_response, "401 Unauthorized", {"error": "unauthorized"})
            return bridge_reply_job(start_response)
        bridge_photo = re.fullmatch(r"/api/bridge/jobs/([a-f0-9]{32})/photos/(\d{2})\.(jpg|jpeg|png|webp)", path)
        if bridge_photo and method == "POST":
            if not bridge_authorized(environ):
                return reply(start_response, "401 Unauthorized", {"error": "unauthorized"})
            length = int(environ.get("CONTENT_LENGTH") or 0)
            if length < 1024 or length > 30 * 1024 * 1024:
                return reply(start_response, "413 Payload Too Large", {"error": "invalid image size"})
            root = DATA_ROOT / "bridge-uploads" / bridge_photo.group(1)
            root.mkdir(parents=True, exist_ok=True)
            destination = root / f"{bridge_photo.group(2)}.{bridge_photo.group(3)}"
            destination.write_bytes(environ["wsgi.input"].read(length))
            return reply(start_response, "200 OK", {"ok": True})
        bridge_finish = re.fullmatch(r"/api/bridge/jobs/([a-f0-9]{32})/(complete|fail)", path)
        if bridge_finish and method == "POST":
            if not bridge_authorized(environ):
                return reply(start_response, "401 Unauthorized", {"error": "unauthorized"})
            length = int(environ.get("CONTENT_LENGTH") or 0)
            payload = json.loads(environ["wsgi.input"].read(length) or b"{}") if 0 <= length <= 100_000 else {}
            status = "ready" if bridge_finish.group(2) == "complete" else "failed"
            now = datetime.now(timezone.utc).isoformat()
            with database() as db:
                db.execute("UPDATE ai_bridge_jobs SET status=?,error=?,updated_at=? WHERE id=?",
                           (status, str(payload.get("error") or "")[:2000] or None, now, bridge_finish.group(1)))
            return reply(start_response, "200 OK", {"ok": True, "status": status})
        if path.startswith("/packages/") and method == "GET":
            requested = (PACKAGES_ROOT / path.removeprefix("/packages/")).resolve()
            if PACKAGES_ROOT.resolve() not in requested.parents or not requested.is_file():
                return reply(start_response, "404 Not Found", {"error": "Не знайдено"})
            content_type = "text/html; charset=utf-8" if requested.suffix == ".html" else "application/json; charset=utf-8" if requested.suffix == ".json" else "application/pdf" if requested.suffix == ".pdf" else "image/png" if requested.suffix == ".png" else "image/webp" if requested.suffix == ".webp" else "image/jpeg"
            body = requested.read_bytes()
            start_response("200 OK", [("Content-Type", content_type), ("Content-Length", str(len(body)))])
            return [body]
        if path in {"/api/extract", "/api/publish", "/api/translate", "/api/pdf", "/api/photos", "/api/package"} and method == "POST":
            length = int(environ.get("CONTENT_LENGTH") or 0)
            if length <= 0 or length > 2_000_000:
                return reply(start_response, "413 Payload Too Large", {"error": "Некоректний розмір запиту."})
            payload = json.loads(environ["wsgi.input"].read(length) or b"{}")
            if path.endswith("extract"):
                return reply(start_response, "200 OK", extract_listing(payload.get("url", "")))
            if path.endswith("translate"):
                return reply(start_response, "200 OK", {"title": translate_to_english(str(payload.get("title", ""))), "text": translate_to_english(str(payload.get("text", "")))})
            if path.endswith("pdf"):
                return pdf_reply(start_response, make_pdf(payload))
            if path.endswith("photos"):
                return zip_reply(start_response, make_photo_zip(payload))
            if path.endswith("package"):
                return reply(start_response, "410 Gone", {"error": "Server-side photo packages are disabled; direct source URLs are used."})
            language = str(payload.get("language", "uk"))
            return reply(start_response, "200 OK", publish_single_language(payload, language))
        return reply(start_response, "404 Not Found", {"error": "Не знайдено"})
    except (ValueError, requests.RequestException, RuntimeError) as error:
        return reply(start_response, "400 Bad Request", {"error": str(error)})
    except Exception as error:
        print(f"Unhandled HTTP request error: {type(error).__name__}: {error}", flush=True)
        return reply(start_response, "500 Internal Server Error", {"error": f"Service processing error: {type(error).__name__}: {str(error)[:240]}"})


init_storage()
repair_existing_packages_once()


if __name__ == "__main__":
    class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
        daemon_threads = True

    print(f"Open http://localhost:{PORT}")
    make_server("0.0.0.0", PORT, app, server_class=ThreadingWSGIServer).serve_forever()
