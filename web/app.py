"""Minimal FastAPI wrapper that exposes the CLI flags via a web form."""

from __future__ import annotations

import uuid
from pathlib import Path
import asyncio
import csv
import json
import logging
import os
import tempfile
import threading
import time
from contextlib import contextmanager
from email.message import EmailMessage
from typing import Optional, Sequence
import smtplib
import ssl

from fastapi import BackgroundTasks, FastAPI, Form, Request, Body
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, RedirectResponse, JSONResponse
import httpx

from job_scraper.main import DEFAULT_AREA, main as cli_main
from job_scraper.main import _process_place as _main_process_place  # type: ignore
from job_scraper.main import _write_results as _main_write_results  # type: ignore
from job_scraper.main import _deduplicate_by_job_page as _main_dedupe  # type: ignore
from job_scraper.main import _canonicalize_url as _main_canonicalize_url  # type: ignore
from job_scraper.crawler import AsyncCrawler
from job_scraper.discovery_overpass import (
    OverpassError,
    fetch_places,
    fetch_places_by_grid,
)

# Restore request-level logging (including httpx request lines) in the app process.
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.INFO)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Ephemeral output storage (temp directory) with automatic cleanup
OUTPUT_DIR = Path(tempfile.gettempdir()) / "city_gig_scraper_outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
JOB_TTL_SECONDS = 60 * 60  # 1 hour
CLEANUP_INTERVAL_SECONDS = 10 * 60  # 10 minutes

_cleanup_task: asyncio.Task | None = None

# Simple file-backed stats (no database)
# Use a persistent path in the repo's output folder by default; allow override via env var.
_BASE_DIR = Path(__file__).resolve().parents[1]
STATS_DIR = _BASE_DIR / "output"
STATS_DIR.mkdir(parents=True, exist_ok=True)
_ENV_STATS_FILE = os.getenv("STATS_FILE")
COUNTER_FILE = Path(_ENV_STATS_FILE) if _ENV_STATS_FILE else (STATS_DIR / "stats.json")

# Optional persistent counter via Upstash Redis (free-tier friendly)
REDIS_URL = os.getenv("UPSTASH_REDIS_REST_URL")
REDIS_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")
COUNTER_KEY_NAME = os.getenv("COUNTER_KEY", "jobs_started")

def _load_stats() -> dict[str, int]:
    try:
        if COUNTER_FILE.exists():
            with COUNTER_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and "jobs_started" in data:
                    try:
                        return {"jobs_started": int(data.get("jobs_started", 0))}
                    except Exception:
                        return {"jobs_started": 0}
    except Exception:
        pass
    return {"jobs_started": 0}

def _save_stats(stats: dict[str, int]) -> None:
    try:
        with COUNTER_FILE.open("w", encoding="utf-8") as f:
            json.dump({"jobs_started": int(stats.get("jobs_started", 0))}, f)
    except Exception:
        # best-effort; ignore errors
        pass

# ---- Redis-backed counter helpers (fallback to file) ----
def _redis_enabled() -> bool:
    return bool(REDIS_URL and REDIS_TOKEN)

def _redis_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {REDIS_TOKEN}"}

def _get_jobs_started() -> int:
    # Prefer Redis if configured
    if _redis_enabled():
        try:
            resp = httpx.get(f"{REDIS_URL}/get/{COUNTER_KEY_NAME}", headers=_redis_headers(), timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                val = data.get("result")
                return int(val or 0)
        except Exception:
            # fall through to file
            pass
    # File fallback (local/dev)
    return int(_load_stats().get("jobs_started", 0))

def _increment_jobs_started(amount: int = 1) -> int:
    # Prefer Redis if configured
    if _redis_enabled():
        try:
            resp = httpx.get(f"{REDIS_URL}/incrby/{COUNTER_KEY_NAME}/{int(amount)}", headers=_redis_headers(), timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                val = data.get("result")
                return int(val or 0)
        except Exception:
            # fall through to file
            pass
    # File fallback (local/dev)
    stats = _load_stats()
    stats["jobs_started"] = int(stats.get("jobs_started", 0)) + int(amount)
    _save_stats(stats)
    return stats["jobs_started"]


async def _cleanup_loop() -> None:
    while True:
        try:
            now = time.time()
            expired: list[str] = []
            for job_id, info in list(JOBS.items()):
                created_at = float(info.get("created_at", 0.0) or 0.0)
                status = str(info.get("status", ""))
                # Expire finished/cancelled/error jobs after TTL, and stale running jobs
                ttl_seconds = float(info.get("ttl_seconds", JOB_TTL_SECONDS) or JOB_TTL_SECONDS)
                if created_at and (now - created_at) > ttl_seconds:
                    expired.append(job_id)
            for job_id in expired:
                try:
                    path = Path(str(JOBS.get(job_id, {}).get("output", "")))
                    if path.exists() and path.is_file():
                        path.unlink(missing_ok=True)
                except Exception:
                    pass
                JOBS.pop(job_id, None)
        except Exception:
            # Best-effort cleanup; ignore errors
            pass
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)

app = FastAPI(title="City Gig Scraper UI")

# Simple in-memory store for job status.
JOBS: dict[str, dict[str, object]] = {}


def _load_preview_rows(path: Path, limit: int = 50) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not path.exists() or not path.is_file():
        return rows
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for index, row in enumerate(reader):
                if index >= limit:
                    break
                rows.append({k: str(v) for k, v in row.items()})
    except Exception:
        return []
    return rows


def _ensure_preview(job_record: dict[str, object], *, limit: int = 50) -> list[dict[str, str]]:
    preview = job_record.get("preview")
    if isinstance(preview, list):
        return preview  # type: ignore[return-value]
    output_ref = job_record.get("output") or ""
    path = Path(str(output_ref))
    rows = _load_preview_rows(path, limit=limit)
    job_record["preview"] = rows
    return rows


@contextmanager
def _job_slot() -> None:
    JOB_SEMAPHORE.acquire()
    try:
        yield
    finally:
        JOB_SEMAPHORE.release()

@app.on_event("startup")
async def _on_startup() -> None:
    global _cleanup_task
    if _cleanup_task is None:
        _cleanup_task = asyncio.create_task(_cleanup_loop())
    # Initialize stats file if missing
    try:
        if not COUNTER_FILE.exists():
            _save_stats({"jobs_started": 0})
    except Exception:
        pass

def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_overpass_urls() -> list[str]:
    urls: list[str] = []
    when_multi = os.getenv("OVERPASS_URLS")
    if when_multi:
        urls.extend([u.strip() for u in when_multi.split(",") if u.strip()])
    single = os.getenv("OVERPASS_URL")
    if single:
        urls.append(single.strip())
    # Preserve insertion order but dedupe
    seen: set[str] = set()
    unique: list[str] = []
    for url in urls:
        if url and url not in seen:
            seen.add(url)
            unique.append(url)
    return unique


def _is_valid_email(value: str) -> bool:
    if not value or "@" not in value or any(ch.isspace() for ch in value):
        return False
    local, _, domain = value.rpartition("@")
    if not local or not domain or "." not in domain:
        return False
    return True


def _build_download_url(job_id: str, job_record: dict[str, object]) -> str:
    base_url = str(job_record.get("base_url") or "")
    path = f"download/{job_id}"
    if base_url:
        base = base_url.rstrip("/") + "/"
        return base + path
    return f"/{path}"


def _available_email_providers() -> list[str]:
    providers: list[str] = []
    if RESEND_API_KEY and EMAIL_FROM:
        providers.append("resend")
    if POSTMARK_TOKEN and EMAIL_FROM:
        providers.append("postmark")
    if SMTP_HOST and (SMTP_FROM or EMAIL_FROM or SMTP_USER):
        providers.append("smtp")
    return providers


def _send_email_resend(to_email: str, subject: str, text_body: str, html_body: str | None) -> None:
    if not (RESEND_API_KEY and EMAIL_FROM):
        raise RuntimeError("Resend not configured")
    payload = {
        "from": EMAIL_FROM,
        "to": [to_email],
        "subject": subject,
        "html": html_body or text_body,
        "text": text_body,
    }
    headers = {"Authorization": f"Bearer {RESEND_API_KEY}"}
    response = httpx.post(
        "https://api.resend.com/emails",
        headers=headers,
        json=payload,
        timeout=httpx.Timeout(15.0, connect=5.0, read=15.0),
    )
    response.raise_for_status()


def _send_email_postmark(to_email: str, subject: str, text_body: str, html_body: str | None) -> None:
    if not (POSTMARK_TOKEN and EMAIL_FROM):
        raise RuntimeError("Postmark not configured")
    payload = {
        "From": EMAIL_FROM,
        "To": to_email,
        "Subject": subject,
        "HtmlBody": html_body or text_body,
        "TextBody": text_body,
        "MessageStream": "outbound",
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Postmark-Server-Token": POSTMARK_TOKEN,
    }
    response = httpx.post(
        "https://api.postmarkapp.com/email",
        headers=headers,
        json=payload,
        timeout=httpx.Timeout(15.0, connect=5.0, read=15.0),
    )
    response.raise_for_status()


def _send_email_smtp(to_email: str, subject: str, text_body: str, html_body: str | None) -> None:
    if not SMTP_HOST:
        raise RuntimeError("SMTP host not configured")
    from_addr = SMTP_FROM or EMAIL_FROM or SMTP_USER
    if not from_addr:
        raise RuntimeError("SMTP_FROM/EMAIL_FROM not configured")
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_email
    msg.set_content(text_body or html_body or "")
    if html_body:
        msg.add_alternative(html_body, subtype="html")

    port = int(SMTP_PORT or ("465" if SMTP_MODE == "ssl" else "587"))

    if SMTP_MODE == "ssl":
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, port, context=context, timeout=SMTP_TIMEOUT) as server:
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PASSWORD or "")
            server.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_HOST, port, timeout=SMTP_TIMEOUT) as server:
            if SMTP_MODE == "starttls":
                context = ssl.create_default_context()
                server.starttls(context=context)
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PASSWORD or "")
            server.send_message(msg)


def send_result_email(to_email: str, subject: str, text_body: str, html_body: str | None = None) -> None:
    providers = _available_email_providers()
    if not providers:
        raise RuntimeError("No email provider configured")
    last_error: Exception | None = None
    for attempt in range(3):
        for provider in providers:
            try:
                if provider == "resend":
                    _send_email_resend(to_email, subject, text_body, html_body)
                elif provider == "postmark":
                    _send_email_postmark(to_email, subject, text_body, html_body)
                else:
                    _send_email_smtp(to_email, subject, text_body, html_body)
                logger.info("Sent results email to %s via %s", to_email, provider)
                return
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Email delivery via %s failed (attempt %d): %s",
                    provider,
                    attempt + 1,
                    exc,
                )
        time.sleep(min(2 ** attempt, 5.0))
    raise RuntimeError(f"Email delivery failed: {last_error}") from last_error


UI_USER_AGENT = os.getenv("UI_USER_AGENT", "JobScraper/0.1 (+https://github.com/novaheic)")
UI_CONCURRENCY = _env_int("UI_CONCURRENCY", 8)
UI_MAX_JOB_LINKS = _env_int("UI_MAX_JOB_LINKS", 6)
UI_CRAWL_DEPTH = _env_int("UI_CRAWL_DEPTH", 3)
_ui_limit = _env_int("UI_LIMIT", 0)
UI_LIMIT: Optional[int] = _ui_limit if _ui_limit > 0 else None
UI_OVERPASS_URLS = _parse_overpass_urls()
UI_OVERPASS_URL: Optional[str] = UI_OVERPASS_URLS[0] if UI_OVERPASS_URLS else None
UI_LOG_LEVEL = os.getenv("UI_LOG_LEVEL", "INFO")
EMAIL_JOB_TTL_SECONDS = max(JOB_TTL_SECONDS, _env_int("RESULTS_TTL_SECONDS", 24 * 60 * 60))
_raw_email_from = (os.getenv("EMAIL_FROM") or "").strip()
_raw_smtp_from = (os.getenv("SMTP_FROM") or "").strip()
EMAIL_FROM = _raw_email_from or (_raw_smtp_from or None)
SMTP_FROM = _raw_smtp_from or EMAIL_FROM
RESEND_API_KEY = (os.getenv("RESEND_API_KEY") or "").strip() or None
POSTMARK_TOKEN = (os.getenv("POSTMARK_TOKEN") or "").strip() or None
SMTP_HOST = (os.getenv("SMTP_HOST") or "").strip() or None
SMTP_PORT = (os.getenv("SMTP_PORT") or "").strip() or None
SMTP_USER = (os.getenv("SMTP_USER") or "").strip() or None
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD") or None
_smtp_mode_raw = (os.getenv("SMTP_TLS") or "true").strip().lower()
if _smtp_mode_raw in {"ssl", "smtps"}:
    SMTP_MODE = "ssl"
elif _smtp_mode_raw in {"starttls", "tls", "true", "1", "yes"}:
    SMTP_MODE = "starttls"
else:
    SMTP_MODE = "none"
SMTP_TIMEOUT = float(os.getenv("SMTP_TIMEOUT", "20"))
MAX_ACTIVE_JOBS = max(1, _env_int("UI_MAX_ACTIVE_JOBS", 3))
JOB_SEMAPHORE = threading.Semaphore(MAX_ACTIVE_JOBS)

EU_CITIES: list[str] = [
    # Austria
    "Vienna", "Graz", "Linz", "Salzburg", "Innsbruck", "Klagenfurt", "Villach", "Wels", "Sankt Pölten", "Dornbirn",
    # Belgium
    "Brussels", "Antwerp", "Ghent", "Charleroi", "Liège", "Bruges", "Namur", "Leuven", "Mons", "Aalst",
    # Bulgaria
    "Sofia", "Plovdiv", "Varna", "Burgas", "Ruse", "Stara Zagora", "Pleven", "Sliven", "Dobrich", "Shumen",
    # Croatia
    "Zagreb", "Split", "Rijeka", "Osijek", "Zadar", "Slavonski Brod", "Pula", "Karlovac", "Varaždin", "Šibenik",
    # Cyprus
    "Nicosia", "Limassol", "Larnaca", "Paphos", "Paralimni", "Strovolos", "Ayia Napa", "Aradippou", "Geroskipou", "Lakatamia",
    # Czechia
    "Prague", "Brno", "Ostrava", "Plzeň", "Liberec", "Olomouc", "České Budějovice", "Hradec Králové", "Ústí nad Labem", "Pardubice",
    # Denmark
    "Copenhagen", "Aarhus", "Odense", "Aalborg", "Esbjerg", "Randers", "Kolding", "Horsens", "Vejle", "Roskilde",
    # Estonia
    "Tallinn", "Tartu", "Narva", "Pärnu", "Kohtla-Järve", "Viljandi", "Rakvere", "Maardu", "Kuressaare", "Võru",
    # Finland
    "Helsinki", "Espoo", "Tampere", "Vantaa", "Oulu", "Turku", "Jyväskylä", "Lahti", "Kuopio", "Kouvola",
    # France
    "Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille",
    # Germany
    "Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt am Main", "Frankfurt (Oder)", "Stuttgart", "Düsseldorf", "Dortmund", "Essen", "Leipzig",
    # Greece
    "Athens", "Thessaloniki", "Patras", "Heraklion", "Larissa", "Volos", "Ioannina", "Chania", "Chalkida", "Kalamata",
    # Hungary
    "Budapest", "Debrecen", "Szeged", "Miskolc", "Pécs", "Győr", "Nyíregyháza", "Kecskemét", "Székesfehérvár", "Szombathely",
    # Ireland
    "Dublin", "Cork", "Limerick", "Galway", "Waterford", "Drogheda", "Swords", "Dundalk", "Bray", "Navan",
    # Italy
    "Rome", "Milan", "Naples", "Turin", "Palermo", "Genoa", "Bologna", "Florence", "Bari", "Catania",
    # Latvia
    "Riga", "Daugavpils", "Liepāja", "Jelgava", "Jūrmala", "Ventspils", "Rēzekne", "Valmiera", "Ogre", "Tukums",
    # Lithuania
    "Vilnius", "Kaunas", "Klaipėda", "Šiauliai", "Panevėžys", "Alytus", "Marijampolė", "Mažeikiai", "Jonava", "Utena",
    # Luxembourg
    "Luxembourg", "Esch-sur-Alzette", "Differdange", "Dudelange", "Ettelbruck", "Diekirch", "Wiltz", "Echternach", "Rumelange", "Grevenmacher",
    # Malta
    "Valletta", "Birkirkara", "Mosta", "Qormi", "Żabbar", "St. Paul's Bay", "Sliema", "Żejtun", "Rabat", "Marsaskala",
    # Netherlands
    "Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven", "Tilburg", "Groningen", "Almere", "Breda", "Nijmegen",
    # Poland
    "Warsaw", "Kraków", "Łódź", "Wrocław", "Poznań", "Gdańsk", "Szczecin", "Bydgoszcz", "Lublin", "Katowice",
    # Portugal
    "Lisbon", "Porto", "Vila Nova de Gaia", "Amadora", "Braga", "Coimbra", "Funchal", "Setúbal", "Almada", "Loures",
    # Romania
    "Bucharest", "Cluj-Napoca", "Timișoara", "Iași", "Constanța", "Craiova", "Brașov", "Galați", "Ploiești", "Oradea",
    # Slovakia
    "Bratislava", "Košice", "Prešov", "Žilina", "Nitra", "Banská Bystrica", "Trnava", "Martin", "Trenčín", "Poprad",
    # Slovenia
    "Ljubljana", "Maribor", "Celje", "Kranj", "Velenje", "Koper", "Novo Mesto", "Ptuj", "Trbovlje", "Kamnik",
    # Spain
    "Madrid", "Barcelona", "Valencia", "Seville", "Zaragoza", "Málaga", "Murcia", "Palma", "Las Palmas", "Bilbao",
    # Sweden
    "Stockholm", "Gothenburg", "Malmö", "Uppsala", "Västerås", "Örebro", "Linköping", "Helsingborg", "Jönköping", "Norrköping",
]

US_CITIES: list[str] = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
    "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington DC",
    "Boston", "Nashville", "El Paso", "Detroit", "Memphis",
    "Portland", "Oklahoma City", "Las Vegas", "Louisville", "Baltimore",
]

CA_CITIES: list[str] = [
    "Toronto", "Montreal", "Vancouver", "Calgary", "Edmonton",
    "Ottawa", "Winnipeg", "Quebec City", "Hamilton", "Kitchener",
]

WORLD_TOP_CITIES: list[str] = [
    "Tokyo", "Delhi", "Shanghai", "Dhaka", "São Paulo", "Mexico City", "Cairo", "Beijing",
    "Mumbai", "Osaka", "Chongqing", "Karachi", "Buenos Aires", "Istanbul", "Kolkata",
    "Manila", "Lagos", "Rio de Janeiro", "Tianjin", "Kinshasa", "Guangzhou", "Moscow",
    "Shenzhen", "Lahore", "Bangalore", "Paris", "Bogota", "Jakarta", "Chennai", "Lima",
    "Bangkok", "Seoul", "Nagoya", "Hyderabad", "London", "Tehran", "Chicago", "Chengdu",
    "Nanjing", "Wuhan", "Ho Chi Minh City", "Luanda", "Ahmedabad", "Kuala Lumpur", "Xi'an",
    "Hong Kong", "Dongguan", "Hangzhou", "Foshan", "Shenyang", "Riyadh", "Baghdad", "Santiago",
    "Surabaya", "Madrid", "Qingdao", "Riyadh", "Singapore", "Alexandria", "Ankara", "Yangon",
    "Johannesburg", "Addis Ababa", "Casablanca", "Shantou", "Suzhou", "Zhengzhou", "Jinan",
    "Hanoi", "Shijiazhuang", "Harbin", "Dalian", "Kunming", "Hangzhou", "Nairobi", "Taipei",
    "Melbourne", "Sydney", "Toronto", "Vancouver", "Montreal", "Johor Bahru", "Belo Horizonte",
    "Fortaleza", "Kuwait City", "Doha", "Abu Dhabi", "Dubai", "Tel Aviv", "Karaj",
]

CITY_SUGGESTIONS: list[str] = list(
    dict.fromkeys(EU_CITIES + US_CITIES + CA_CITIES + WORLD_TOP_CITIES)
)  # preserve order and de-duplicate

AMENITY_OPTIONS: list[tuple[str, str, str]] = [
    ("cafe", "☕", "Cafes"),
    ("restaurant", "🍽️", "Restaurants"),
    ("bar", "🍸", "Bars"),
    ("pub", "🍺", "Pubs"),
    ("fast_food", "🍔", "Fast Food"),
    ("bakery", "🥐", "Bakeries"),
    ("ice_cream", "🍨", "Ice Cream"),
    ("biergarten", "🍻", "Beer Gardens"),
    ("food_court", "🥡", "Food Courts"),
    ("nightclub", "🕺", "Nightclubs"),
    ("hotel", "🏨", "Hotels"),
    ("hostel", "🛏️", "Hostels"),
    ("cinema", "🎬", "Cinemas"),
    ("theatre", "🎭", "Theatres"),
]

DEFAULT_AMENITY_SELECTION = ["cafe", "restaurant", "bar", "pub", "bakery"]
DEFAULT_AMENITY_STRING = ",".join(DEFAULT_AMENITY_SELECTION)


def _build_argv(
    *,
    area: str,
    amenities: str,
    output: str,
    user_agent: str,
    concurrency: int,
    max_job_links: int,
    crawl_depth: int,
    log_level: str,
    limit: Optional[int],
    overpass_url: Optional[str],
    split_districts: bool,
) -> list[str]:
    argv: list[str] = [
        "--area",
        area,
        "--amenities",
        amenities,
        "--output",
        output,
        "--user-agent",
        user_agent,
        "--concurrency",
        str(concurrency),
        "--max-job-links",
        str(max_job_links),
        "--crawl-depth",
        str(crawl_depth),
        "--log-level",
        log_level,
    ]

    if limit is not None:
        argv.extend(["--limit", str(limit)])

    if overpass_url:
        argv.extend(["--overpass-url", overpass_url])

    if split_districts:
        argv.append("--split-into-districts")

    return argv


async def _execute_job(
    *,
    job_id: str,
    area: str,
    amenities: str,
    user_agent: str,
    concurrency: int,
    max_job_links: int,
    crawl_depth: int,
    log_level: str,
    overpass_url: str | None,
    limit: int | None,
    overpass_urls: Sequence[str] | None,
) -> None:
    # Phase 1: list places
    job_record = JOBS.get(job_id)
    if not job_record or job_record.get("status") == "cancelled":
        return
    job_record.update(
        {
            "status": "running",
            "phase": "listing",
            "total": 0,
            "processed": 0,
            "found": 0,
        }
    )
    kwargs: dict[str, object] = {}
    if overpass_urls:
        kwargs["overpass_urls"] = list(overpass_urls)
        if not overpass_url and overpass_urls:
            overpass_url = overpass_urls[0]
    if overpass_url:
        kwargs["overpass_url"] = overpass_url

    # Early cancellation check before doing network-bound discovery
    if job_record.get("status") == "cancelled":
        return
    try:
        try:
            places = fetch_places(area, amenities.split(","), **kwargs)
        except OverpassError:
            places = fetch_places_by_grid(area, amenities.split(","), **kwargs)
    except Exception as exc:  # pragma: no cover
        job_record["status"] = "error"
        job_record["error"] = f"Discovery failed: {exc}"
        return
    except MemoryError as exc:  # pragma: no cover
        job_record["status"] = "error"
        job_record["error"] = "Scrape failed: out of memory."
        return

    if limit is not None and limit >= 0:
        places = places[:limit]

    total = len(places)
    job_record["total"] = total
    job_record["phase"] = "scanning"

    # Phase 2: scan with progress (streaming write, bounded workers)
    output_ref = job_record.get("output")
    if not output_ref:
        job_record["status"] = "error"
        job_record["error"] = "Missing output path"
        return
    out_path = Path(str(output_ref))
    fieldnames = ["name", "type", "homepage", "job_page_url"]
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # If cancelled after discovery, exit before spinning up crawl tasks
    if job_record.get("status") == "cancelled":
        return

    crawler = AsyncCrawler(user_agent=user_agent, concurrency=concurrency)
    try:
        async with crawler as active_crawler:
            with out_path.open("w", newline="", encoding="utf-8") as out_handle:
                writer = csv.DictWriter(out_handle, fieldnames=fieldnames)
                writer.writeheader()
                out_handle.flush()

                write_lock = asyncio.Lock()
                seen_job_pages: set[str] = set()

                queue: asyncio.Queue[object] = asyncio.Queue(maxsize=max(1, concurrency * 5))
                num_workers = max(1, concurrency)

                async def worker() -> None:
                    while True:
                        item = await queue.get()
                        try:
                            if item is None:
                                return
                            if job_record.get("status") == "cancelled":
                                continue
                            place = item  # type: ignore[assignment]
                            try:
                                res = await _main_process_place(
                                    place,
                                    active_crawler,
                                    max_job_links=max_job_links,
                                    crawl_depth=crawl_depth,
                                )
                            except Exception:
                                res = None
                            job_record["processed"] = int(job_record.get("processed", 0) or 0) + 1
                            if res is not None and getattr(res, "hiring", False) and getattr(res, "job_page_url", None):
                                canonical = _main_canonicalize_url(res.job_page_url) or res.job_page_url
                                async with write_lock:
                                    if canonical not in seen_job_pages:
                                        seen_job_pages.add(canonical)
                                        writer.writerow(
                                            {
                                                "name": res.place.name,
                                                "type": res.place.amenity,
                                                "homepage": res.place.website,
                                                "job_page_url": res.job_page_url or "",
                                            }
                                        )
                                        out_handle.flush()
                                        job_record["found"] = int(job_record.get("found", 0) or 0) + 1
                        finally:
                            queue.task_done()

                workers = [asyncio.create_task(worker()) for _ in range(num_workers)]
                try:
                    for place in places:
                        if job_record.get("status") == "cancelled":
                            break
                        await queue.put(place)
                finally:
                    for _ in range(num_workers):
                        await queue.put(None)
                    await queue.join()
                    for w in workers:
                        await w
    except MemoryError:  # pragma: no cover
        job_record["status"] = "error"
        job_record["error"] = "Scrape failed: out of memory. Please narrow the search."
        return
    except Exception as exc:  # pragma: no cover
        job_record["status"] = "error"
        job_record["error"] = f"Scan failed: {exc}"
        return
    finally:
        crawler.clear_host_state()

    # If cancelled during scanning, stop (best-effort: output may have partial rows)
    if job_record.get("status") == "cancelled":
        _ensure_preview(job_record)
        return

    _ensure_preview(job_record)

    job_record["status"] = "done"
    job_record["phase"] = "complete"
    job_record["finished_at"] = time.time()
    notify_email_value = str(job_record.get("notify_email") or "")
    if notify_email_value:
        download_url = _build_download_url(job_id, job_record)
        ttl_seconds = float(job_record.get("ttl_seconds", EMAIL_JOB_TTL_SECONDS) or EMAIL_JOB_TTL_SECONDS)
        ttl_hours = max(1, int(round(ttl_seconds / 3600)))
        area_label = str(job_record.get("area") or "your selected area")
        plural = "" if ttl_hours == 1 else "s"
        subject = "Your City Gig Scraper CSV is ready"
        text_body = (
            f"Hi,\n\n"
            f"Your City Gig Scraper job for {area_label} is complete.\n"
            f"Download CSV: {download_url}\n\n"
            f"The link stays active for about {ttl_hours} hour{plural}.\n"
            "Thanks for using City Gig Scraper!"
        )
        html_body = f"""
        <p>Hi,</p>
        <p>Your City Gig Scraper job for <strong>{area_label}</strong> is complete.</p>
        <p><a href="{download_url}">Download your CSV</a></p>
        <p>The link stays active for about {ttl_hours} hour{plural}.</p>
        <p>Thanks for using City Gig Scraper!</p>
        """
        try:
            send_result_email(notify_email_value, subject, text_body, html_body)
            job_record["email_sent_at"] = time.time()
        except Exception as exc:
            logger.warning("Failed to send completion email to %s: %s", notify_email_value, exc)
            job_record["email_error"] = str(exc)


def _run_job_sync(*, job_id: str, **kwargs) -> None:
    with _job_slot():
        job = JOBS.get(job_id)
        if not job or job.get("status") == "cancelled":
            return
        job["status"] = "running"
        job["started_at"] = time.time()
        _increment_jobs_started(1)
        try:
            asyncio.run(_execute_job(job_id=job_id, **kwargs))
        except MemoryError:  # pragma: no cover
            job = JOBS.get(job_id)
            if job:
                job["status"] = "error"
                job["error"] = "Scrape failed: out of memory. Please narrow the search."
            return


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    pill_markup = "\n          ".join(
        f'<button type="button" class="amenity-pill{" selected" if slug in DEFAULT_AMENITY_SELECTION else ""}" data-amenity="{slug}">{emoji} {label}</button>'
        for slug, emoji, label in AMENITY_OPTIONS
    )
    city_options = "\n          ".join(
        f'<option value="{city}"></option>' for city in CITY_SUGGESTIONS
    )
    city_suggestions_json = json.dumps(CITY_SUGGESTIONS)
    return f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>City Gig Scraper</title>
    <style>
      body {{
        font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        margin: 2rem auto;
        max-width: 880px;
        line-height: 1.5;
        display: flex;
        flex-direction: column;
        min-height: 100vh;
        padding-bottom: 3.25rem; /* keep content visible above fixed footer */
      }}
      label {{
        display: block;
        font-weight: 600;
        margin-top: 1.25rem;
      }}
      input[type="text"],
      input[type="number"],
      input[type="email"],
      select {{
        width: 100%;
        padding: 0.55rem;
        border-radius: 6px;
        border: 1px solid #d0d7de;
      }}
      #notify-email-input {{
        max-width: 250px;
      }}
      .row {{
        display: flex;
        gap: 1rem;
        flex-wrap: wrap;
      }}
      .row > div {{
        flex: 1;
        min-width: 180px;
      }}
      button {{
        margin-top: 1.5rem;
        padding: 0.7rem 1.4rem;
        border-radius: 999px;
        background: #111827;
        color: #ffffff;
        border: none;
        font-weight: 600;
        cursor: pointer;
      }}
      button:hover {{
        background: #1f2937;
      }}
      .checkbox {{
        display: flex;
        gap: 0.75rem;
        align-items: center;
        margin-top: 1.5rem;
      }}
      .hint {{
        color: #4b5563;
        font-size: 0.9rem;
      }}
      .error-text {{
        color: #dc2626;
        font-size: 0.85rem;
        margin-top: 0.35rem;
      }}
      .has-error input {{
        border-color: #dc2626;
      }}
      .amenities-section.has-error {{
        outline: 2px solid #dc2626;
        outline-offset: 4px;
        border-radius: 8px;
        padding: 0.35rem;
      }}
      .area-autocomplete {{
        position: relative;
        margin-top: 0.75rem;
      }}
      .area-autocomplete input {{
        margin-top: 0;
      }}
      .area-suggestions {{
        position: absolute;
        top: calc(100% + 6px);
        left: 0;
        right: 0;
        background: #ffffff;
        color: #111827;
        border: 1px solid #d0d7de;
        border-radius: 12px;
        box-shadow: 0 18px 30px rgba(15, 23, 42, 0.18);
        z-index: 30;
        max-height: 240px;
        overflow-y: auto;
        padding: 0.35rem 0 0.35rem 0; /* small space above; keep a bit of space below */
      }}
      .area-suggestions[hidden] {{
        display: none;
      }}
      .area-suggestion {{
        width: 100%;
        border: none;
        background: transparent;
        padding: 0.55rem 0.95rem;
        margin: 0;
        text-align: left;
        display: flex;
        align-items: center;
        gap: 0.5rem;
        cursor: pointer;
        font-size: 0.95rem;
        color: #111827;
      }}
      .area-suggestion:hover,
      .area-suggestion:focus {{
        background: #eef2ff;
        outline: none;
        color: #111827;
      }}
      .area-suggestion span {{
        display: block;
      }}
      .amenities-section {{
        margin-top: 1.5rem;
      }}
      .amenity-pills {{
        display: flex;
        flex-wrap: wrap;
        gap: 0.6rem;
        margin-top: 0.75rem;
      }}
      .amenity-pill {{
        border: 1px solid #d0d7de;
        background: #f8fafc;
        border-radius: 999px;
        padding: 0.45rem 0.9rem;
        cursor: pointer;
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        font-weight: 600;
        color: #58606b; /* softer 80%-ish gray for unselected */
        transition: all 0.15s ease-in-out;
      }}
      .amenity-pill:hover {{
        border-color: #94a3b8;
        color: #0c0d0d;
        background: #e2e8f0;
      }}
      .amenity-pill.selected {{
        background: #2563eb;
        color: #ffffff;
        border-color: #2563eb;
      }}
      .amenity-actions {{
        margin-top: 0.5rem;
      }}
      .amenity-actions button {{
        border: none;
        background: transparent;
        color: #6b7280; /* gray-500 */
        padding: 0.1rem 0;
        cursor: pointer;
        font-weight: 600;
        text-decoration: none;
      }}
      .amenity-actions button:hover {{
        text-decoration: underline;
        color: #374151; /* gray-700 */
      }}
      a {{
        color: #2563eb;
        text-decoration: none;
      }}
      a:hover {{
        text-decoration: underline;
      }}
      @media (max-width: 768px) {{
        body {{
          padding-left: 1rem;
          padding-right: 1rem;
        }}
      }}
    </style>
  </head>
  <body>
    <h1>City Gig Scraper</h1>
    <p class="hint">
      Are you looking for a job as a barista, hostess, bartender, waiter, cook, or in any other service role?
      This tool searches cafes, restaurants, bars, and related venues for hiring pages. Originally built for Berlin, but should work worldwide.
    </p>
    <form method="post" action="/run">
      <label>Location</label>
      <div class="area-autocomplete" id="area-field">
        <input
          type="text"
          id="area-input"
          name="area"
          value=""
          placeholder="Start typing a city (e.g., Berlin, Tokyo, New York)"
          autocomplete="off"
        />
        <div class="area-suggestions" id="area-suggestions" hidden></div>
      </div>
      <div id="area-error" class="error-text" style="display:none;"></div>
      <div class="amenities-section">
        <input type="hidden" name="amenities" id="amenities-input" value="{DEFAULT_AMENITY_STRING}" />
        <div class="amenity-pills">
          {pill_markup}
        </div>
      <div class="amenity-actions">
          <button type="button" id="select-all-amenities" data-mode="select">Select all amenities</button>
        </div>
      </div>
      <div id="amenities-error" class="error-text" style="display:none;"></div>
      <!-- splitting is adaptive; no UI control -->

    <div class="run-controls">
        <button type="submit" id="run-button" data-state="idle">Run scrape</button>
        <span id="run-status" class="hint" style="margin-left: 0.75rem;"></span>
      </div>
      <div id="notify-email-wrapper" style="margin-top:1.1rem; display:none;">
        <label id="notify-email-field" style="display:block;font-weight:400;color:#667085;font-size:0.95rem;">
          This might take a while—drop your email and we'll send you the CSV when we're done.
          <div style="display:flex;gap:0.6rem;align-items:center;margin-top:0.4rem;">
            <input
              type="email"
              name="notify_email"
              id="notify-email-input"
              placeholder="you@example.com"
            />
            <button type="button" id="notify-email-confirm" style="margin:0;padding:0.55rem 0.95rem;" aria-label="Confirm email">✔</button>
          </div>
        </label>
        <div id="notify-email-error" class="error-text" style="display:none;"></div>
        <div id="notify-email-confirmed" class="hint" style="margin-top:0.4rem;display:none;"></div>
        <div id="notify-email-actions" style="margin-top:0.4rem;display:none;">
          <button type="button" id="notify-email-edit" style="margin-right:0.6rem;">Edit</button>
          <button type="button" id="notify-email-delete">Remove</button>
        </div>
      </div>
      <div id="preview-container" style="margin-top: 1.25rem;" hidden>
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem;">
          <strong>Preview (first 50 rows)</strong>
          <a id="download-link" href="#" download class="download-link" style="display:none;">Download CSV</a>
        </div>
        <div id="preview-table" style="overflow:auto;border:1px solid #e5e7eb;border-radius:8px;"></div>
      </div>
    </form>
    <script type="application/json" id="city-suggestions-data">{city_suggestions_json}</script>
    <script>
      const citySuggestions = JSON.parse(document.getElementById("city-suggestions-data").textContent || "[]");
      const areaInput = document.getElementById("area-input");
      const areaSuggestionsBox = document.getElementById("area-suggestions");
      const AREA_DEBOUNCE_MS = 120;
      const AREA_MIN_QUERY = 1;
      const AREA_MAX_RESULTS = 8;
      let areaDebounceTimer = null;

      function clearAreaSuggestions() {{
        if (!areaSuggestionsBox) {{
          return;
        }}
        areaSuggestionsBox.innerHTML = "";
        areaSuggestionsBox.hidden = true;
      }}

      function renderAreaSuggestions(matches) {{
        if (!areaSuggestionsBox) {{
          return;
        }}

        areaSuggestionsBox.innerHTML = "";

        matches.slice(0, AREA_MAX_RESULTS).forEach((city) => {{
          const button = document.createElement("button");
          button.type = "button";
          button.className = "area-suggestion";
          button.textContent = city;
          button.addEventListener("click", () => {{
            areaInput.value = city;
            clearAreaSuggestions();
            areaInput.focus();
          }});
          areaSuggestionsBox.appendChild(button);
        }});

        areaSuggestionsBox.hidden = matches.length === 0;
      }}

      function handleAreaLookup() {{
        if (!areaInput) {{
          return;
        }}
        const query = areaInput.value.trim();
        if (query.length < AREA_MIN_QUERY) {{
          clearAreaSuggestions();
          return;
        }}
        const lowered = query.toLowerCase();
        const matches = citySuggestions.filter((city) => city.toLowerCase().startsWith(lowered));
        renderAreaSuggestions(matches);
      }}

      if (areaInput) {{
        areaInput.addEventListener("input", () => {{
          if (areaDebounceTimer) {{
            clearTimeout(areaDebounceTimer);
          }}
          areaDebounceTimer = setTimeout(handleAreaLookup, AREA_DEBOUNCE_MS);
        }});

        areaInput.addEventListener("focus", handleAreaLookup);

        areaInput.addEventListener("blur", () => {{
          setTimeout(clearAreaSuggestions, 180);
        }});

        document.addEventListener("keydown", (event) => {{
          if (!areaSuggestionsBox || areaSuggestionsBox.hidden) {{
            return;
          }}
          const focusable = Array.from(areaSuggestionsBox.querySelectorAll("button.area-suggestion"));
          if (!focusable.length) {{
            return;
          }}
          const currentIndex = focusable.findIndex((el) => el === document.activeElement);

          if (event.key === "ArrowDown") {{
            event.preventDefault();
            const nextIndex = (currentIndex + 1) % focusable.length;
            focusable[nextIndex].focus();
          }} else if (event.key === "ArrowUp") {{
            event.preventDefault();
            const prevIndex = (currentIndex - 1 + focusable.length) % focusable.length;
            focusable[prevIndex].focus();
          }} else if (event.key === "Enter" && document.activeElement?.classList.contains("area-suggestion")) {{
            event.preventDefault();
            document.activeElement.click();
          }}
        }});
      }}

      const hiddenAmenities = document.getElementById("amenities-input");
      const amenityPills = Array.from(document.querySelectorAll(".amenity-pill"));
      const selectAllBtn = document.getElementById("select-all-amenities");
      const formEl = document.querySelector("form");
      const runButton = document.getElementById("run-button");
      const runStatus = document.getElementById("run-status");
      const previewContainer = document.getElementById("preview-container");
      const previewTable = document.getElementById("preview-table");
      const downloadLink = document.getElementById("download-link");
      const areaField = document.getElementById("area-field");
      const areaError = document.getElementById("area-error");
      const amenitiesField = document.querySelector(".amenities-section");
      const amenitiesError = document.getElementById("amenities-error");
      const notifyWrapper = document.getElementById("notify-email-wrapper");
      const notifyEmailInput = document.getElementById("notify-email-input");
      const notifyEmailField = document.getElementById("notify-email-field");
      const notifyEmailError = document.getElementById("notify-email-error");
      const notifyEmailConfirm = document.getElementById("notify-email-confirm");
      const notifyEmailConfirmed = document.getElementById("notify-email-confirmed");
      const notifyEmailActions = document.getElementById("notify-email-actions");
      const notifyEmailEdit = document.getElementById("notify-email-edit");
      const notifyEmailDelete = document.getElementById("notify-email-delete");
      const jobsStartedEl = document.getElementById("jobs-started");
      let currentJobId = null;
      let pollTimer = null;
      let notifyConfigured = false;
      let confirmedEmailValue = "";

      async function refreshJobsStarted() {{
        try {{
          const el = document.getElementById("jobs-started");
          const res = await fetch("/stats");
          const data = await res.json();
          if (el && data && typeof data.jobs_started === "number") {{
            el.textContent = `Uses: ${{data.jobs_started}}`;
          }}
        }} catch (e) {{
          // ignore
        }}
      }}

      function updateHiddenAmenities() {{
        const selectedAmenities = amenityPills
          .filter((pill) => pill.classList.contains("selected"))
          .map((pill) => pill.dataset.amenity);
        hiddenAmenities.value = selectedAmenities.join(",");
      }}

      function toggleSelectAllButtonLabel() {{
        const hasUnselected = amenityPills.some((pill) => !pill.classList.contains("selected"));
        if (hasUnselected) {{
          selectAllBtn.textContent = "Select all categories";
          selectAllBtn.dataset.mode = "select";
        }} else {{
          selectAllBtn.textContent = "Deselect all categories";
          selectAllBtn.dataset.mode = "deselect";
        }}
      }}

      amenityPills.forEach((pill) => {{
        pill.addEventListener("click", () => {{
          pill.classList.toggle("selected");
          updateHiddenAmenities();
          toggleSelectAllButtonLabel();
        }});
      }});

      if (selectAllBtn) {{
        selectAllBtn.addEventListener("click", (event) => {{
          event.preventDefault();
          const shouldSelectAll = selectAllBtn.dataset.mode !== "deselect";
          amenityPills.forEach((pill) => pill.classList.toggle("selected", shouldSelectAll));
          updateHiddenAmenities();
          toggleSelectAllButtonLabel();
        }});
      }}

      updateHiddenAmenities();
      toggleSelectAllButtonLabel();

      function setRunState(state) {{
        runButton.dataset.state = state;
        if (state === "idle") {{
          runButton.textContent = "Run scrape";
        }} else if (state === "running") {{
          runButton.textContent = "Cancel scrape";
        }} else if (state === "cancelling") {{
          runButton.textContent = "Cancelling…";
        }}
      }}

      function showFieldError(containerEl, errorEl, message) {{
        if (containerEl) containerEl.classList.add("has-error");
        if (errorEl) {{
          errorEl.textContent = message || "";
          errorEl.style.display = message ? "block" : "none";
        }}
      }}

      function clearFieldError(containerEl, errorEl) {{
        if (containerEl) containerEl.classList.remove("has-error");
        if (errorEl) {{
          errorEl.textContent = "";
          errorEl.style.display = "none";
        }}
      }}

      function escapeHtml(s) {{ return (s ?? "").toString().replace(/[&<>]/g, m => ({{"&":"&amp;","<":"&lt;",">":"&gt;"}}[m])); }}
      function normalizeKey(k) {{ return (k || "").toString().trim().toLowerCase(); }}
      function displayHeader(col) {{
        const key = normalizeKey(col);
        if (key === "job_page_url" || key === "hiring_page_url" || key === "hiring_page") {{
          return "Hiring Page";
        }}
        return col;
      }}
      function renderCell(col, value) {{
        const key = normalizeKey(col);
        const val = (value ?? "").toString().trim();
        if ((key === "job_page_url" || key === "hiring_page_url" || key === "hiring_page") && val) {{
          const safeUrl = val.replace(/\\"/g, "&quot;");
          return `<a href="${{safeUrl}}" target="_blank" rel="noopener noreferrer">${{escapeHtml(val)}}</a>`;
        }}
        return escapeHtml(val);
      }}
      function renderPreview(rows) {{
        if (!rows || !rows.length) {{
          previewTable.innerHTML = "<div class='hint' style='padding:0.6rem;'>No rows.</div>";
          return;
        }}
        const cols = Object.keys(rows[0]);
        let html = "<table style='width:100%;border-collapse:collapse;font-size:0.95rem;'>";
        html += "<thead><tr>";
        cols.forEach(c => {{
          html += `<th style="text-align:left;border-bottom:1px solid #e5e7eb;padding:0.5rem;">${{displayHeader(c)}}</th>`;
        }});
        html += "</tr></thead><tbody>";
        rows.forEach(r => {{
          html += "<tr>";
          cols.forEach(c => {{
            html += `<td style="border-bottom:1px solid #f1f5f9;padding:0.5rem;vertical-align:top;">${{renderCell(c, r[c])}}</td>`;
          }});
          html += "</tr>";
        }});
        html += "</tbody></table>";
        previewTable.innerHTML = html;
      }}

      async function pollStatus() {{
        if (!currentJobId) return;
        try {{
          const res = await fetch(`/status_json/${{currentJobId}}`);
          const data = await res.json();
          if (data.error === "unknown_job") {{
            runStatus.textContent = "Job Cancelled.";
            setRunState("idle");
            return;
          }}
          if (data.status === "queued") {{
            if (typeof data.queue_position === "number" && typeof data.queue_length === "number") {{
              runStatus.textContent = `Queued… waiting for an available slot (${{data.queue_position}}/${{data.queue_length}}).`;
            }} else {{
              runStatus.textContent = "Queued… waiting for an available slot.";
            }}
            return;
          }}
          if (data.status === "running" && data.phase === "listing") {{
            runStatus.textContent = "Listing places…";
          }} else if (data.status === "running" && data.phase === "scanning") {{
            const total = data.total ?? 0;
            const processed = data.processed ?? 0;
            const found = data.found ?? 0;
            runStatus.textContent = `Scanned ${{processed}}/${{total}} places, found ${{found}} hiring pages.`;
          }} else if (data.status === "done") {{
            runStatus.textContent = "Done.";
            setRunState("idle");
            clearInterval(pollTimer);
            pollTimer = null;
            previewContainer.hidden = false;
            renderPreview(data.preview || []);
            if (data.download_url) {{
              downloadLink.href = data.download_url;
              downloadLink.style.display = "inline-block";
            }}
            if (data.email_sent) {{
              const msg = confirmedEmailValue
                ? "Successfully sent to " + confirmedEmailValue
                : "Download link sent successfully.";
              showEmailMessage(msg, false);
            }} else if (data.email_failed) {{
              const msg = confirmedEmailValue
                ? "Failed to send email to " + confirmedEmailValue
                : "Failed to send email notification.";
              showEmailMessage(msg, true);
            }} else if (data.notify_set) {{
              const msg = confirmedEmailValue
                ? "Notification set. We'll email " + confirmedEmailValue + " when results are ready."
                : "Notification set.";
              showEmailMessage(msg, false);
            }} else {{
              hideEmailUI();
            }}
            return;
          }} else if (data.status === "cancelled") {{
            runStatus.textContent = "Cancelled.";
            setRunState("idle");
            clearInterval(pollTimer);
            pollTimer = null;
            // Show results if available (partial results from cancelled scan)
            if (data.download_url) {{
              previewContainer.hidden = false;
              renderPreview(data.preview || []);
              downloadLink.href = data.download_url;
              downloadLink.style.display = "inline-block";
            }}
            hideEmailUI();
            return;
          }} else if (data.status === "error") {{
            runStatus.textContent = "Error: " + (data.error || "");
            setRunState("idle");
            clearInterval(pollTimer);
            pollTimer = null;
            return;
          }}
        }} catch (e) {{
          // transient fetch issue; keep polling
        }}
      }}

      // Live validation
      areaInput.addEventListener("input", () => {{
        if (areaInput.value.trim()) {{
          clearFieldError(areaField, areaError);
        }}
      }});

      if (notifyEmailInput) {{
        notifyEmailInput.addEventListener("input", () => {{
          if (notifyEmailField) {{
            clearFieldError(notifyEmailField, notifyEmailError);
          }}
        }});
        // Pressing Enter in the email field should confirm email, not submit/cancel the form
        notifyEmailInput.addEventListener("keydown", (e) => {{
          if (e.key === "Enter") {{
            e.preventDefault();
            e.stopPropagation();
            if (notifyEmailConfirm) {{
              notifyEmailConfirm.click();
            }}
          }}
        }});
      }}

      function validateAmenities() {{
        const val = hiddenAmenities.value.trim();
        if (!val) {{
          showFieldError(amenitiesField, amenitiesError, "Please select at least one category.");
          return false;
        }}
        clearFieldError(amenitiesField, amenitiesError);
        return true;
      }}

      amenityPills.forEach((pill) => {{
        pill.addEventListener("click", () => {{
          validateAmenities();
        }});
      }});

      if (selectAllBtn) {{
        selectAllBtn.addEventListener("click", () => {{
          validateAmenities();
        }});
      }}

      formEl.addEventListener("submit", async (e) => {{
        e.preventDefault();

        // Client-side validation
        let valid = true;
        if (!areaInput.value.trim()) {{
          showFieldError(areaField, areaError, "Please enter a city.");
          areaInput.focus();
          valid = false;
        }} else {{
          clearFieldError(areaField, areaError);
        }}
        if (!validateAmenities()) {{
          valid = false;
        }}
        if (notifyEmailInput) {{
          const emailVal = notifyEmailInput.value.trim();
          if (emailVal && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(emailVal)) {{
            showFieldError(notifyEmailField, notifyEmailError, "Please enter a valid email address.");
            valid = false;
          }} else {{
            clearFieldError(notifyEmailField, notifyEmailError);
          }}
        }}
        if (!valid) {{
          return;
        }}

        if (runButton.dataset.state === "running" && currentJobId) {{
          setRunState("cancelling");
          try {{
            await fetch(`/cancel/${{currentJobId}}`, {{ method: "POST" }});
          }} finally {{
            // status poller will pick up 'cancelled'
          }}
          hideEmailUI();
          return;
        }}
        // start new job
        previewContainer.hidden = true;
        downloadLink.style.display = "none";
        runStatus.textContent = "";
        hideEmailUI();
        notifyConfigured = false;
        try {{
          const formData = new FormData(formEl);
          const res = await fetch("/run", {{ method: "POST", body: formData }});
          if (!res.ok) {{
            const data = await res.json().catch(() => ({{}}));
            if (data && data.error === "validation") {{
              if (data.area) showFieldError(areaField, areaError, data.area);
              if (data.amenities) showFieldError(amenitiesField, amenitiesError, data.amenities);
              if (data.notify_email) showFieldError(notifyEmailField, notifyEmailError, data.notify_email);
              setRunState("idle");
              return;
            }}
            throw new Error("Start failed");
          }}
          const data = await res.json();
          currentJobId = data.job_id;
          setRunState("running");
          runStatus.textContent = "Queued… waiting for an available slot.";
          if (notifyWrapper) {{
            showEmailInputState();
          }}
          // Refresh footer counter after a successful start
          refreshJobsStarted();
          if (pollTimer) clearInterval(pollTimer);
          pollTimer = setInterval(pollStatus, 800);
        }} catch (err) {{
          runStatus.textContent = "Failed to start.";
          setRunState("idle");
        }}
      }});

      function hideEmailUI() {{
        if (notifyWrapper) notifyWrapper.style.display = "none";
        if (notifyEmailField) notifyEmailField.classList.remove("has-error");
        if (notifyEmailError) {{
          notifyEmailError.textContent = "";
          notifyEmailError.style.display = "none";
        }}
        if (notifyEmailInput) notifyEmailInput.style.display = "";
        if (notifyEmailConfirm) {{
          notifyEmailConfirm.style.display = "";
          notifyEmailConfirm.disabled = false;
          notifyEmailConfirm.textContent = "✔";
        }}
        if (notifyEmailConfirmed) {{
          notifyEmailConfirmed.textContent = "";
          notifyEmailConfirmed.style.display = "none";
        }}
        if (notifyEmailActions) notifyEmailActions.style.display = "none";
      }}

      function showEmailInputState() {{
        if (!notifyWrapper) return;
        notifyWrapper.style.display = "block";
        if (notifyEmailField) notifyEmailField.classList.remove("has-error");
        if (notifyEmailError) {{
          notifyEmailError.textContent = "";
          notifyEmailError.style.display = "none";
        }}
        if (confirmedEmailValue) {{
          if (notifyEmailInput) notifyEmailInput.style.display = "none";
          if (notifyEmailConfirm) {{
            notifyEmailConfirm.style.display = "none";
            notifyEmailConfirm.disabled = false;
            notifyEmailConfirm.textContent = "✔";
          }}
          if (notifyEmailConfirmed) {{
            notifyEmailConfirmed.textContent = "We'll send a download link to " + confirmedEmailValue;
            notifyEmailConfirmed.style.display = "block";
          }}
          if (notifyEmailActions) notifyEmailActions.style.display = "block";
        }} else {{
          if (notifyEmailInput) {{
            notifyEmailInput.style.display = "";
            notifyEmailInput.value = "";
          }}
          if (notifyEmailConfirm) {{
            notifyEmailConfirm.style.display = "";
            notifyEmailConfirm.disabled = false;
            notifyEmailConfirm.textContent = "✔";
          }}
          if (notifyEmailConfirmed) {{
            notifyEmailConfirmed.textContent = "";
            notifyEmailConfirmed.style.display = "none";
          }}
          if (notifyEmailActions) notifyEmailActions.style.display = "none";
        }}
      }}

      function showEmailMessage(message, isError = false) {{
        if (!notifyWrapper) return;
        notifyWrapper.style.display = "block";
        if (notifyEmailField) notifyEmailField.classList.toggle("has-error", isError);
        if (notifyEmailError) {{
          notifyEmailError.textContent = "";
          notifyEmailError.style.display = "none";
        }}
        if (notifyEmailInput) notifyEmailInput.style.display = "none";
        if (notifyEmailConfirm) notifyEmailConfirm.style.display = "none";
        if (notifyEmailActions) notifyEmailActions.style.display = "none";
        if (notifyEmailConfirmed) {{
          notifyEmailConfirmed.textContent = message;
          notifyEmailConfirmed.style.display = message ? "block" : "none";
        }}
      }}

      if (notifyEmailConfirm) {{
        notifyEmailConfirm.addEventListener("click", async () => {{
          if (!notifyEmailInput) {{
            return;
          }}
          const emailVal = notifyEmailInput.value.trim();
          if (!emailVal) {{
            showFieldError(notifyEmailField, notifyEmailError, "Please enter an email address.");
            return;
          }}
          if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(emailVal)) {{
            showFieldError(notifyEmailField, notifyEmailError, "Please enter a valid email address.");
            return;
          }}
          if (!currentJobId) {{
            showFieldError(notifyEmailField, notifyEmailError, "Start a scrape first.");
            return;
          }}
          clearFieldError(notifyEmailField, notifyEmailError);
          notifyEmailConfirm.disabled = true;
          notifyEmailConfirm.textContent = "Saving…";
          try {{
            const res = await fetch(`/set_email/${{currentJobId}}`, {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json"
              }},
              body: JSON.stringify({{ notify_email: emailVal }})
            }});
            const data = await res.json().catch(() => ({{}}));
            if (!res.ok || (data && data.error)) {{
              const message = (data && data.error) || "Could not save email.";
              showFieldError(notifyEmailField, notifyEmailError, message);
              notifyEmailConfirm.disabled = false;
              notifyEmailConfirm.textContent = "✔";
              return;
            }}
            confirmedEmailValue = emailVal;
            notifyConfigured = true;
            showEmailInputState();
            return;
          }} catch (err) {{
            showFieldError(notifyEmailField, notifyEmailError, "Could not save email.");
            notifyEmailConfirm.disabled = false;
            notifyEmailConfirm.textContent = "✔";
          }}
        }});
      }}

      if (notifyEmailEdit) {{
        notifyEmailEdit.addEventListener("click", () => {{
          if (!notifyEmailInput) {{
            return;
          }}
          notifyConfigured = false;
          if (notifyEmailInput) {{
            notifyEmailInput.style.display = "";
            notifyEmailInput.value = confirmedEmailValue;
          }}
          if (notifyEmailConfirm) {{
            notifyEmailConfirm.style.display = "";
            notifyEmailConfirm.disabled = false;
            notifyEmailConfirm.textContent = "✔";
          }}
          if (notifyEmailActions) {{
            notifyEmailActions.style.display = "none";
          }}
          if (notifyEmailConfirmed) {{
            notifyEmailConfirmed.textContent = "";
            notifyEmailConfirmed.style.display = "none";
          }}
        }});
      }}

      if (notifyEmailDelete) {{
        notifyEmailDelete.addEventListener("click", async () => {{
          if (!currentJobId) {{
            showFieldError(notifyEmailField, notifyEmailError, "Start a scrape first.");
            return;
          }}
          notifyEmailDelete.disabled = true;
          try {{
            const res = await fetch(`/set_email/${{currentJobId}}`, {{
              method: "DELETE"
            }});
            const data = await res.json().catch(() => ({{}}));
            if (!res.ok || (data && data.error)) {{
              const message = (data && data.error) || "Could not remove email.";
              showFieldError(notifyEmailField, notifyEmailError, message);
              notifyEmailDelete.disabled = false;
              return;
            }}
            confirmedEmailValue = "";
            notifyConfigured = false;
            showEmailInputState();
            notifyEmailDelete.disabled = false;
          }} catch (err) {{
            showFieldError(notifyEmailField, notifyEmailError, "Could not remove email.");
            notifyEmailDelete.disabled = false;
          }}
        }});
      }}
      // Initial footer counter
      window.addEventListener("load", refreshJobsStarted);
    </script>
    <footer style="position:fixed;left:0;right:0;bottom:0;padding:0.6rem 1rem;border-top:1px solid #e5e7eb;background:#ffffff;color:#6b7280;font-size:0.9rem;">
      <div style="max-width:880px;margin:0 auto;display:flex;justify-content:space-between;align-items:center;">
        <div>
          <span>Source code on </span>
          <a href="https://github.com/novaheic/city-gig-scraper" target="_blank" rel="noopener" style="color:#2563eb;">
            GitHub
          </a>
        </div>
        <div id="jobs-started" class="hint">Jobs started: –</div>
      </div>
    </footer>
  </body>
</html>
"""


@app.post("/run")
def run(
    request: Request,
    background: BackgroundTasks,
    area: str = Form(DEFAULT_AREA),
    amenities: str = Form(DEFAULT_AMENITY_STRING),
    notify_email: str | None = Form(None),
):
    # Server-side validation
    if not (area and area.strip()):
        return JSONResponse({"error": "validation", "area": "Please enter a city."}, status_code=400)
    if not (amenities and amenities.strip()):
        return JSONResponse({"error": "validation", "amenities": "Please select at least one amenity."}, status_code=400)

    email_value: str | None = notify_email.strip() if notify_email else None
    if email_value and not _is_valid_email(email_value):
        return JSONResponse({"error": "validation", "notify_email": "Please enter a valid email address."}, status_code=400)

    job_id = uuid.uuid4().hex[:10]
    output_path = OUTPUT_DIR / f"{job_id}.csv"

    JOBS[job_id] = {
        "status": "queued",
        "output": str(output_path),
        "created_at": time.time(),
        "queued_at": time.time(),
    }
    JOBS[job_id]["area"] = area
    JOBS[job_id]["amenities"] = amenities
    JOBS[job_id]["base_url"] = str(request.base_url)
    if email_value:
        JOBS[job_id]["notify_email"] = email_value
    ttl_seconds = JOB_TTL_SECONDS
    if email_value:
        ttl_seconds = max(ttl_seconds, EMAIL_JOB_TTL_SECONDS)
    JOBS[job_id]["ttl_seconds"] = ttl_seconds

    background.add_task(
        _run_job_sync,
        job_id=job_id,
        area=area,
        amenities=amenities,
        user_agent=UI_USER_AGENT,
        concurrency=UI_CONCURRENCY,
        max_job_links=UI_MAX_JOB_LINKS,
        crawl_depth=UI_CRAWL_DEPTH,
        log_level=UI_LOG_LEVEL,
        overpass_url=UI_OVERPASS_URL,
        limit=UI_LIMIT,
        overpass_urls=UI_OVERPASS_URLS,
    )
    return {"job_id": job_id}

@app.post("/set_email/{job_id}")
def set_email(job_id: str, payload: dict = Body(...)):
    job = JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "Unknown job."}, status_code=404)

    email_value = str(payload.get("notify_email", "") or "").strip()
    if not email_value or not _is_valid_email(email_value):
        return JSONResponse({"error": "Please enter a valid email address."}, status_code=400)

    status = str(job.get("status", ""))
    if status == "cancelled":
        return JSONResponse({"error": "Job was cancelled."}, status_code=400)

    job["notify_email"] = email_value
    ttl_seconds = float(job.get("ttl_seconds", JOB_TTL_SECONDS) or JOB_TTL_SECONDS)
    if ttl_seconds < EMAIL_JOB_TTL_SECONDS:
        job["ttl_seconds"] = EMAIL_JOB_TTL_SECONDS

    # ensure base_url exists (fallback to empty string)
    job.setdefault("base_url", "")

    if status == "done":
        try:
            download_url = _build_download_url(job_id, job)
            ttl_hours = max(1, int(round(float(job.get("ttl_seconds", EMAIL_JOB_TTL_SECONDS) or EMAIL_JOB_TTL_SECONDS) / 3600)))
            area_label = str(job.get("area") or "your selected area")
            plural = "" if ttl_hours == 1 else "s"
            subject = "Your City Gig Scraper CSV is ready"
            text_body = (
                f"Hi,\n\n"
                f"Your City Gig Scraper job for {area_label} is complete.\n"
                f"Download CSV: {download_url}\n\n"
                f"The link stays active for about {ttl_hours} hour{plural}.\n"
                "Thanks for using City Gig Scraper!"
            )
            html_body = f"""
            <p>Hi,</p>
            <p>Your City Gig Scraper job for <strong>{area_label}</strong> is complete.</p>
            <p><a href=\"{download_url}\">Download your CSV</a></p>
            <p>The link stays active for about {ttl_hours} hour{plural}.</p>
            <p>Thanks for using City Gig Scraper!</p>
            """
            send_result_email(email_value, subject, text_body, html_body)
            job["email_sent_at"] = time.time()
            job.pop("email_error", None)
        except Exception as exc:  # pragma: no cover
            job["email_error"] = str(exc)
            return JSONResponse({"error": f"Failed to send email: {exc}"}, status_code=500)

    return {"ok": True}


@app.delete("/set_email/{job_id}")
def delete_email(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "Unknown job."}, status_code=404)

    status = str(job.get("status", ""))
    if status == "cancelled":
        return JSONResponse({"error": "Job was cancelled."}, status_code=400)

    job.pop("notify_email", None)
    job.pop("email_error", None)
    job.pop("email_sent_at", None)
    # Revert TTL to default if it was extended for email
    job["ttl_seconds"] = JOB_TTL_SECONDS

    return {"ok": True}

@app.get("/stats")
def stats():
    return {"jobs_started": _get_jobs_started()}


@app.head("/stats")
def stats_head():
    return stats()


@app.get("/status/{job_id}", response_class=HTMLResponse)
def status(job_id: str):
    info = JOBS.get(job_id)
    if not info:
        return HTMLResponse("<p>Unknown job.</p>", status_code=404)

    if info["status"] == "running":
        return f"""
        <p>Job {job_id} running...</p>
        <p><a href="/status/{job_id}">Refresh</a></p>
        """

    if info["status"] == "error":
        error_message = info.get("error", "Unknown error")
        return HTMLResponse(
            f"<p>Job {job_id} failed.</p><pre>{error_message}</pre>", status_code=500
        )

    download_url = f"/download/{job_id}"
    return f"""
    <p>Job {job_id} complete.</p>
    <p><a href="{download_url}">Download CSV</a></p>
    """


@app.get("/status_json/{job_id}")
def status_json(job_id: str):
    info = JOBS.get(job_id)
    if not info:
        return {"error": "unknown_job"}
    payload = {
        "job_id": job_id,
        "status": info.get("status"),
        "phase": info.get("phase"),
        "total": info.get("total"),
        "processed": info.get("processed"),
        "found": info.get("found"),
        "notify_set": bool(info.get("notify_email")),
        "email_sent": bool(info.get("email_sent_at")),
        "email_failed": bool(info.get("email_error")),
        "error": info.get("error"),
    }
    if info.get("status") == "queued":
        queued_jobs = sorted(
            (
                (jid, data)
                for jid, data in JOBS.items()
                if data.get("status") == "queued"
            ),
            key=lambda item: float(item[1].get("queued_at") or item[1].get("created_at") or 0.0),
        )
        for idx, (queued_id, _) in enumerate(queued_jobs, 1):
            if queued_id == job_id:
                payload["queue_position"] = idx
                break
        payload["queue_length"] = len(queued_jobs)
    if info.get("status") == "done":
        path = Path(str(info.get("output", "")))
        payload["download_url"] = f"/download/{job_id}" if path.exists() else None
        payload["preview"] = _ensure_preview(info)
    elif info.get("status") == "cancelled":
        path = Path(str(info.get("output", "")))
        if path.exists():
            payload["download_url"] = f"/download/{job_id}"
        payload["preview"] = _ensure_preview(info)
    return payload


@app.post("/cancel/{job_id}")
def cancel(job_id: str):
    info = JOBS.get(job_id)
    if not info:
        return {"ok": False, "error": "unknown_job"}
    info["status"] = "cancelled"
    # Check if output file exists and has content (at least one data row)
    has_results = False
    try:
        path = Path(str(info.get("output", "")))
        if path.exists() and path.is_file():
            # Check if file has at least one data row (beyond header)
            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                try:
                    next(reader)  # Try to read first data row
                    has_results = True
                except StopIteration:
                    # File only has header, no data rows
                    has_results = False
    except Exception:
        pass
    
    # Only delete output if it has no results, and only remove job if no results
    if not has_results:
        try:
            path = Path(str(info.get("output", "")))
            if path.exists() and path.is_file():
                path.unlink(missing_ok=True)
        except Exception:
            pass
        # Remove job metadata only if no results
        JOBS.pop(job_id, None)
    else:
        _ensure_preview(info)
    # If has_results is True, keep the job in JOBS so status endpoint can find it
    return {"ok": True}

@app.get("/download/{job_id}")
def download(job_id: str):
    info = JOBS.get(job_id)
    if not info or info.get("status") not in ("done", "cancelled"):
        return PlainTextResponse("Job not ready", status_code=404)

    output_path = Path(info.get("output", ""))
    if not output_path.exists():
        return PlainTextResponse("File missing", status_code=404)

    return FileResponse(
        output_path,
        filename=f"{job_id}.csv",
        media_type="text/csv",
    )


