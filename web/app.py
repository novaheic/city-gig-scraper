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
import time
import json
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, Form
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, RedirectResponse, JSONResponse

from job_scraper.main import DEFAULT_AREA, main as cli_main
from job_scraper.main import _process_place as _main_process_place  # type: ignore
from job_scraper.main import _write_results as _main_write_results  # type: ignore
from job_scraper.main import _deduplicate_by_job_page as _main_dedupe  # type: ignore
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

def _get_jobs_started() -> int:
    return int(_load_stats().get("jobs_started", 0))

def _increment_jobs_started(amount: int = 1) -> int:
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
                if created_at and (now - created_at) > JOB_TTL_SECONDS:
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

UI_USER_AGENT = "JobScraper/0.1 (+https://github.com/novaheic)"
UI_CONCURRENCY = 18
UI_MAX_JOB_LINKS = 6
UI_CRAWL_DEPTH = 3
UI_LIMIT: Optional[int] = None
UI_OVERPASS_URL: Optional[str] = None
UI_LOG_LEVEL = "INFO"

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
) -> None:
    # Phase 1: list places
    JOBS[job_id].update(
        {
            "status": "running",
            "phase": "listing",
            "total": 0,
            "processed": 0,
            "found": 0,
        }
    )
    kwargs: dict[str, object] = {}
    if overpass_url:
        kwargs["overpass_url"] = overpass_url

    try:
        try:
            places = fetch_places(area, amenities.split(","), **kwargs)
        except OverpassError:
            places = fetch_places_by_grid(area, amenities.split(","), **kwargs)
    except Exception as exc:  # pragma: no cover
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"] = f"Discovery failed: {exc}"
        return

    total = len(places)
    JOBS[job_id]["total"] = total
    JOBS[job_id]["phase"] = "scanning"

    # Phase 2: scan with progress
    results = []
    try:
        async with AsyncCrawler(user_agent=user_agent, concurrency=concurrency) as crawler:
            tasks = [
                asyncio.create_task(
                    _main_process_place(
                        place,
                        crawler,
                        max_job_links=max_job_links,
                        crawl_depth=crawl_depth,
                    )
                )
                for place in places
            ]

            for coro in asyncio.as_completed(tasks):
                # Support cancellation signal
                if JOBS.get(job_id, {}).get("status") == "cancelled":
                    for t in tasks:
                        t.cancel()
                    break
                try:
                    res = await coro
                except Exception:
                    # Count as processed even if one task fails
                    res = None
                JOBS[job_id]["processed"] = int(JOBS[job_id].get("processed", 0)) + 1
                if res is not None and getattr(res, "hiring", False):
                    JOBS[job_id]["found"] = int(JOBS[job_id].get("found", 0)) + 1
                    results.append(res)
    except Exception as exc:  # pragma: no cover
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"] = f"Scan failed: {exc}"
        return

    # Phase 3: write CSV and finish
    deduped = _main_dedupe(results)
    out_path = Path(JOBS[job_id]["output"])
    try:
        _main_write_results(out_path, deduped)
    except Exception as exc:  # pragma: no cover
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"] = f"Write failed: {exc}"
        return

    JOBS[job_id]["status"] = "done"


def _run_job_sync(*, job_id: str, **kwargs) -> None:
    asyncio.run(_execute_job(job_id=job_id, **kwargs))


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
      select {{
        width: 100%;
        padding: 0.55rem;
        border-radius: 6px;
        border: 1px solid #d0d7de;
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
      const amenitiesError = document.getElementById("amenities-error");
      const jobsStartedEl = document.getElementById("jobs-started");
      let currentJobId = null;
      let pollTimer = null;

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

      function renderPreview(rows) {{
        if (!rows || !rows.length) {{
          previewTable.innerHTML = "<div class='hint' style='padding:0.6rem;'>No rows.</div>";
          return;
        }}
        const cols = Object.keys(rows[0]);
        let html = "<table style='width:100%;border-collapse:collapse;font-size:0.95rem;'>";
        html += "<thead><tr>";
        cols.forEach(c => {{
          html += `<th style="text-align:left;border-bottom:1px solid #e5e7eb;padding:0.5rem;">${{c}}</th>`;
        }});
        html += "</tr></thead><tbody>";
        rows.forEach(r => {{
          html += "<tr>";
          cols.forEach(c => {{
            html += `<td style="border-bottom:1px solid #f1f5f9;padding:0.5rem;vertical-align:top;">${{(r[c] ?? "").toString()}}</td>`;
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
          if (data.status === "running" && data.phase === "listing") {{
            runStatus.textContent = "Listing places…";
          }} else if (data.status === "running" && data.phase === "scanning") {{
            const total = data.total ?? 0;
            const processed = data.processed ?? 0;
            const found = data.found ?? 0;
            runStatus.textContent = `Scanned ${{processed}}/${{total}} places, found ${{found}} hiring pages - most results appear in the last 1/3 of the scan.`;
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
            return;
          }} else if (data.status === "cancelled") {{
            runStatus.textContent = "Cancelled.";
            setRunState("idle");
            clearInterval(pollTimer);
            pollTimer = null;
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

      function validateAmenities() {{
        const val = hiddenAmenities.value.trim();
        if (!val) {{
          showFieldError(document.querySelector(".amenities-section"), amenitiesError, "Please select at least one category.");
          return false;
        }}
        clearFieldError(document.querySelector(".amenities-section"), amenitiesError);
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
          return;
        }}
        // start new job
        previewContainer.hidden = true;
        downloadLink.style.display = "none";
        runStatus.textContent = "";
        try {{
          const formData = new FormData(formEl);
          const res = await fetch("/run", {{ method: "POST", body: formData }});
          if (!res.ok) {{
            const data = await res.json().catch(() => ({{}}));
            if (data && data.error === "validation") {{
              if (data.area) showFieldError(areaField, areaError, data.area);
              if (data.amenities) showFieldError(document.querySelector(".amenities-section"), amenitiesError, data.amenities);
              setRunState("idle");
              return;
            }}
            throw new Error("Start failed");
          }}
          const data = await res.json();
          currentJobId = data.job_id;
          setRunState("running");
          runStatus.textContent = "Listing places…";
          // Refresh footer counter after a successful start
          refreshJobsStarted();
          if (pollTimer) clearInterval(pollTimer);
          pollTimer = setInterval(pollStatus, 800);
        }} catch (err) {{
          runStatus.textContent = "Failed to start.";
          setRunState("idle");
        }}
      }});
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
    background: BackgroundTasks,
    area: str = Form(DEFAULT_AREA),
    amenities: str = Form(DEFAULT_AMENITY_STRING),
):
    # Server-side validation
    if not (area and area.strip()):
        return JSONResponse({"error": "validation", "area": "Please enter a city."}, status_code=400)
    if not (amenities and amenities.strip()):
        return JSONResponse({"error": "validation", "amenities": "Please select at least one amenity."}, status_code=400)

    job_id = uuid.uuid4().hex[:10]
    output_path = OUTPUT_DIR / f"{job_id}.csv"

    JOBS[job_id] = {
        "status": "running",
        "output": str(output_path),
        "created_at": time.time(),
    }
    # Count this as a started job (no DB; file-backed counter)
    _increment_jobs_started(1)

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
    )
    return {"job_id": job_id}

@app.get("/stats")
def stats():
    return {"jobs_started": _get_jobs_started()}


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
    }
    if info.get("status") == "done":
        path = Path(info.get("output", ""))
        payload["download_url"] = f"/download/{job_id}" if path.exists() else None
        # Provide a small preview (first 50 rows)
        preview_rows = []
        try:
            if path.exists():
                with path.open("r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    for i, row in enumerate(reader):
                        if i >= 50:
                            break
                        preview_rows.append(row)
        except Exception:
            preview_rows = []
        payload["preview"] = preview_rows
    return payload


@app.post("/cancel/{job_id}")
def cancel(job_id: str):
    info = JOBS.get(job_id)
    if not info:
        return {"ok": False, "error": "unknown_job"}
    info["status"] = "cancelled"
    # Best-effort delete any output immediately
    try:
        path = Path(str(info.get("output", "")))
        if path.exists() and path.is_file():
            path.unlink(missing_ok=True)
    except Exception:
        pass
    # Remove job metadata
    JOBS.pop(job_id, None)
    return {"ok": True}

@app.get("/download/{job_id}")
def download(job_id: str):
    info = JOBS.get(job_id)
    if not info or info.get("status") != "done":
        return PlainTextResponse("Job not ready", status_code=404)

    output_path = Path(info.get("output", ""))
    if not output_path.exists():
        return PlainTextResponse("File missing", status_code=404)

    return FileResponse(
        output_path,
        filename=f"{job_id}.csv",
        media_type="text/csv",
    )


