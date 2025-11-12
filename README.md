# City Gig Scraper

Discover hospitality venues from OpenStreetMap and scan their websites for hiring pages/signals. Export results to CSV. Use it via a friendly Web UI or the original CLI.

- Built on Overpass (OSM) discovery + polite, async crawling
- Detects job/career pages and common hiring keywords
- CSV export, with preview and download in the UI

Originally created for Berlin‑Mitte; now works for any city as long as OSM has coverage.

## Quick start

### 1) Install

```powershell
python -m venv .venv
\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2) Run the Web UI

```powershell
uvicorn web.app:app --host 127.0.0.1 --port 8000 --reload
```

Open http://127.0.0.1:8000 and:
- Enter a city (autocomplete helps; e.g., Berlin, Tokyo, New York)
- Pick categories (amenities) to search (e.g., Cafes, Restaurants, Bars)
- Click “Run scrape”
- Watch live progress; when done, preview the first 50 rows and download the CSV

Notes about the UI:
- Jobs run in the background; you can cancel while running.
- Downloads are generated in a temporary folder and are kept for ~1 hour.
- A usage counter is stored at `output/stats.json` (override with the `STATS_FILE` env var).

Set a custom stats path (optional):

```powershell
$env:STATS_FILE="output\stats.json"
```

### 3) Or run the CLI

Recommended for bulk runs or automation. Replace the user‑agent with your own contact info.

```powershell
python -m job_scraper.main `
  --area "Bezirk Mitte, Berlin" `
  --output "output/berlin_mitte_jobs.csv" `
  --user-agent "YourNameJobScraper/0.1 (+https://your.site/contact)" `
  --concurrency 15 `
  --max-job-links 10
```

## Features

- OSM‑powered discovery by amenity types (cafe, restaurant, bar, pub, bakery, fast_food, etc.)
- Adaptive querying: tries single‑area; falls back to grid tiling on errors/timeouts
- Optional district splitting for large cities to avoid Overpass timeouts
- Asynchronous, polite crawling (httpx, configurable concurrency)
- Heuristic detection of job pages and hiring cues; vendor platform recognition
- CSV output ready for Google Sheets
- Minimal Web UI (FastAPI) with city autocomplete, amenity pills, live progress, preview, and download

## Web UI details

- Start locally: `uvicorn web.app:app --host 127.0.0.1 --port 8000 --reload`
- Home (`/`): submit Location and Categories, then “Run scrape”
- Status polling and cancellation are built‑in
- Preview (first 50 rows) is shown when done
- Download via a button (served from a temporary file)
- Usage counter endpoint (`/stats`) writes to `output/stats.json` (override with `STATS_FILE`)
- Temporary CSVs live under your system temp in a `city_gig_scraper_outputs` folder and are auto‑cleaned after ~1 hour
- Jobs queue automatically when all worker slots are busy. The status indicator shows `Queued… (position/length)` until a slot frees up.
- `/stats` now supports `HEAD` requests so free uptime monitors can keep the app warm without counting as traffic.

### Hosting & tuning

Environment variables (usable via `.env`, Render/Railway dashboards, etc.):

- `OVERPASS_URLS`: Comma-separated list of Overpass endpoints. Requests fail over in order. Example: `https://overpass.kumi.systems/api/interpreter,https://overpass.openstreetmap.ru/api/interpreter`
- `OVERPASS_URL`: Fallback single endpoint (appended to `OVERPASS_URLS`).
- `UI_CONCURRENCY`: Async crawler concurrency (default 18).
- `UI_MAX_JOB_LINKS`: Max job links per venue (default 6).
- `UI_CRAWL_DEPTH`: Internal link depth for job pages (default 3).
- `UI_LIMIT`: Optional soft cap on venues processed per run.
- `UI_MAX_ACTIVE_JOBS`: Max concurrent jobs (default 3). Additional runs queue.
- `UI_USER_AGENT`: Override the polite default header.
- `UI_LOG_LEVEL`: Logging verbosity (`INFO`/`DEBUG`).

Example `.env` for a hosted instance:

```dotenv
OVERPASS_URLS=https://overpass.kumi.systems/api/interpreter,https://overpass.openstreetmap.ru/api/interpreter
UI_CONCURRENCY=10
UI_MAX_JOB_LINKS=4
UI_CRAWL_DEPTH=2
UI_MAX_ACTIVE_JOBS=3
UPSTASH_REDIS_REST_URL=...           # optional, for persistent counters/cache
UPSTASH_REDIS_REST_TOKEN=...
```

Tip: Pair the app with a lightweight uptime ping (e.g., UptimeRobot `HEAD https://your-app/stats` every 10 minutes) to avoid cold starts on free tiers.

## CLI usage and flags

Basic run:

```powershell
python -m job_scraper.main `
  --area "Bezirk Mitte, Berlin" `
  --output "output/berlin_mitte_jobs.csv" `
  --user-agent "YourNameJobScraper/0.1 (+https://your.site/contact)"
```

Common flags:
- **--area**: Administrative area name in OSM. Use exact names where possible.
- **--amenities**: Comma‑separated OSM amenity types.
  - Default: `cafe,restaurant,bar,pub,fast_food,bakery,ice_cream,biergarten,food_court`
- **--output**: CSV path to write results.
- **--user-agent**: Include your contact info (URL or email) in the header.
- **--concurrency**: Max concurrent HTTP requests (try 5–18; be polite).
- **--max-job-links**: Max candidate job links to follow per site.
- **--crawl-depth**: Max internal link depth (>= 1).
- **--limit**: Limit number of places (useful for quick tests).
- **--overpass-url**: Override Overpass endpoint if needed.
- **--log-level**: `INFO` (default) or `DEBUG` for more detail.
- **--split-into-districts**: Automatically split large areas into districts to reduce Overpass timeouts.

Scan an entire city with district splitting (recommended for large metros):

```powershell
python -m job_scraper.main `
  --area "Berlin" `
  --split-into-districts `
  --output "output/berlin_all_jobs.csv" `
  --user-agent "YourNameJobScraper/0.1 (+mailto:you@example.com)" `
  --concurrency 18 `
  --max-job-links 10
```

Change the amenity mix:

```powershell
# Only cafes
python -m job_scraper.main --area "Berlin" --amenities cafe --output output/cafes_only.csv

# Restaurants and bars only
python -m job_scraper.main --area "Berlin" --amenities restaurant,bar --output output/restaurants_bars.csv

# Add nightclubs to defaults
python -m job_scraper.main --area "Berlin" --amenities cafe,restaurant,bar,pub,fast_food,bakery,ice_cream,biergarten,food_court,nightclub
```

Tips for areas:
- Use exact OSM names. Prefer “Frankfurt am Main” over “Frankfurt”.
- If resolution fails, the tool may fall back to a Berlin‑Mitte bounding box. Double‑check spelling/casing.
- For quick trials, combine a smaller area with `--limit` (e.g., `--limit 20`).
- Overpass endpoints can also be supplied via environment variables in the CLI:
  - `OVERPASS_URLS=https://overpass.kumi.systems/api/interpreter,https://overpass.openstreetmap.ru/api/interpreter`
  - `OVERPASS_URL=https://overpass-api.de/api/interpreter` (optional single fallback)
  - CLI honors the same failover logic used by the Web UI.

## Output

The CSV schema:
- `name`: Venue name
- `type`: OSM amenity type
- `homepage`: Discovered website
- `job_page_url`: Detected job/career page (if found)

CLI outputs are written to your chosen `--output` path (commonly under `output/`).
UI outputs are temporary and downloadable via the browser.

Google Sheets import:
1) Copy the CSV contents
2) Paste into cell A1 in a new Google Sheet (Cmd/Ctrl+Shift+V for plain text)
3) Data → Split text to columns

## Politeness and stability

- Keep concurrency reasonable; be respectful of target sites.
- Include a real user agent with contact info when using the CLI.
- Overpass can rate‑limit or time out on large queries; use district splitting for big cities.
- Provide multiple Overpass mirrors via `OVERPASS_URLS` so the scraper can fail over automatically.
- Increase `UI_MAX_ACTIVE_JOBS` only if your host has spare CPU/network; otherwise the built-in queue keeps things fair for concurrent users.

## Development

Project layout:
- `job_scraper/` — CLI entrypoint and core logic (discovery, crawling, detection)
- `web/app.py` — FastAPI app for the Web UI
- `output/` — Default location for CLI outputs and the UI’s `stats.json`

Run the web app in dev:

```powershell
uvicorn web.app:app --reload
```

Run the CLI locally:

```powershell
python -m job_scraper.main --area "Bezirk Mitte, Berlin" --output output/test.csv --user-agent "DevTest/0.1 (+mailto:you@example.com)"
```

