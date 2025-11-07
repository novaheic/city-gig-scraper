# Berlin Mitte Hospitality Job Scraper (MVP)

Prototype for discovering hospitality venues in Berlin‑Mitte via the
Overpass API, fetching their websites politely, and flagging hiring cues.

## Setup

Create and activate a virtual environment, then install dependencies:

```
python -m venv .venv
\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Usage

After installing dependencies, run the CLI. Replace the user agent with a
value that includes your contact information.

```
python -m job_scraper.main ^
  --area "Bezirk Mitte, Berlin" ^
  --output "output/berlin_mitte_jobs.csv" ^
  --user-agent "BerlinJobScraper/0.1 (+https://example.com/contact)" ^
  --concurrency 15 ^
  --max-job-links 10
```

### Flags

- **--area**: Administrative area name to search in OpenStreetMap. The tool tries to
  resolve the exact OSM relation by name and queries that area. If it cannot resolve
  the name, it falls back to a fixed Berlin‑Mitte bounding box. For best results, use
  exact OSM names (e.g., Berlin districts start with "Bezirk ..., Berlin").
  - Examples: "Bezirk Mitte, Berlin" (default), "Bezirk Friedrichshain-Kreuzberg, Berlin",
    "Bezirk Neukölln, Berlin", "Bezirk Pankow, Berlin".

- **--amenities**: Comma‑separated list of amenity types to include (OSM `amenity=*`).
  Defaults: `cafe,restaurant,bar,pub,fast_food,bakery,ice_cream,biergarten,food_court`.
  - Example (cafes only): `--amenities cafe`
  - Example (add nightclub): `--amenities cafe,restaurant,bar,nightclub`

- **--output**: Path to the CSV that will be written with discovered venues and hiring signals.
  - Example: `--output output/fhain_jobs.csv`

- **--user-agent**: User‑Agent string sent with HTTP requests. Include contact info
  (URL or email) so website owners can reach you.
  - Example: `--user-agent "YourNameJobScraper/0.1 (+https://your.site/contact)"`

- **--concurrency**: Max concurrent HTTP requests. Higher is faster but be polite.
  Typical range: 5–15.

- **--max-job-links**: Max number of candidate "jobs/careers" links to follow per site.
  Lower this to speed up; raise to be more thorough.

- **--crawl-depth**: How deep to follow internal links when searching for job pages (>= 1).

- **--limit**: Limit the number of places processed/tried (useful for quick tests).
  - Example: `--limit 25`

- **--overpass-url**: Optional Overpass API endpoint override.

- **--log-level**: Logging verbosity (e.g., `INFO`, `DEBUG`).

- **--split-into-districts**: Automatically split large areas (e.g., "Berlin") into
  districts and query each separately to avoid timeouts. See "Scanning entire cities"
  section below for details.

### Changing the area (examples)

Use exact OSM administrative names for reliable results.

```
# Location
python -m job_scraper.main --area "Bezirk Friedrichshain-Kreuzberg, Berlin" --output output/fhain_xberg_jobs.csv
python -m job_scraper.main --area "Bezirk Neukölln, Berlin" --output output/neukolln_jobs.csv
python -m job_scraper.main --area "Hamburg" --output output/hamburg_jobs.csv
python -m job_scraper.main --area "München" --output output/muenchen_jobs.csv
...
```

Note:
- Do not use ambiguous names like "Frankfurt" alone. Prefer the exact OSM name,
  e.g., "Frankfurt am Main" or "Frankfurt (Oder)".

Tips:
- If an area name cannot be resolved in OSM, the scraper will fall back to a
  fixed Berlin‑Mitte bounding box (even if you requested another city). Double‑check
  the spelling/casing and prefer the full administrative name.
- For quick trials, combine a smaller area with `--limit` (e.g., `--limit 20`).
- **Large areas may timeout**: Querying entire cities (e.g., "Berlin" alone) with many
  amenities can cause Overpass API timeouts (504 errors). Use the `--split-into-districts`
  flag to automatically break large areas into smaller districts and query each separately.

### Scanning entire cities

To scan an entire city without timeouts, use the `--split-into-districts` flag. This
automatically discovers sub-areas (districts) of the parent area and queries each
separately, then combines the results.

**Example - scanning all of Berlin:**

```powershell
python -m job_scraper.main `
  --area "Berlin" `
  --split-into-districts `
  --output "output/berlin_all_jobs.csv" `
  --user-agent "BerlinJobScraper/0.1 (+mailto:your@email.com)" `
  --concurrency 18 `
  --max-job-links 10
```

**How it works:**
1. The scraper queries OSM to find all districts within the parent area (e.g., all
   Berlin Bezirke when querying "Berlin").
2. Each district is queried separately to avoid timeouts.
3. Results are combined and deduplicated by website URL.
4. The combined results are then scraped for hiring signals.

**Note:** If sub-areas cannot be found automatically, the scraper falls back to
querying the area as a single unit. This feature works best for cities with
well-defined administrative districts in OSM.

### Changing the amenities (examples)

The `--amenities` flag accepts a comma-separated list of OpenStreetMap amenity types.
These determine which types of venues are discovered and scraped.

**Common hospitality/food service amenities:**
- `cafe` - Coffee shops and cafes
- `restaurant` - Full-service restaurants
- `bar` - Bars and cocktail lounges
- `pub` - Pubs and taverns
- `fast_food` - Fast food restaurants
- `bakery` - Bakeries
- `ice_cream` - Ice cream shops
- `biergarten` - Beer gardens
- `food_court` - Food courts
- `nightclub` - Nightclubs
- `canteen` - Cafeterias and canteens

**Other useful amenity types:**
- `hotel` - Hotels
- `hostel` - Hostels
- `cinema` - Movie theaters
- `theatre` - Theaters
- `library` - Libraries
- `pharmacy` - Pharmacies
- `hospital` - Hospitals
- `bank` - Banks
- `fuel` - Gas stations
- `parking` - Parking facilities

**Examples:**

```
# Only cafes
python -m job_scraper.main --area "Berlin" --amenities cafe --output output/cafes_only.csv

# Restaurants and bars only
python -m job_scraper.main --area "Berlin" --amenities restaurant,bar --output output/restaurants_bars.csv

# Add nightclubs to default set
python -m job_scraper.main --area "Berlin" --amenities cafe,restaurant,bar,pub,fast_food,bakery,ice_cream,biergarten,food_court,nightclub

# Hotels and hostels
python -m job_scraper.main --area "Berlin" --amenities hotel,hostel --output output/accommodation.csv

# Mix of hospitality and entertainment
python -m job_scraper.main --area "Berlin" --amenities restaurant,bar,cinema,theatre --output output/entertainment.csv
```

**Tips:**
- Use exact OSM amenity tag names (lowercase with underscores). Check the
  [OpenStreetMap Wiki](https://wiki.openstreetmap.org/wiki/Key:amenity) for the
  complete list of valid amenity types.
- The default amenities are defined in `job_scraper/main.py` as `DEFAULT_AMENITIES`.
  You can modify that constant to change the default for all runs.
- Combine with `--limit` to test different amenity combinations quickly.

**Google Sheets Import**

Just copy the csv's content, click on cell A1 in a new google sheet > cmd+shift+v > Data (with A column still highlighted) > Split Text to Columns


