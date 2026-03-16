# Telia Weather ETL Pipeline

A Python ETL pipeline that collects 30 days of historical weather data for European capital cities and stores it in a local SQLite database. Built as a home assignment for the Telia Data Engineer internship.

## Quick Start

```bash
# No dependencies to install — uses only the Python standard library (3.10+)
python etl_pipeline.py
```

The pipeline will:
1. Fetch all European countries and their capitals
2. Collect 30 days of historical weather for each capital
3. Store everything in `weather.db`
4. Run verification checks and print a summary

### CLI Options

```bash
python etl_pipeline.py --days 14          # Custom look-back window
python etl_pipeline.py --db my_data.db    # Custom database path
```

## Data Sources

| Source | URL | Purpose |
|--------|-----|---------|
| RestCountries API | `https://restcountries.com/v3.1/region/europe` | Country names, capitals, coordinates, population, area |
| Open-Meteo Archive API | `https://archive-api.open-meteo.com/v1/archive` | Daily historical weather (temperature, precipitation, wind, sunshine) |

## Pipeline Steps

### 1. Extract
- Fetches all European countries from RestCountries in a single GET request.
- For each capital with valid coordinates, fetches 30 days of daily weather from Open-Meteo.
- Uses HTTP retries with exponential back-off (3 attempts) for resilience.
- Adds a ~0.4s delay between weather requests to respect rate limits.

### 2. Transform
- Flattens nested JSON (e.g. `name.common` → `country_name`).
- Skips countries missing a capital or coordinates, logging each skip.
- Converts sunshine duration from seconds → hours.
- Coerces `None`/`NaN` values to `NULL` for SQLite.
- Enforces correct types: `population` → int, `area` → float, coordinates → float.

### 3. Load
- **Raw layer** (`raw_countries`, `raw_weather`): stores full JSON payloads as-is for auditability.
- **Clean layer** (`countries`, `daily_weather`): typed, structured tables ready for analytics.
- Uses `INSERT OR REPLACE` for idempotent re-runs.

### 4. Views
Three analytical views built on the clean layer:

| View | Description |
|------|-------------|
| `v_avg_temperature_ranking` | Capitals ranked by average temperature (descending) |
| `v_most_rainfall` | Countries ranked by total precipitation |
| `v_30day_summary` | Comprehensive 30-day summary per country with all metrics |

### 5. Verify
Automated sanity checks after loading:
- Country and weather record counts > 0
- Every country has matching weather data
- Temperature max ≥ min for all rows
- Precipitation is non-negative
- Sunshine duration within 0–24 hours
- Raw payloads preserved
- Preview of top 5 warmest and wettest capitals

## Database Schema

### ER Diagram

```
┌──────────────────────────┐       ┌──────────────────────────────┐
│       countries           │       │       daily_weather           │
├──────────────────────────┤       ├──────────────────────────────┤
│ PK  country_name   TEXT   │──┐    │ PK  country_name   TEXT      │
│     capital_city   TEXT   │  │    │ PK  date            TEXT      │
│     latitude       REAL   │  │    │     temperature_max_c  REAL   │
│     longitude      REAL   │  │    │     temperature_min_c  REAL   │
│     population     INT    │  │    │     precipitation_mm   REAL   │
│     area_km2       REAL   │  │    │     wind_speed_max_kmh REAL   │
│     loaded_at      TEXT   │  │    │     sunshine_hours     REAL   │
└──────────────────────────┘  │    │     loaded_at          TEXT   │
                               │    └──────────────────────────────┘
                               │         FK ▲
                               └────────────┘

┌──────────────────────────┐       ┌──────────────────────────────┐
│     raw_countries         │       │       raw_weather             │
├──────────────────────────┤       ├──────────────────────────────┤
│ PK  id          INTEGER   │       │ PK  country_name   TEXT      │
│     payload     TEXT      │       │     payload         TEXT      │
│     loaded_at   TEXT      │       │     loaded_at       TEXT      │
└──────────────────────────┘       └──────────────────────────────┘
```

### Clean Layer
- **`countries`** — One row per European country with capital coordinates and demographics.
- **`daily_weather`** — One row per country per day. Composite PK `(country_name, date)` with FK to `countries`.

### Raw Layer
- **`raw_countries`** — Full RestCountries API response stored as JSON for auditability.
- **`raw_weather`** — One row per capital with the complete Open-Meteo JSON payload.

## Project Structure

```
weather-app/
├── etl_pipeline.py     # Main pipeline script (all ETL logic)
├── dump_schema.py      # Utility: exports DB schema to SQL file
├── schema.sql          # Pre-generated database structure dump (DDL + views)
├── requirements.txt    # Note that only stdlib is needed
├── .gitignore          # Ignore .db files, __pycache__, .venv, IDE files
└── README.md           # This file
```

## Design Decisions

### Why SQLite?
SQLite is built into Python's standard library, requires zero configuration, and produces a single portable file. It's ideal for a self-contained ETL demo — the evaluator can run the pipeline and immediately query results without setting up a database server.

### Why no external dependencies?
Using only the standard library (`urllib`, `sqlite3`, `json`, `logging`, `argparse`) means the project runs on any Python 3.10+ installation with zero setup. This reduces friction for evaluation and demonstrates proficiency with Python's built-in tools.

### Why raw/clean separation?
The two-layer architecture (raw JSON payloads + typed clean tables) follows data engineering best practices:
- **Auditability**: raw payloads are preserved exactly as received from the APIs.
- **Reprocessing**: if transformation logic changes, clean tables can be rebuilt from raw data without re-fetching.
- **Debugging**: when a data issue appears in clean tables, the original payload is always available for comparison.

### Why idempotent loads?
Using `INSERT OR REPLACE` ensures the pipeline can be re-run safely without duplicating data. This is essential for production pipelines that may need to be restarted after partial failures.

### Why retry with back-off?
External APIs can have transient failures (rate limits, timeouts, server errors). Exponential back-off (1s → 2s → 4s) with 3 attempts makes the pipeline resilient without overwhelming the API with rapid retries.

## Example Queries

After running the pipeline, explore the data with:

```sql
-- Top 10 warmest European capitals
SELECT country_name, avg_temperature_c, avg_max_c, avg_min_c
FROM v_avg_temperature_ranking
LIMIT 10;

-- Rainiest countries
SELECT country_name, total_precipitation_mm, avg_daily_precipitation_mm
FROM v_most_rainfall
LIMIT 10;

-- Full 30-day summary for a specific country
SELECT * FROM v_30day_summary WHERE country_name = 'Estonia';

-- Coldest day recorded across all capitals
SELECT country_name, date, temperature_min_c
FROM daily_weather
WHERE temperature_min_c IS NOT NULL
ORDER BY temperature_min_c ASC
LIMIT 5;

-- Countries with the most sunshine
SELECT country_name, total_sunshine_hours, avg_daily_sunshine_hours
FROM v_30day_summary
ORDER BY total_sunshine_hours DESC
LIMIT 10;
```

Run queries directly from the command line:
```bash
sqlite3 weather.db "SELECT * FROM v_avg_temperature_ranking LIMIT 10;"
```

## License

MIT
