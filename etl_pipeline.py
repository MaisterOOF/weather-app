"""
Collects 30 days of historical weather data for European capital cities
and stores it in a local SQLite database with raw and clean layers.

Usage:

    python etl_pipeline.py --days 10 [--db weather.db]
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from sql_definitions import ALL_DDL, ALL_VIEW_NAMES, ALL_VIEWS

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RESTCOUNTRIES_URL = (
    "https://restcountries.com/v3.1/region/europe"
    "?fields=name,capital,capitalInfo,population,area"
)

OPEN_METEO_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

DAILY_VARIABLES = (
    "temperature_2m_max,temperature_2m_min,precipitation_sum,"
    "wind_speed_10m_max,sunshine_duration"
)

ARCHIVE_LAG_DAYS = 5          # Open-Meteo archive has ~5-day lag
HTTP_RETRIES = 3              # Max retry attempts for HTTP requests
HTTP_BACKOFF_BASE = 1.0       # Base seconds for exponential back-off
RATE_LIMIT_DELAY = 0.5        # Seconds between Open-Meteo requests

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "weather.db"
DEFAULT_LOOKBACK_DAYS = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTTP Helpers
# ---------------------------------------------------------------------------


def http_get_json(url: str) -> dict | list:
    """Perform an HTTP GET with retry and exponential back-off.

    Returns the parsed JSON response body.
    Raises ``RuntimeError`` after exhausting all retry attempts.
    """
    last_error: Exception | None = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "TeliaWeatherETL/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
            last_error = exc
            wait = HTTP_BACKOFF_BASE * (2 ** (attempt - 1))
            log.warning("HTTP attempt %d/%d failed for %s: %s — retrying in %.1fs",
                        attempt, HTTP_RETRIES, url[:80], exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {url} after {HTTP_RETRIES} attempts: {last_error}")


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------


def extract_countries() -> list[dict]:
    """Fetch European countries from the RestCountries API."""
    log.info("Extracting countries from RestCountries API …")
    data = http_get_json(RESTCOUNTRIES_URL)
    log.info("Received %d country records.", len(data))
    return data


def extract_weather(latitude: float, longitude: float,
                    start_date: str, end_date: str) -> dict:
    """Fetch daily weather archive for a single location from Open-Meteo."""
    params = (
        f"?latitude={latitude}&longitude={longitude}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&daily={DAILY_VARIABLES}&timezone=auto"
    )
    return http_get_json(OPEN_METEO_BASE_URL + params)


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def transform_countries(raw: list[dict]) -> list[dict]:
    """Flatten and validate country records.

    Returns a list of cleaned dicts; skips countries without a capital or
    valid coordinates and logs a warning for each.
    """
    cleaned: list[dict] = []
    for entry in raw:
        name = entry.get("name", {}).get("common", "Unknown")

        capitals = entry.get("capital")
        if not capitals:
            log.warning("Skipping %s — no capital city listed.", name)
            continue

        latlng = entry.get("capitalInfo", {}).get("latlng")
        if not latlng or len(latlng) < 2:
            log.warning("Skipping %s — no capital coordinates available.", name)
            continue

        cleaned.append({
            "country_name": str(name),
            "capital_city": str(capitals[0]),
            "latitude":     float(latlng[0]),
            "longitude":    float(latlng[1]),
            "population":   int(entry.get("population", 0)),
            "area_km2":     float(entry.get("area", 0.0)),
        })

    log.info("Transformed %d countries (%d skipped).",
             len(cleaned), len(raw) - len(cleaned))
    return cleaned


def _safe_float(value: object) -> float | None:
    """Return *value* as a float, or ``None`` if it is missing / NaN."""
    if value is None:
        return None
    try:
        f = float(value)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def transform_weather(country_name: str, raw: dict) -> list[dict]:
    """Convert a raw Open-Meteo daily response into a list of row dicts.

    Converts sunshine_duration from seconds → hours and coerces
    None/NaN values to None.
    """
    daily = raw.get("daily", {})
    dates = daily.get("time", [])
    rows: list[dict] = []
    for i, date_str in enumerate(dates):
        sunshine_sec = _safe_float(daily.get("sunshine_duration", [None] * (i + 1))[i])
        sunshine_hrs = round(sunshine_sec / 3600, 2) if sunshine_sec is not None else None

        rows.append({
            "country_name":       country_name,
            "date":               date_str,
            "temperature_max_c":  _safe_float(daily.get("temperature_2m_max", [None] * (i + 1))[i]),
            "temperature_min_c":  _safe_float(daily.get("temperature_2m_min", [None] * (i + 1))[i]),
            "precipitation_mm":   _safe_float(daily.get("precipitation_sum", [None] * (i + 1))[i]),
            "wind_speed_max_kmh": _safe_float(daily.get("wind_speed_10m_max", [None] * (i + 1))[i]),
            "sunshine_hours":     sunshine_hrs,
        })
    return rows


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------


def init_database(conn: sqlite3.Connection) -> None:
    """Create all tables (raw + clean layers) if they do not exist."""
    conn.executescript(ALL_DDL)
    log.info("Database tables initialised.")


def load_raw_countries(conn: sqlite3.Connection, payload: list[dict]) -> None:
    """Insert the full RestCountries JSON payload into the raw layer."""
    conn.execute(
        "INSERT INTO raw_countries (payload) VALUES (?)",
        (json.dumps(payload, ensure_ascii=False),),
    )
    conn.commit()
    log.info("Raw countries payload stored.")


def load_raw_weather(conn: sqlite3.Connection, country_name: str,
                     payload: dict) -> None:
    """Insert a single capital's raw weather JSON into the raw layer."""
    conn.execute(
        "INSERT OR REPLACE INTO raw_weather (country_name, payload) VALUES (?, ?)",
        (country_name, json.dumps(payload, ensure_ascii=False)),
    )
    conn.commit()


def load_clean_countries(conn: sqlite3.Connection, countries: list[dict]) -> None:
    """Upsert cleaned country records into the clean layer."""
    conn.executemany(
        """INSERT OR REPLACE INTO countries
           (country_name, capital_city, latitude, longitude, population, area_km2)
           VALUES (:country_name, :capital_city, :latitude, :longitude,
                   :population, :area_km2)""",
        countries,
    )
    conn.commit()
    log.info("Loaded %d countries into clean layer.", len(countries))


def load_clean_weather(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """Bulk-upsert daily weather rows into the clean layer."""
    conn.executemany(
        """INSERT OR REPLACE INTO daily_weather
           (country_name, date, temperature_max_c, temperature_min_c,
            precipitation_mm, wind_speed_max_kmh, sunshine_hours)
           VALUES (:country_name, :date, :temperature_max_c, :temperature_min_c,
                   :precipitation_mm, :wind_speed_max_kmh, :sunshine_hours)""",
        rows,
    )
    conn.commit()
    log.info("Loaded %d weather records into clean layer.", len(rows))


def create_views(conn: sqlite3.Connection) -> None:
    """Create (or replace) analytical views."""
    # Drop existing views first so definitions can be updated
    for view_name in ALL_VIEW_NAMES:
        conn.execute(f"DROP VIEW IF EXISTS {view_name}")
    conn.executescript(ALL_VIEWS)
    log.info("Analytical views created.")


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------


def verify(conn: sqlite3.Connection) -> None:
    """Run automated sanity checks and print a summary."""
    log.info("=" * 60)
    log.info("VERIFICATION")
    log.info("=" * 60)

    # 1. Country count
    (country_count,) = conn.execute("SELECT COUNT(*) FROM countries").fetchone()
    _check("Countries loaded", country_count > 0, f"{country_count} rows")

    # 2. Weather record count
    (weather_count,) = conn.execute("SELECT COUNT(*) FROM daily_weather").fetchone()
    _check("Weather records loaded", weather_count > 0, f"{weather_count} rows")

    # 3. Every country has weather data
    orphans = conn.execute(
        """SELECT c.country_name FROM countries c
           LEFT JOIN daily_weather dw ON dw.country_name = c.country_name
           WHERE dw.country_name IS NULL"""
    ).fetchall()
    _check("All countries have weather data", len(orphans) == 0,
           f"{len(orphans)} countries missing weather" if orphans else "OK")

    # 4. Temperature sanity: max >= min
    (bad_temps,) = conn.execute(
        """SELECT COUNT(*) FROM daily_weather
           WHERE temperature_max_c IS NOT NULL
             AND temperature_min_c IS NOT NULL
             AND temperature_max_c < temperature_min_c"""
    ).fetchone()
    _check("Temperature max >= min", bad_temps == 0,
           f"{bad_temps} invalid rows" if bad_temps else "OK")

    # 5. Precipitation non-negative
    (neg_precip,) = conn.execute(
        "SELECT COUNT(*) FROM daily_weather WHERE precipitation_mm < 0"
    ).fetchone()
    _check("Precipitation non-negative", neg_precip == 0,
           f"{neg_precip} negative rows" if neg_precip else "OK")

    # 6. Sunshine within 0–24 hours
    (bad_sun,) = conn.execute(
        """SELECT COUNT(*) FROM daily_weather
           WHERE sunshine_hours IS NOT NULL
             AND (sunshine_hours < 0 OR sunshine_hours > 24)"""
    ).fetchone()
    _check("Sunshine 0–24 h", bad_sun == 0,
           f"{bad_sun} out-of-range rows" if bad_sun else "OK")

    # 7. Raw layer preserved
    (raw_c,) = conn.execute("SELECT COUNT(*) FROM raw_countries").fetchone()
    (raw_w,) = conn.execute("SELECT COUNT(*) FROM raw_weather").fetchone()
    _check("Raw payloads preserved", raw_c > 0 and raw_w > 0,
           f"raw_countries={raw_c}, raw_weather={raw_w}")

    # 8. Preview warmest and wettest
    log.info("-" * 60)
    log.info("Top 5 warmest capitals:")
    for row in conn.execute(
        "SELECT country_name, avg_temperature_c FROM v_avg_temperature_ranking LIMIT 5"
    ):
        log.info("  %-25s  %.2f °C", row[0], row[1])

    log.info("Top 5 wettest capitals:")
    for row in conn.execute(
        "SELECT country_name, total_precipitation_mm FROM v_most_rainfall LIMIT 5"
    ):
        log.info("  %-25s  %.2f mm", row[0], row[1])

    log.info("=" * 60)


def _check(label: str, passed: bool, detail: str) -> None:
    """Log a single verification check result."""
    status = "PASS" if passed else "FAIL"
    log.info("  [%s] %s — %s", status, label, detail)


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------


def run_pipeline(db_path: Path, lookback_days: int) -> None:
    """Execute the full ETL pipeline end-to-end."""
    log.info("Starting Telia Weather ETL Pipeline")
    log.info("Database: %s | Look-back: %d days", db_path, lookback_days)

    # --- Date range ---
    end_date = datetime.now().date() - timedelta(days=ARCHIVE_LAG_DAYS)
    start_date = end_date - timedelta(days=lookback_days - 1)
    log.info("Weather date range: %s → %s", start_date, end_date)

    # --- Extract countries ---
    raw_countries = extract_countries()

    # --- Transform countries ---
    countries = transform_countries(raw_countries)

    # --- Init database ---
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    init_database(conn)

    # --- Load raw + clean countries ---
    load_raw_countries(conn, raw_countries)
    load_clean_countries(conn, countries)

    # --- Weather loop ---
    all_weather_rows: list[dict] = []
    for i, country in enumerate(countries, 1):
        name = country["country_name"]
        capital = country["capital_city"]
        log.info("[%d/%d] Fetching weather for %s (%s) …",
                 i, len(countries), capital, name)
        try:
            raw_weather = extract_weather(
                country["latitude"], country["longitude"],
                start_date.isoformat(), end_date.isoformat(),
            )
        except RuntimeError as exc:
            log.warning("Could not fetch weather for %s: %s — skipping.", name, exc)
            continue

        load_raw_weather(conn, name, raw_weather)
        weather_rows = transform_weather(name, raw_weather)
        all_weather_rows.extend(weather_rows)

        if i < len(countries):
            time.sleep(RATE_LIMIT_DELAY)

    # --- Bulk load clean weather ---
    if all_weather_rows:
        load_clean_weather(conn, all_weather_rows)

    # --- Create views ---
    create_views(conn)

    # --- Verify ---
    verify(conn)

    conn.close()
    log.info("Pipeline finished successfully.")


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------


def main() -> None:
    """Parse arguments and launch the pipeline."""
    parser = argparse.ArgumentParser(
        description="Telia Weather ETL — European capital weather data pipeline",
    )
    parser.add_argument(
        "--days", type=int, default=DEFAULT_LOOKBACK_DAYS,
        help="Number of days to look back (default: %(default)s)",
    )
    parser.add_argument(
        "--db", type=str, default=str(DEFAULT_DB_PATH),
        help="Path to SQLite database file (default: %(default)s)",
    )
    args = parser.parse_args()
    run_pipeline(db_path=Path(args.db), lookback_days=args.days)


if __name__ == "__main__":
    main()
