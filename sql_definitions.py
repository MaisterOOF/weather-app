"""
SQL Definitions
===============
All DDL (table creation) and view definitions used by the ETL pipeline.
Separated for readability — raw layer, clean layer, and analytical views.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Raw Layer — stores original API responses as-is for auditability
# ---------------------------------------------------------------------------

DDL_RAW_COUNTRIES = """
CREATE TABLE IF NOT EXISTS raw_countries (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    payload     TEXT    NOT NULL,
    loaded_at   TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""

DDL_RAW_WEATHER = """
CREATE TABLE IF NOT EXISTS raw_weather (
    country_name  TEXT PRIMARY KEY,
    payload       TEXT NOT NULL,
    loaded_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

# ---------------------------------------------------------------------------
# Clean Layer — typed, structured tables ready for analytics
# ---------------------------------------------------------------------------

DDL_COUNTRIES = """
CREATE TABLE IF NOT EXISTS countries (
    country_name  TEXT    PRIMARY KEY,
    capital_city  TEXT    NOT NULL,
    latitude      REAL    NOT NULL,
    longitude     REAL    NOT NULL,
    population    INTEGER NOT NULL,
    area_km2      REAL    NOT NULL,
    loaded_at     TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""

DDL_DAILY_WEATHER = """
CREATE TABLE IF NOT EXISTS daily_weather (
    country_name        TEXT NOT NULL,
    date                TEXT NOT NULL,
    temperature_max_c   REAL,
    temperature_min_c   REAL,
    precipitation_mm    REAL,
    wind_speed_max_kmh  REAL,
    sunshine_hours      REAL,
    loaded_at           TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (country_name, date),
    FOREIGN KEY (country_name) REFERENCES countries(country_name)
);
"""

# ---------------------------------------------------------------------------
# Analytical Views
# ---------------------------------------------------------------------------

VIEW_AVG_TEMPERATURE_RANKING = """
CREATE VIEW IF NOT EXISTS v_avg_temperature_ranking AS
SELECT
    country_name,
    ROUND(AVG((temperature_max_c + temperature_min_c) / 2.0), 2) AS avg_temperature_c,
    ROUND(AVG(temperature_max_c), 2)                              AS avg_max_c,
    ROUND(AVG(temperature_min_c), 2)                              AS avg_min_c,
    COUNT(*)                                                       AS days_observed
FROM daily_weather
WHERE temperature_max_c IS NOT NULL
  AND temperature_min_c IS NOT NULL
GROUP BY country_name
ORDER BY avg_temperature_c DESC;
"""

VIEW_MOST_RAINFALL = """
CREATE VIEW IF NOT EXISTS v_most_rainfall AS
SELECT
    country_name,
    ROUND(SUM(precipitation_mm), 2)                AS total_precipitation_mm,
    ROUND(AVG(precipitation_mm), 2)                AS avg_daily_precipitation_mm,
    COUNT(*)                                       AS days_observed
FROM daily_weather
WHERE precipitation_mm IS NOT NULL
GROUP BY country_name
ORDER BY total_precipitation_mm DESC;
"""

VIEW_30DAY_SUMMARY = """
CREATE VIEW IF NOT EXISTS v_30day_summary AS
SELECT
    dw.country_name,
    c.capital_city,
    c.population,
    c.area_km2,
    COUNT(*)                                                        AS days_observed,
    MIN(dw.date)                                                    AS period_start,
    MAX(dw.date)                                                    AS period_end,
    ROUND(AVG((dw.temperature_max_c + dw.temperature_min_c) / 2.0), 2) AS avg_temperature_c,
    ROUND(MIN(dw.temperature_min_c), 2)                             AS min_temperature_c,
    ROUND(MAX(dw.temperature_max_c), 2)                             AS max_temperature_c,
    ROUND(SUM(dw.precipitation_mm), 2)                              AS total_precipitation_mm,
    ROUND(AVG(dw.precipitation_mm), 2)                              AS avg_daily_precipitation_mm,
    ROUND(MAX(dw.wind_speed_max_kmh), 2)                            AS peak_wind_speed_kmh,
    ROUND(AVG(dw.wind_speed_max_kmh), 2)                            AS avg_wind_speed_kmh,
    ROUND(SUM(dw.sunshine_hours), 2)                                AS total_sunshine_hours,
    ROUND(AVG(dw.sunshine_hours), 2)                                AS avg_daily_sunshine_hours
FROM daily_weather dw
JOIN countries c ON c.country_name = dw.country_name
GROUP BY dw.country_name
ORDER BY dw.country_name;
"""

# ---------------------------------------------------------------------------
# All DDL and views grouped for convenience
# ---------------------------------------------------------------------------

ALL_DDL = DDL_RAW_COUNTRIES + DDL_RAW_WEATHER + DDL_COUNTRIES + DDL_DAILY_WEATHER

ALL_VIEW_NAMES = ("v_avg_temperature_ranking", "v_most_rainfall", "v_30day_summary")

ALL_VIEWS = VIEW_AVG_TEMPERATURE_RANKING + VIEW_MOST_RAINFALL + VIEW_30DAY_SUMMARY
