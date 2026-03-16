"""
View Verification Tests
=======================
Uses a small in-memory SQLite database with known data so that every
aggregation (AVG, SUM, COUNT, ORDER BY) can be checked exactly.

Run:
    python -m pytest test_views.py -v
"""

from __future__ import annotations

import sqlite3

import pytest

from sql_definitions import ALL_DDL, ALL_VIEW_NAMES, ALL_VIEWS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def conn():
    """Create an in-memory database with schema, seed data, and views."""
    db = sqlite3.connect(":memory:")
    db.execute("PRAGMA foreign_keys=ON")
    db.executescript(ALL_DDL)

    # --- Seed countries ---
    db.executemany(
        """INSERT INTO countries
           (country_name, capital_city, latitude, longitude, population, area_km2)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [
            ("Alpha", "AlphaCity", 10.0, 20.0, 1000000, 500.0),
            ("Beta",  "BetaCity",  30.0, 40.0, 2000000, 800.0),
        ],
    )

    # --- Seed weather (3 days each, easy-to-verify numbers) ---
    # Alpha: temps 10/0, 12/2, 14/4  -> avg_temp = (5+7+9)/3 = 7.0
    #         precip 1, 2, 3          -> total = 6.0, avg = 2.0
    #         sunshine 3600, 7200, 10800 sec -> 1, 2, 3 hrs (but we store hours)
    # Beta:  temps 20/10, 22/12, 24/14 -> avg_temp = (15+17+19)/3 = 17.0
    #         precip 10, 20, 30         -> total = 60.0, avg = 20.0
    db.executemany(
        """INSERT INTO daily_weather
           (country_name, date, temperature_max_c, temperature_min_c,
            precipitation_mm, wind_speed_max_kmh, sunshine_hours)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            ("Alpha", "2026-01-01", 10.0, 0.0,  1.0, 15.0, 1.0),
            ("Alpha", "2026-01-02", 12.0, 2.0,  2.0, 20.0, 2.0),
            ("Alpha", "2026-01-03", 14.0, 4.0,  3.0, 25.0, 3.0),
            ("Beta",  "2026-01-01", 20.0, 10.0, 10.0, 30.0, 5.0),
            ("Beta",  "2026-01-02", 22.0, 12.0, 20.0, 35.0, 6.0),
            ("Beta",  "2026-01-03", 24.0, 14.0, 30.0, 40.0, 7.0),
        ],
    )

    for view_name in ALL_VIEW_NAMES:
        db.execute(f"DROP VIEW IF EXISTS {view_name}")
    db.executescript(ALL_VIEWS)

    yield db
    db.close()


# ---------------------------------------------------------------------------
# v_avg_temperature_ranking
# ---------------------------------------------------------------------------

class TestAvgTemperatureRanking:

    def test_row_count(self, conn):
        rows = conn.execute("SELECT * FROM v_avg_temperature_ranking").fetchall()
        assert len(rows) == 2

    def test_ordering_warmest_first(self, conn):
        rows = conn.execute(
            "SELECT country_name FROM v_avg_temperature_ranking"
        ).fetchall()
        assert rows[0][0] == "Beta"
        assert rows[1][0] == "Alpha"

    def test_avg_temperature_values(self, conn):
        rows = conn.execute(
            "SELECT country_name, avg_temperature_c FROM v_avg_temperature_ranking"
        ).fetchall()
        values = {r[0]: r[1] for r in rows}
        # Alpha: avg((10+0)/2, (12+2)/2, (14+4)/2) = avg(5, 7, 9) = 7.0
        assert values["Alpha"] == 7.0
        # Beta: avg((20+10)/2, (22+12)/2, (24+14)/2) = avg(15, 17, 19) = 17.0
        assert values["Beta"] == 17.0

    def test_avg_max_min(self, conn):
        rows = conn.execute(
            "SELECT country_name, avg_max_c, avg_min_c FROM v_avg_temperature_ranking"
        ).fetchall()
        values = {r[0]: (r[1], r[2]) for r in rows}
        assert values["Alpha"] == (12.0, 2.0)   # avg(10,12,14), avg(0,2,4)
        assert values["Beta"] == (22.0, 12.0)    # avg(20,22,24), avg(10,12,14)

    def test_days_observed(self, conn):
        rows = conn.execute(
            "SELECT country_name, days_observed FROM v_avg_temperature_ranking"
        ).fetchall()
        for _, days in rows:
            assert days == 3


# ---------------------------------------------------------------------------
# v_most_rainfall
# ---------------------------------------------------------------------------

class TestMostRainfall:

    def test_row_count(self, conn):
        rows = conn.execute("SELECT * FROM v_most_rainfall").fetchall()
        assert len(rows) == 2

    def test_ordering_wettest_first(self, conn):
        rows = conn.execute(
            "SELECT country_name FROM v_most_rainfall"
        ).fetchall()
        assert rows[0][0] == "Beta"
        assert rows[1][0] == "Alpha"

    def test_total_precipitation(self, conn):
        rows = conn.execute(
            "SELECT country_name, total_precipitation_mm FROM v_most_rainfall"
        ).fetchall()
        values = {r[0]: r[1] for r in rows}
        assert values["Alpha"] == 6.0    # 1 + 2 + 3
        assert values["Beta"] == 60.0    # 10 + 20 + 30

    def test_avg_daily_precipitation(self, conn):
        rows = conn.execute(
            "SELECT country_name, avg_daily_precipitation_mm FROM v_most_rainfall"
        ).fetchall()
        values = {r[0]: r[1] for r in rows}
        assert values["Alpha"] == 2.0    # 6 / 3
        assert values["Beta"] == 20.0    # 60 / 3


# ---------------------------------------------------------------------------
# v_30day_summary
# ---------------------------------------------------------------------------

class TestSummary30Day:

    def test_row_count(self, conn):
        rows = conn.execute("SELECT * FROM v_30day_summary").fetchall()
        assert len(rows) == 2

    def test_ordered_alphabetically(self, conn):
        rows = conn.execute(
            "SELECT country_name FROM v_30day_summary"
        ).fetchall()
        assert rows[0][0] == "Alpha"
        assert rows[1][0] == "Beta"

    def test_joins_country_info(self, conn):
        rows = conn.execute(
            "SELECT country_name, capital_city, population, area_km2 "
            "FROM v_30day_summary"
        ).fetchall()
        values = {r[0]: r[1:] for r in rows}
        assert values["Alpha"] == ("AlphaCity", 1000000, 500.0)
        assert values["Beta"] == ("BetaCity", 2000000, 800.0)

    def test_date_range(self, conn):
        rows = conn.execute(
            "SELECT country_name, period_start, period_end FROM v_30day_summary"
        ).fetchall()
        for _, start, end in rows:
            assert start == "2026-01-01"
            assert end == "2026-01-03"

    def test_temperature_aggregations(self, conn):
        rows = conn.execute(
            "SELECT country_name, avg_temperature_c, min_temperature_c, max_temperature_c "
            "FROM v_30day_summary"
        ).fetchall()
        values = {r[0]: r[1:] for r in rows}
        # Alpha: avg=7.0, min=0.0, max=14.0
        assert values["Alpha"] == (7.0, 0.0, 14.0)
        # Beta: avg=17.0, min=10.0, max=24.0
        assert values["Beta"] == (17.0, 10.0, 24.0)

    def test_precipitation_aggregations(self, conn):
        rows = conn.execute(
            "SELECT country_name, total_precipitation_mm, avg_daily_precipitation_mm "
            "FROM v_30day_summary"
        ).fetchall()
        values = {r[0]: r[1:] for r in rows}
        assert values["Alpha"] == (6.0, 2.0)
        assert values["Beta"] == (60.0, 20.0)

    def test_wind_aggregations(self, conn):
        rows = conn.execute(
            "SELECT country_name, peak_wind_speed_kmh, avg_wind_speed_kmh "
            "FROM v_30day_summary"
        ).fetchall()
        values = {r[0]: r[1:] for r in rows}
        assert values["Alpha"] == (25.0, 20.0)   # max(15,20,25), avg(15,20,25)
        assert values["Beta"] == (40.0, 35.0)     # max(30,35,40), avg(30,35,40)

    def test_sunshine_aggregations(self, conn):
        rows = conn.execute(
            "SELECT country_name, total_sunshine_hours, avg_daily_sunshine_hours "
            "FROM v_30day_summary"
        ).fetchall()
        values = {r[0]: r[1:] for r in rows}
        assert values["Alpha"] == (6.0, 2.0)     # sum(1,2,3), avg(1,2,3)
        assert values["Beta"] == (18.0, 6.0)     # sum(5,6,7), avg(5,6,7)

    def test_days_observed(self, conn):
        rows = conn.execute(
            "SELECT country_name, days_observed FROM v_30day_summary"
        ).fetchall()
        for _, days in rows:
            assert days == 3


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestNullHandling:
    """Verify views handle NULL values gracefully."""

    def test_null_temperatures_excluded_from_ranking(self, conn):
        """Country with all-NULL temps should not appear in ranking."""
        conn.execute(
            """INSERT INTO countries
               (country_name, capital_city, latitude, longitude, population, area_km2)
               VALUES ('Gamma', 'GammaCity', 50.0, 60.0, 500000, 300.0)"""
        )
        conn.execute(
            """INSERT INTO daily_weather
               (country_name, date, temperature_max_c, temperature_min_c,
                precipitation_mm, wind_speed_max_kmh, sunshine_hours)
               VALUES ('Gamma', '2026-01-01', NULL, NULL, 5.0, 10.0, 2.0)"""
        )
        rows = conn.execute(
            "SELECT country_name FROM v_avg_temperature_ranking"
        ).fetchall()
        names = [r[0] for r in rows]
        assert "Gamma" not in names

    def test_null_precipitation_excluded_from_rainfall(self, conn):
        """Country with all-NULL precip should not appear in rainfall view."""
        conn.execute(
            """INSERT INTO countries
               (country_name, capital_city, latitude, longitude, population, area_km2)
               VALUES ('Delta', 'DeltaCity', 55.0, 65.0, 100000, 100.0)"""
        )
        conn.execute(
            """INSERT INTO daily_weather
               (country_name, date, temperature_max_c, temperature_min_c,
                precipitation_mm, wind_speed_max_kmh, sunshine_hours)
               VALUES ('Delta', '2026-01-01', 10.0, 5.0, NULL, 10.0, 2.0)"""
        )
        rows = conn.execute(
            "SELECT country_name FROM v_most_rainfall"
        ).fetchall()
        names = [r[0] for r in rows]
        assert "Delta" not in names
 