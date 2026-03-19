"""
Transform Function Tests
=========================
Unit tests for the transform layer: country parsing, weather parsing,
and the _parse_float_or_none helper.

Run:
    python -m pytest test_transforms.py -v
"""

from __future__ import annotations

import math

from etl_pipeline import _parse_float_or_none, transform_countries, transform_weather


# ---------------------------------------------------------------------------
# _parse_float_or_none
# ---------------------------------------------------------------------------

class TestSafeFloat:

    def test_normal_number(self):
        assert _parse_float_or_none(12.5) == 12.5

    def test_integer(self):
        assert _parse_float_or_none(7) == 7.0

    def test_string_number(self):
        assert _parse_float_or_none("3.14") == 3.14

    def test_none_returns_none(self):
        assert _parse_float_or_none(None) is None

    def test_nan_returns_none(self):
        assert _parse_float_or_none(float("nan")) is None

    def test_non_numeric_string_returns_none(self):
        assert _parse_float_or_none("abc") is None

    def test_zero(self):
        assert _parse_float_or_none(0) == 0.0

    def test_negative(self):
        assert _parse_float_or_none(-5.5) == -5.5


# ---------------------------------------------------------------------------
# transform_countries
# ---------------------------------------------------------------------------

class TestTransformCountries:

    def _make_country(self, **overrides):
        """Build a valid RestCountries-style dict, with optional overrides."""
        base = {
            "name": {"common": "Testland"},
            "capital": ["Testville"],
            "capitalInfo": {"latlng": [59.43, 24.74]},
            "population": 1000000,
            "area": 45000.0,
        }
        base.update(overrides)
        return base

    def test_valid_country(self):
        result = transform_countries([self._make_country()])
        assert len(result) == 1
        assert result[0]["country_name"] == "Testland"
        assert result[0]["capital_city"] == "Testville"
        assert result[0]["latitude"] == 59.43
        assert result[0]["longitude"] == 24.74
        assert result[0]["population"] == 1000000
        assert result[0]["area_km2"] == 45000.0

    def test_missing_capital_skipped(self):
        result = transform_countries([self._make_country(capital=None)])
        assert len(result) == 0

    def test_empty_capital_list_skipped(self):
        result = transform_countries([self._make_country(capital=[])])
        assert len(result) == 0

    def test_missing_coordinates_skipped(self):
        result = transform_countries([
            self._make_country(capitalInfo={}),
        ])
        assert len(result) == 0

    def test_empty_latlng_skipped(self):
        result = transform_countries([
            self._make_country(capitalInfo={"latlng": []}),
        ])
        assert len(result) == 0

    def test_partial_latlng_skipped(self):
        result = transform_countries([
            self._make_country(capitalInfo={"latlng": [59.43]}),
        ])
        assert len(result) == 0

    def test_missing_population_defaults_zero(self):
        raw = self._make_country()
        del raw["population"]
        result = transform_countries([raw])
        assert result[0]["population"] == 0

    def test_missing_area_defaults_zero(self):
        raw = self._make_country()
        del raw["area"]
        result = transform_countries([raw])
        assert result[0]["area_km2"] == 0.0

    def test_multiple_countries_mixed(self):
        """Valid + invalid countries — only valid ones pass through."""
        countries = [
            self._make_country(),
            self._make_country(name={"common": "NoCapital"}, capital=None),
            self._make_country(name={"common": "NoCoords"}, capitalInfo={}),
        ]
        result = transform_countries(countries)
        assert len(result) == 1
        assert result[0]["country_name"] == "Testland"


# ---------------------------------------------------------------------------
# transform_weather
# ---------------------------------------------------------------------------

class TestTransformWeather:

    SAMPLE_RAW = {
        "daily": {
            "time": ["2026-01-01", "2026-01-02"],
            "temperature_2m_max": [5.0, 8.0],
            "temperature_2m_min": [-1.0, 2.0],
            "precipitation_sum": [0.5, 3.2],
            "wind_speed_10m_max": [20.0, 15.0],
            "sunshine_duration": [14400, 28800],  # 4h, 8h in seconds
        }
    }

    def test_row_count_matches_dates(self):
        rows = transform_weather("Estonia", self.SAMPLE_RAW)
        assert len(rows) == 2

    def test_country_name_set(self):
        rows = transform_weather("Estonia", self.SAMPLE_RAW)
        assert all(r["country_name"] == "Estonia" for r in rows)

    def test_dates_preserved(self):
        rows = transform_weather("Estonia", self.SAMPLE_RAW)
        assert rows[0]["date"] == "2026-01-01"
        assert rows[1]["date"] == "2026-01-02"

    def test_temperatures(self):
        rows = transform_weather("Estonia", self.SAMPLE_RAW)
        assert rows[0]["temperature_max_c"] == 5.0
        assert rows[0]["temperature_min_c"] == -1.0
        assert rows[1]["temperature_max_c"] == 8.0
        assert rows[1]["temperature_min_c"] == 2.0

    def test_precipitation(self):
        rows = transform_weather("Estonia", self.SAMPLE_RAW)
        assert rows[0]["precipitation_mm"] == 0.5
        assert rows[1]["precipitation_mm"] == 3.2

    def test_wind_speed(self):
        rows = transform_weather("Estonia", self.SAMPLE_RAW)
        assert rows[0]["wind_speed_max_kmh"] == 20.0
        assert rows[1]["wind_speed_max_kmh"] == 15.0

    def test_sunshine_converted_to_hours(self):
        rows = transform_weather("Estonia", self.SAMPLE_RAW)
        assert rows[0]["sunshine_hours"] == 4.0    # 14400 / 3600
        assert rows[1]["sunshine_hours"] == 8.0    # 28800 / 3600

    def test_null_values_handled(self):
        raw = {
            "daily": {
                "time": ["2026-01-01"],
                "temperature_2m_max": [None],
                "temperature_2m_min": [None],
                "precipitation_sum": [None],
                "wind_speed_10m_max": [None],
                "sunshine_duration": [None],
            }
        }
        rows = transform_weather("Estonia", raw)
        assert rows[0]["temperature_max_c"] is None
        assert rows[0]["temperature_min_c"] is None
        assert rows[0]["precipitation_mm"] is None
        assert rows[0]["wind_speed_max_kmh"] is None
        assert rows[0]["sunshine_hours"] is None

    def test_nan_values_become_none(self):
        raw = {
            "daily": {
                "time": ["2026-01-01"],
                "temperature_2m_max": [float("nan")],
                "temperature_2m_min": [float("nan")],
                "precipitation_sum": [float("nan")],
                "wind_speed_10m_max": [float("nan")],
                "sunshine_duration": [float("nan")],
            }
        }
        rows = transform_weather("Estonia", raw)
        assert rows[0]["temperature_max_c"] is None
        assert rows[0]["sunshine_hours"] is None

    def test_empty_daily_returns_empty(self):
        rows = transform_weather("Estonia", {"daily": {}})
        assert rows == []

    def test_missing_daily_key_returns_empty(self):
        rows = transform_weather("Estonia", {})
        assert rows == []
