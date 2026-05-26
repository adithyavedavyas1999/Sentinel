from __future__ import annotations

from datetime import date

from sentinel.sensors.tlc_freshness import _next_month


def test_next_month_rolls_year():
    assert _next_month(date(2024, 12, 1)) == date(2025, 1, 1)


def test_next_month_within_year():
    assert _next_month(date(2024, 3, 1)) == date(2024, 4, 1)
