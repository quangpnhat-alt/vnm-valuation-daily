"""
Multi-date smoke: latest validated reviewed anchor is selected for each `as_of_date`.

Uses processed-style anchor rows matching reviewed snapshots in `data/raw/vnm_anchor_valuation.csv`.
"""

from __future__ import annotations

import pytest

from vnm_valuation.valuation import run_daily_valuation

from tests.reviewed_anchor_fixtures import (
    CLOSE_2026_04_16,
    REVIEWED_ANCHOR_FAIR_VALUE,
    REVIEWED_FAIR_2024_03_31,
    REVIEWED_FAIR_2024_09_30,
    REVIEWED_FAIR_2025_03_31,
    REVIEWED_FAIR_2025_09_30,
    fx_and_cost_dataframes_wide,
    market_dataframe_for,
    reviewed_snapshot_timeline_dataframe,
)


@pytest.mark.parametrize(
    "as_of_date,expected_anchor_fair_value",
    [
        ("2024-04-20", REVIEWED_FAIR_2024_03_31),
        ("2024-11-01", REVIEWED_FAIR_2024_09_30),
        ("2025-04-10", REVIEWED_FAIR_2025_03_31),
        ("2025-10-15", REVIEWED_FAIR_2025_09_30),
        ("2026-04-16", REVIEWED_ANCHOR_FAIR_VALUE),
    ],
)
def test_reviewed_snapshot_selected_for_as_of_across_timeline(as_of_date: str, expected_anchor_fair_value: float):
    anchor_df = reviewed_snapshot_timeline_dataframe()
    market_df = market_dataframe_for(as_of_date, close=CLOSE_2026_04_16)
    fx_df, input_cost_df = fx_and_cost_dataframes_wide()

    out = run_daily_valuation(as_of_date, market_df, fx_df, input_cost_df, anchor_df)
    row = out.iloc[0]

    assert bool(row["anchor_used"]) is True
    assert row["anchor_status"] == "used"
    assert row["valuation_mode"] == "anchor_adjusted"
    assert float(row["anchor_fair_value"]) == pytest.approx(expected_anchor_fair_value, abs=0.5)
    assert str(row["anchor_error_message"] or "") == ""


def test_stale_fallback_when_reviewed_snapshot_too_old_and_no_newer_row():
    """
    Latest snapshot on/before a far `as_of` is still 2026-03-31; age vs as_of exceeds stale threshold.
    """
    anchor_df = reviewed_snapshot_timeline_dataframe()
    as_of_late = "2028-06-01"
    market_df = market_dataframe_for(as_of_late, close=CLOSE_2026_04_16)
    fx_df, input_cost_df = fx_and_cost_dataframes_wide()

    out = run_daily_valuation(as_of_late, market_df, fx_df, input_cost_df, anchor_df)
    row = out.iloc[0]

    assert bool(row["anchor_used"]) is False
    assert row["anchor_status"] == "stale"
    assert row["valuation_mode"] == "market_fallback"
    assert "stale" in str(row["anchor_error_message"]).lower()
