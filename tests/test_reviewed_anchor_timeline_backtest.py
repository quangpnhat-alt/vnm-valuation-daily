"""Audited reviewed-anchor timeline sweep (DEFAULT_TIMELINE_SWEEP_DATES)."""

from __future__ import annotations

import pandas as pd
import pytest

from vnm_valuation.mini_backtest import (
    DEFAULT_TIMELINE_SWEEP_DATES,
    expected_anchor_fair_value_for_selected_date,
    run_timeline_backtest,
)


def test_timeline_backtest_export_shape_and_selection():
    df = run_timeline_backtest()
    assert len(df) == len(DEFAULT_TIMELINE_SWEEP_DATES)
    assert list(df["as_of_date"]) == DEFAULT_TIMELINE_SWEEP_DATES
    assert "anchor_error_message" in df.columns

    by = df.set_index("as_of_date")

    assert by.loc["2025-04-10", "selected_anchor_date"] == "2025-03-31"
    assert by.loc["2025-07-15", "selected_anchor_date"] == "2025-03-31"
    assert by.loc["2025-10-15", "selected_anchor_date"] == "2025-09-30"
    assert by.loc["2026-01-15", "selected_anchor_date"] == "2025-09-30"
    assert by.loc["2026-04-16", "selected_anchor_date"] == "2026-03-31"

    late = by.loc["2028-06-01"]
    assert late["selected_anchor_date"] == "2026-03-31"
    assert bool(late["anchor_used"]) is False
    assert late["anchor_status"] == "stale"
    assert late["valuation_mode"] == "market_fallback"
    assert pd.isna(late["anchor_fair_value"])
    assert str(late["anchor_error_message"] or "").lower().find("stale") >= 0

    for d in ["2025-04-10", "2025-07-15", "2025-10-15", "2026-01-15", "2026-04-16"]:
        row = by.loc[d]
        assert row["valuation_mode"] == "anchor_adjusted"
        assert bool(row["anchor_used"]) is True
        assert row["anchor_status"] == "used"
        assert str(row["anchor_error_message"] or "") == ""
        sel = str(row["selected_anchor_date"])
        exp = expected_anchor_fair_value_for_selected_date(sel)
        assert exp is not None
        assert float(row["anchor_fair_value"]) == pytest.approx(exp, abs=0.5)
