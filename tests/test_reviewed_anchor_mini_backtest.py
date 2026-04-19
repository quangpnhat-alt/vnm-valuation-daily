"""High-level checks for deterministic reviewed-anchor mini-backtest sweep."""

from __future__ import annotations

import pandas as pd
import pytest

from vnm_valuation.mini_backtest import (
    DEFAULT_SWEEP_DATES,
    expected_anchor_fair_value_for_selected_date,
    run_mini_backtest,
)


def test_mini_backtest_row_count_and_anchor_selection():
    df = run_mini_backtest()
    assert len(df) == len(DEFAULT_SWEEP_DATES)
    assert list(df["as_of_date"]) == DEFAULT_SWEEP_DATES

    by = df.set_index("as_of_date")

    assert by.loc["2025-04-01", "selected_anchor_date"] == "2025-03-31"
    assert by.loc["2025-07-15", "selected_anchor_date"] == "2025-03-31"
    assert by.loc["2025-10-01", "selected_anchor_date"] == "2025-09-30"
    assert by.loc["2026-01-15", "selected_anchor_date"] == "2025-09-30"
    assert by.loc["2026-04-16", "selected_anchor_date"] == "2026-03-31"

    late = by.loc["2028-06-01"]
    assert late["selected_anchor_date"] == "2026-03-31"
    assert bool(late["anchor_used"]) is False
    assert late["anchor_status"] == "stale"
    assert late["valuation_mode"] == "market_fallback"
    assert pd.isna(late["anchor_fair_value"])

    assert "anchor_error_message" in df.columns

    for d in ["2025-04-01", "2025-07-15", "2025-10-01", "2026-01-15", "2026-04-16"]:
        row = by.loc[d]
        assert row["valuation_mode"] == "anchor_adjusted"
        assert bool(row["anchor_used"]) is True
        assert row["anchor_status"] == "used"
        assert str(row["anchor_error_message"] or "") == ""
        sel = str(row["selected_anchor_date"])
        exp = expected_anchor_fair_value_for_selected_date(sel)
        assert exp is not None
        assert float(row["anchor_fair_value"]) == pytest.approx(exp, abs=0.5)


def test_write_mini_backtest_csv_roundtrip(tmp_path):
    from pathlib import Path

    from vnm_valuation.mini_backtest import write_mini_backtest_csv

    df = run_mini_backtest()
    p = Path(tmp_path) / "sweep.csv"
    write_mini_backtest_csv(p, df)
    assert p.exists()
    back = pd.read_csv(p)
    assert len(back) == len(df)
