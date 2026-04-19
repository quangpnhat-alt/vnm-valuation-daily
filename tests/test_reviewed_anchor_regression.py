"""
Regression: reviewed anchor snapshot 2026-03-31 selected for as_of 2026-04-16.

Uses synthetic market / FX / input-cost histories chosen so signals stay in the same
ballpark as a full local pipeline run (~72071 VND final fair value) without depending
on checked-in parquet outputs. Valuation formulas and logic are exercised via the
public `run_daily_valuation` API only.
"""

from __future__ import annotations

import pytest

from vnm_valuation.valuation import run_daily_valuation

from tests.reviewed_anchor_fixtures import (
    AS_OF,
    CLOSE_2026_04_16,
    EXPECTED_FINAL_FAIR_VALUE_REF,
    FINAL_FAIR_VALUE_ABS_TOL,
    REVIEWED_ANCHOR_FAIR_VALUE,
    fx_and_cost_dataframes,
    market_dataframe,
    processed_style_anchor_dataframe,
)


def test_reviewed_anchor_2026_03_31_regression():
    assert REVIEWED_ANCHOR_FAIR_VALUE == pytest.approx(70380.0, abs=1e-9)

    market_df = market_dataframe()
    fx_df, input_cost_df = fx_and_cost_dataframes()
    anchor_df = processed_style_anchor_dataframe()

    out = run_daily_valuation(AS_OF, market_df, fx_df, input_cost_df, anchor_df)
    assert len(out) == 1
    row = out.iloc[0]

    assert row["valuation_mode"] == "anchor_adjusted"
    assert bool(row["anchor_used"]) is True
    assert row["anchor_status"] == "used"
    assert str(row["anchor_error_message"] or "") == ""

    assert float(row["anchor_fair_value"]) == pytest.approx(REVIEWED_ANCHOR_FAIR_VALUE, abs=0.01)
    assert float(row["close"]) == CLOSE_2026_04_16

    anchor = float(row["anchor_fair_value"])
    adj = float(row["adjustment_pct"])
    final = float(row["final_fair_value"])

    assert -0.25 <= adj <= 0.25
    assert final == pytest.approx(anchor * (1.0 + adj), rel=0, abs=1e-6)
    assert final > anchor

    assert final == pytest.approx(EXPECTED_FINAL_FAIR_VALUE_REF, abs=FINAL_FAIR_VALUE_ABS_TOL)

    assert float(row["relative_valuation_signal"]) > 0.0
