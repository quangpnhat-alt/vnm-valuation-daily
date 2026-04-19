"""
Negative end-to-end: raw anchor CSV → build → parquet → run_daily_valuation fallback paths.

Uses temp files and `scripts/build_vnm_anchor_valuation.py --output` so repo `data/` is untouched.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from vnm_valuation.valuation import run_daily_valuation

from tests.reviewed_anchor_fixtures import (
    AS_OF,
    CLOSE_2026_04_16,
    fx_and_cost_dataframes,
    market_dataframe_for,
    run_anchor_builder,
)


def _assert_market_fallback(
    row: pd.Series,
    *,
    close: float,
    expected_anchor_status: str,
) -> None:
    assert row["valuation_mode"] == "market_fallback"
    assert bool(row["anchor_used"]) is False
    assert row["anchor_status"] == expected_anchor_status
    assert pd.isna(row["anchor_fair_value"])

    adj = float(row["adjustment_pct"])
    final = float(row["final_fair_value"])
    assert final == pytest.approx(close * (1.0 + adj), rel=0, abs=1e-4)

    assert float(row["relative_valuation_signal"]) == 0.0
    assert float(row["sentiment_signal"]) == 0.0
    assert str(row["anchor_error_message"] or "").strip() != ""


@pytest.mark.parametrize(
    "raw_csv",
    [
        pytest.param(
            # Keyword "placeholder" in notes → implicit not validated (no explicit column).
            "valuation_date,ticker,dcf_value,ev_ebitda_value,pe_value,source,notes\n"
            "2026-03-31,VNM,69800,71200,70600,Q1-2026 workbook,placeholder series for testing\n",
            id="placeholder_in_notes",
        ),
        pytest.param(
            # Explicit anchor_validated=false; row is on/before as_of and numerically valid.
            "valuation_date,ticker,dcf_value,ev_ebitda_value,pe_value,source,notes,anchor_validated\n"
            "2026-03-31,VNM,69800,71200,70600,Q1-2026 model,Signed-off numbers; awaiting governance promote,false\n",
            id="anchor_validated_false",
        ),
    ],
)
def test_e2e_fallback_unvalidated_anchor(tmp_path: Path, raw_csv: str):
    raw = tmp_path / "raw.csv"
    raw.write_text(raw_csv, encoding="utf-8")
    out_pq = tmp_path / "anchor.parquet"
    run_anchor_builder(raw_path=raw, out_parquet=out_pq)

    built = pd.read_parquet(out_pq)
    row31 = built.loc[built["valuation_date"].astype(str) == "2026-03-31"].iloc[-1]
    assert bool(row31["anchor_validated"]) is False

    market_df = market_dataframe_for(AS_OF, close=CLOSE_2026_04_16)
    fx_df, input_cost_df = fx_and_cost_dataframes()
    out = run_daily_valuation(AS_OF, market_df, fx_df, input_cost_df, pd.read_parquet(out_pq))

    r = out.iloc[0]
    _assert_market_fallback(r, close=CLOSE_2026_04_16, expected_anchor_status="unvalidated")
    assert "validated" in str(r["anchor_error_message"]).lower()


def test_e2e_fallback_stale_anchor(tmp_path: Path):
    """Validated anchor row exists on/before as_of but is older than the stale threshold."""
    raw = tmp_path / "raw.csv"
    raw.write_text(
        "valuation_date,ticker,dcf_value,ev_ebitda_value,pe_value,source,notes,anchor_validated\n"
        "2024-03-15,VNM,67500,68800,68100,FY23 equity model v2,Reviewed FY23 close; production snapshot,true\n",
        encoding="utf-8",
    )
    out_pq = tmp_path / "anchor.parquet"
    run_anchor_builder(raw_path=raw, out_parquet=out_pq)

    market_df = market_dataframe_for(AS_OF, close=CLOSE_2026_04_16)
    fx_df, input_cost_df = fx_and_cost_dataframes()
    out = run_daily_valuation(AS_OF, market_df, fx_df, input_cost_df, pd.read_parquet(out_pq))

    r = out.iloc[0]
    _assert_market_fallback(r, close=CLOSE_2026_04_16, expected_anchor_status="stale")
    assert "stale" in str(r["anchor_error_message"]).lower()


def test_e2e_fallback_missing_anchor_on_or_before_as_of(tmp_path: Path):
    """No anchor with valuation_date <= as_of; future-dated row only → missing."""
    raw = tmp_path / "raw.csv"
    raw.write_text(
        "valuation_date,ticker,dcf_value,ev_ebitda_value,pe_value,source,notes,anchor_validated\n"
        "2027-06-30,VNM,72000,73000,72500,Future model,Forward scenario only,true\n",
        encoding="utf-8",
    )
    out_pq = tmp_path / "anchor.parquet"
    run_anchor_builder(raw_path=raw, out_parquet=out_pq)

    market_df = market_dataframe_for(AS_OF, close=CLOSE_2026_04_16)
    fx_df, input_cost_df = fx_and_cost_dataframes()
    out = run_daily_valuation(AS_OF, market_df, fx_df, input_cost_df, pd.read_parquet(out_pq))

    r = out.iloc[0]
    _assert_market_fallback(r, close=CLOSE_2026_04_16, expected_anchor_status="missing")
    assert "on or before" in str(r["anchor_error_message"]).lower()
