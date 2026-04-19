"""
Negative end-to-end: raw anchor CSV → build → parquet → run_daily_valuation fallback paths.

Uses temp files and `scripts/build_vnm_anchor_valuation.py --output` so repo `data/` is untouched.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from vnm_valuation.valuation import run_daily_valuation

from tests.reviewed_anchor_fixtures import AS_OF, CLOSE_2026_04_16, fx_and_cost_dataframes, market_dataframe_for


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run_build(raw_csv: str, out_parquet: Path) -> None:
    repo = _repo_root()
    script = repo / "scripts" / "build_vnm_anchor_valuation.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--input",
            str(raw_csv),
            "--output",
            str(out_parquet),
        ],
        cwd=str(repo),
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + "\n" + proc.stderr


def _fallback_assertions(row: pd.Series, *, close: float) -> None:
    assert row["valuation_mode"] == "market_fallback"
    assert bool(row["anchor_used"]) is False
    assert str(row["anchor_status"]) in ("unvalidated", "stale", "missing")
    assert pd.isna(row["anchor_fair_value"])
    adj = float(row["adjustment_pct"])
    final = float(row["final_fair_value"])
    assert final == pytest.approx(close * (1.0 + adj), rel=0, abs=1e-4)
    assert float(row["relative_valuation_signal"]) == 0.0
    assert float(row["sentiment_signal"]) == 0.0


def test_e2e_placeholder_notes_unvalidated_fallback(tmp_path: Path):
    """Keyword 'placeholder' in notes → not validated → market_fallback."""
    raw = tmp_path / "raw.csv"
    raw.write_text(
        "valuation_date,ticker,dcf_value,ev_ebitda_value,pe_value,source,notes\n"
        "2026-03-31,VNM,69800,71200,70600,Q1-2026 workbook,placeholder series for testing\n",
        encoding="utf-8",
    )
    out_pq = tmp_path / "anchor.parquet"
    _run_build(raw, out_pq)

    built = pd.read_parquet(out_pq)
    assert bool(built.loc[built["valuation_date"].astype(str) == "2026-03-31", "anchor_validated"].iloc[-1]) is False

    market_df = market_dataframe_for(AS_OF, close=CLOSE_2026_04_16)
    fx_df, input_cost_df = fx_and_cost_dataframes()
    out = run_daily_valuation(AS_OF, market_df, fx_df, input_cost_df, pd.read_parquet(out_pq))
    _fallback_assertions(out.iloc[0], close=CLOSE_2026_04_16)
    assert "not validated" in str(out.iloc[0]["anchor_error_message"]).lower()


def test_e2e_anchor_validated_false_unvalidated_fallback(tmp_path: Path):
    """Explicit anchor_validated=false → market_fallback."""
    raw = tmp_path / "raw.csv"
    raw.write_text(
        "valuation_date,ticker,dcf_value,ev_ebitda_value,pe_value,source,notes,anchor_validated\n"
        "2026-03-31,VNM,69800,71200,70600,Q1-2026 model,Draft — do not use in production,false\n",
        encoding="utf-8",
    )
    out_pq = tmp_path / "anchor.parquet"
    _run_build(raw, out_pq)

    market_df = market_dataframe_for(AS_OF, close=CLOSE_2026_04_16)
    fx_df, input_cost_df = fx_and_cost_dataframes()
    out = run_daily_valuation(AS_OF, market_df, fx_df, input_cost_df, pd.read_parquet(out_pq))
    _fallback_assertions(out.iloc[0], close=CLOSE_2026_04_16)
    assert out.iloc[0]["anchor_status"] == "unvalidated"


def test_e2e_stale_anchor_fallback(tmp_path: Path):
    """Latest anchor on/before as_of is older than 365 days → stale → market_fallback."""
    raw = tmp_path / "raw.csv"
    raw.write_text(
        "valuation_date,ticker,dcf_value,ev_ebitda_value,pe_value,source,notes,anchor_validated\n"
        "2024-03-15,VNM,67500,68800,68100,FY23 equity model v2,Reviewed FY23 close; production snapshot,true\n",
        encoding="utf-8",
    )
    out_pq = tmp_path / "anchor.parquet"
    _run_build(raw, out_pq)

    market_df = market_dataframe_for(AS_OF, close=CLOSE_2026_04_16)
    fx_df, input_cost_df = fx_and_cost_dataframes()
    out = run_daily_valuation(AS_OF, market_df, fx_df, input_cost_df, pd.read_parquet(out_pq))
    _fallback_assertions(out.iloc[0], close=CLOSE_2026_04_16)
    assert out.iloc[0]["anchor_status"] == "stale"
    assert "stale" in str(out.iloc[0]["anchor_error_message"]).lower()


def test_e2e_no_anchor_on_or_before_as_of_missing_fallback(tmp_path: Path):
    """No anchor row with valuation_date <= as_of → missing → market_fallback."""
    raw = tmp_path / "raw.csv"
    raw.write_text(
        "valuation_date,ticker,dcf_value,ev_ebitda_value,pe_value,source,notes,anchor_validated\n"
        "2027-06-30,VNM,72000,73000,72500,Future model,Forward scenario only,true\n",
        encoding="utf-8",
    )
    out_pq = tmp_path / "anchor.parquet"
    _run_build(raw, out_pq)

    market_df = market_dataframe_for(AS_OF, close=CLOSE_2026_04_16)
    fx_df, input_cost_df = fx_and_cost_dataframes()
    out = run_daily_valuation(AS_OF, market_df, fx_df, input_cost_df, pd.read_parquet(out_pq))
    _fallback_assertions(out.iloc[0], close=CLOSE_2026_04_16)
    assert out.iloc[0]["anchor_status"] == "missing"
