"""
End-to-end: raw anchor CSV → build_vnm_anchor_valuation → parquet → run_daily_valuation.

Exercises blending, validation flags, and valuation on the processed anchor read from disk
without writing to the repo's data/processed/ tree.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from vnm_valuation.valuation import run_daily_valuation

from tests.reviewed_anchor_fixtures import (
    AS_OF,
    CLOSE_2026_04_16,
    EXPECTED_FINAL_FAIR_VALUE_REF,
    FINAL_FAIR_VALUE_ABS_TOL,
    REVIEWED_ANCHOR_FAIR_VALUE,
    REVIEWED_VALUATION_DATE,
    fx_and_cost_dataframes,
    market_dataframe,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _raw_csv_reviewed_snapshot() -> str:
    """Minimal multi-row raw file: older row + reviewed 2026-03-31 (method inputs blend to 70380)."""
    return (
        "valuation_date,ticker,dcf_value,ev_ebitda_value,pe_value,source,notes,anchor_validated\n"
        "2024-03-15,VNM,95000,98000,96000,Q1-2024 workbook,placeholder series,false\n"
        f"2026-03-31,VNM,69800,71200,70600,Q1-2026 equity model (v3),"
        f"approved production anchor snapshot,true\n"
    )


def test_e2e_raw_anchor_build_then_valuation(tmp_path: Path):
    raw_path = tmp_path / "vnm_anchor_valuation.csv"
    raw_path.write_text(_raw_csv_reviewed_snapshot(), encoding="utf-8")
    out_parquet = tmp_path / "vnm_anchor_valuation.parquet"

    repo = _repo_root()
    script = repo / "scripts" / "build_vnm_anchor_valuation.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--input",
            str(raw_path),
            "--output",
            str(out_parquet),
        ],
        cwd=str(repo),
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + "\n" + proc.stderr

    built = pd.read_parquet(out_parquet)
    assert not built.empty
    snap = built.loc[built["valuation_date"].astype(str) == REVIEWED_VALUATION_DATE].iloc[-1]
    assert float(snap["anchor_fair_value"]) == pytest.approx(REVIEWED_ANCHOR_FAIR_VALUE, abs=0.01)
    assert bool(snap["anchor_validated"]) is True

    market_df = market_dataframe()
    fx_df, input_cost_df = fx_and_cost_dataframes()
    anchor_df = pd.read_parquet(out_parquet)

    out = run_daily_valuation(AS_OF, market_df, fx_df, input_cost_df, anchor_df)
    assert len(out) == 1
    row = out.iloc[0]

    assert row["valuation_mode"] == "anchor_adjusted"
    assert bool(row["anchor_used"]) is True
    assert row["anchor_status"] == "used"

    anchor = float(row["anchor_fair_value"])
    adj = float(row["adjustment_pct"])
    final = float(row["final_fair_value"])

    assert anchor == pytest.approx(REVIEWED_ANCHOR_FAIR_VALUE, abs=0.01)
    assert float(row["close"]) == CLOSE_2026_04_16
    assert final == pytest.approx(anchor * (1.0 + adj), rel=0, abs=1e-6)
    assert final > anchor
    assert final == pytest.approx(EXPECTED_FINAL_FAIR_VALUE_REF, abs=FINAL_FAIR_VALUE_ABS_TOL)
