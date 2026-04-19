"""
Shared deterministic inputs for reviewed-anchor regression and end-to-end tests.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Reviewed raw blend (50% DCF / 30% EV-EBITDA / 20% P/E) → processed anchor_fair_value
_DCF = 69800.0
_EV = 71200.0
_PE = 70600.0
REVIEWED_ANCHOR_FAIR_VALUE = 0.5 * _DCF + 0.3 * _EV + 0.2 * _PE  # 70380.0

# Matches `data/raw/vnm_anchor_valuation.csv` reviewed 2025 snapshots (same blend rule).
_DCF_25Q1 = 67500.0
_EV_25Q1 = 69200.0
_PE_25Q1 = 68100.0
REVIEWED_FAIR_2025_03_31 = 0.5 * _DCF_25Q1 + 0.3 * _EV_25Q1 + 0.2 * _PE_25Q1  # 68130.0

_DCF_25Q3 = 68200.0
_EV_25Q3 = 69800.0
_PE_25Q3 = 68800.0
REVIEWED_FAIR_2025_09_30 = 0.5 * _DCF_25Q3 + 0.3 * _EV_25Q3 + 0.2 * _PE_25Q3  # 68800.0

AS_OF = "2026-04-16"
REVIEWED_VALUATION_DATE = "2026-03-31"
CLOSE_2026_04_16 = 61300.0

# Calibrated so combined adjustment is in the same ballpark as a full local pipeline run.
EXPECTED_FINAL_FAIR_VALUE_REF = 72071.0196606344
FINAL_FAIR_VALUE_ABS_TOL = 1.5


def market_dataframe() -> pd.DataFrame:
    return market_dataframe_for(AS_OF, close=CLOSE_2026_04_16)


def market_dataframe_for(as_of: str, *, close: float) -> pd.DataFrame:
    """Market row for a single `as_of` date (used by positive and fallback e2e tests)."""
    return pd.DataFrame(
        [{"date": as_of, "ticker": "VNM", "close": float(close), "currency": "VND"}]
    )


def fx_and_cost_dataframes() -> tuple[pd.DataFrame, pd.DataFrame]:
    """USD/VND and raw_milk paths long enough for 5D FX and 30D cost signals."""
    n = 120
    dates = pd.date_range("2026-01-01", periods=n, freq="D")
    base_rate = 25000.0
    last_5_bump = 1.006
    rates = np.full(n, base_rate, dtype=float)
    rates[-5:] = base_rate * np.linspace(1.0, last_5_bump, 5)

    end_cost = 1.0725
    costs = np.full(n, 1.0, dtype=float)
    costs[-31:] = np.linspace(1.0, end_cost, 31)

    fx_df = pd.DataFrame(
        {
            "date": dates,
            "base_ccy": "USD",
            "quote_ccy": "VND",
            "rate": rates,
        }
    )
    input_cost_df = pd.DataFrame(
        {
            "date": dates,
            "item": "raw_milk",
            "cost": costs,
            "currency": "USD",
        }
    )
    return fx_df, input_cost_df


def fx_and_cost_dataframes_wide() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Long USD/VND and raw_milk series from 2025-01-01 so 2025–2028 `as_of` dates all have
    history for FX/cost signals (multi-date smoke tests).
    """
    n = 950
    dates = pd.date_range("2025-01-01", periods=n, freq="D")
    base_rate = 25000.0
    last_5_bump = 1.006
    rates = np.full(n, base_rate, dtype=float)
    rates[-5:] = base_rate * np.linspace(1.0, last_5_bump, 5)

    end_cost = 1.0725
    costs = np.full(n, 1.0, dtype=float)
    costs[-31:] = np.linspace(1.0, end_cost, 31)

    fx_df = pd.DataFrame(
        {
            "date": dates,
            "base_ccy": "USD",
            "quote_ccy": "VND",
            "rate": rates,
        }
    )
    input_cost_df = pd.DataFrame(
        {
            "date": dates,
            "item": "raw_milk",
            "cost": costs,
            "currency": "USD",
        }
    )
    return fx_df, input_cost_df


def reviewed_snapshot_timeline_dataframe() -> pd.DataFrame:
    """
    Three validated reviewed rows aligned with `data/raw/vnm_anchor_valuation.csv`
    (processed-style: pre-blended `anchor_fair_value`).
    """
    return pd.DataFrame(
        [
            {
                "ticker": "VNM",
                "anchor_fair_value": REVIEWED_FAIR_2025_03_31,
                "anchor_currency": "VND",
                "valuation_date": "2025-03-31",
                "anchor_validated": True,
                "source": "Q1-2025 equity model (v2)",
                "notes": "Reviewed FY24 close; approved snapshot",
            },
            {
                "ticker": "VNM",
                "anchor_fair_value": REVIEWED_FAIR_2025_09_30,
                "anchor_currency": "VND",
                "valuation_date": "2025-09-30",
                "anchor_validated": True,
                "source": "Q3-2025 equity model (v2)",
                "notes": "H1/H2 bridge review sign-off; approved snapshot",
            },
            {
                "ticker": "VNM",
                "anchor_fair_value": REVIEWED_ANCHOR_FAIR_VALUE,
                "anchor_currency": "VND",
                "valuation_date": "2026-03-31",
                "anchor_validated": True,
                "source": "Q1-2026 equity model (v3)",
                "notes": "DCF and rel-val review sign-off; approved production anchor snapshot",
            },
        ]
    )


def repo_root() -> Path:
    """Project root (parent of `tests/`)."""
    return Path(__file__).resolve().parents[1]


def run_anchor_builder(*, raw_path: Path, out_parquet: Path) -> None:
    """
    Run `scripts/build_vnm_anchor_valuation.py` with --input/--output into temp paths.
    Used by positive and negative anchor E2E tests (no writes under repo data/).
    """
    root = repo_root()
    script = root / "scripts" / "build_vnm_anchor_valuation.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--input",
            str(raw_path),
            "--output",
            str(out_parquet),
        ],
        cwd=str(root),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stdout + "\n" + proc.stderr)


def processed_style_anchor_dataframe() -> pd.DataFrame:
    """Two rows: older + reviewed 2026-03-31 (pre-blended anchor_fair_value)."""
    return pd.DataFrame(
        [
            {
                "ticker": "VNM",
                "anchor_fair_value": 96100.0,
                "anchor_currency": "VND",
                "valuation_date": "2024-03-15",
                "anchor_validated": False,
            },
            {
                "ticker": "VNM",
                "anchor_fair_value": REVIEWED_ANCHOR_FAIR_VALUE,
                "anchor_currency": "VND",
                "valuation_date": REVIEWED_VALUATION_DATE,
                "anchor_validated": True,
                "notes": "approved production anchor snapshot",
                "source": "Q1-2026 equity model (v3)",
            },
        ]
    )
