"""
Deterministic market/FX/cost/anchor inputs shared by tests and offline mini-backtest tooling.

Does not change valuation rules; only centralizes fixed DataFrames aligned with
`data/raw/vnm_anchor_valuation.csv` reviewed snapshots.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Reviewed raw blend (50% DCF / 30% EV-EBITDA / 20% P/E) → processed anchor_fair_value
_DCF = 69800.0
_EV = 71200.0
_PE = 70600.0
REVIEWED_ANCHOR_FAIR_VALUE = 0.5 * _DCF + 0.3 * _EV + 0.2 * _PE  # 70380.0

_DCF_24Q1 = 65500.0
_EV_24Q1 = 66800.0
_PE_24Q1 = 66100.0
REVIEWED_FAIR_2024_03_31 = 0.5 * _DCF_24Q1 + 0.3 * _EV_24Q1 + 0.2 * _PE_24Q1  # 66010.0

_DCF_24Q3 = 66200.0
_EV_24Q3 = 67800.0
_PE_24Q3 = 66900.0
REVIEWED_FAIR_2024_09_30 = 0.5 * _DCF_24Q3 + 0.3 * _EV_24Q3 + 0.2 * _PE_24Q3  # 66820.0

_DCF_25Q1 = 67500.0
_EV_25Q1 = 69200.0
_PE_25Q1 = 68100.0
REVIEWED_FAIR_2025_03_31 = 0.5 * _DCF_25Q1 + 0.3 * _EV_25Q1 + 0.2 * _PE_25Q1  # 68130.0

_DCF_25Q3 = 68200.0
_EV_25Q3 = 69800.0
_PE_25Q3 = 68800.0
REVIEWED_FAIR_2025_09_30 = 0.5 * _DCF_25Q3 + 0.3 * _EV_25Q3 + 0.2 * _PE_25Q3  # 68800.0

DEFAULT_BACKTEST_CLOSE = 61300.0


def fx_and_cost_dataframes_wide() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Long USD/VND and raw_milk series from 2024-01-01 so 2024–2028 `as_of` dates have
    history for FX/cost signals.
    """
    n = 1200
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
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
    """Validated reviewed rows (processed-style pre-blended anchor_fair_value), oldest first."""
    return pd.DataFrame(
        [
            {
                "ticker": "VNM",
                "anchor_fair_value": REVIEWED_FAIR_2024_03_31,
                "anchor_currency": "VND",
                "valuation_date": "2024-03-31",
                "anchor_validated": True,
                "source": "Q1-2024 equity model (v1)",
                "notes": "Reviewed FY23 close; approved snapshot",
            },
            {
                "ticker": "VNM",
                "anchor_fair_value": REVIEWED_FAIR_2024_09_30,
                "anchor_currency": "VND",
                "valuation_date": "2024-09-30",
                "anchor_validated": True,
                "source": "Q3-2024 equity model (v1)",
                "notes": "H1/H2 review sign-off; approved snapshot",
            },
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
