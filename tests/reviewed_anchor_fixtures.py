"""
Shared deterministic inputs for reviewed-anchor regression and end-to-end tests.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Reviewed raw blend (50% DCF / 30% EV-EBITDA / 20% P/E) → processed anchor_fair_value
_DCF = 69800.0
_EV = 71200.0
_PE = 70600.0
REVIEWED_ANCHOR_FAIR_VALUE = 0.5 * _DCF + 0.3 * _EV + 0.2 * _PE  # 70380.0

AS_OF = "2026-04-16"
REVIEWED_VALUATION_DATE = "2026-03-31"
CLOSE_2026_04_16 = 61300.0

# Calibrated so combined adjustment is in the same ballpark as a full local pipeline run.
EXPECTED_FINAL_FAIR_VALUE_REF = 72071.0196606344
FINAL_FAIR_VALUE_ABS_TOL = 1.5


def market_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [{"date": AS_OF, "ticker": "VNM", "close": CLOSE_2026_04_16, "currency": "VND"}]
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
