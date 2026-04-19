from __future__ import annotations

from collections.abc import Iterable, Sequence

import pandas as pd

#
# Canonical schemas (column names) for the MVP.
# Keep these explicit and stable; evolve with intentional versioned changes.
#


# 1) Daily market data for VNM (HOSE: VNM)
VNM_DAILY_MARKET_REQUIRED_COLUMNS: list[str] = [
    "date",  # YYYY-MM-DD
    "ticker",  # e.g. "VNM"
    "close",  # local currency price
]

VNM_DAILY_MARKET_OPTIONAL_COLUMNS: list[str] = [
    "open",
    "high",
    "low",
    "volume",
    "adj_close",
    "currency",  # e.g. "VND"
    "source",
]


# 2) Daily rates / FX data
DAILY_FX_REQUIRED_COLUMNS: list[str] = [
    "date",  # YYYY-MM-DD
    "base_ccy",  # e.g. "USD"
    "quote_ccy",  # e.g. "VND"
    "rate",  # base->quote
]

DAILY_FX_OPTIONAL_COLUMNS: list[str] = [
    "source",
]


# 3) Daily input cost data (placeholder for MVP; can be extended)
DAILY_INPUT_COST_REQUIRED_COLUMNS: list[str] = [
    "date",  # YYYY-MM-DD
    "item",  # e.g. "raw_milk"
    "cost",  # numeric
    "currency",  # e.g. "VND" or "USD"
]

DAILY_INPUT_COST_OPTIONAL_COLUMNS: list[str] = [
    "unit",  # e.g. "kg", "liter"
    "source",
]


# 4) Final valuation output (daily)
DAILY_VALUATION_REQUIRED_COLUMNS: list[str] = [
    "as_of_date",  # YYYY-MM-DD
    "ticker",  # "VNM"
    "price",  # close price (local)
    "currency",  # price currency, e.g. "VND"
    "market_cap",  # local currency
]

DAILY_VALUATION_OPTIONAL_COLUMNS: list[str] = [
    "shares_outstanding",
    "enterprise_value",
    "notes",
    "run_id",
]


def missing_columns(df: pd.DataFrame, required: Sequence[str]) -> list[str]:
    present = set(df.columns.astype(str))
    return [c for c in required if c not in present]


def require_columns(df: pd.DataFrame, required: Sequence[str], *, df_name: str) -> None:
    missing = missing_columns(df, required)
    if missing:
        raise ValueError(
            f"{df_name} is missing required columns: {missing}. "
            f"Present columns: {list(df.columns)}"
        )


def require_any_column(df: pd.DataFrame, candidates: Iterable[str], *, df_name: str) -> str:
    """
    Ensure at least one of the candidate columns exists. Returns the first found.
    """
    present = set(df.columns.astype(str))
    for c in candidates:
        if c in present:
            return c
    raise ValueError(
        f"{df_name} must include at least one of columns: {list(candidates)}. "
        f"Present columns: {list(df.columns)}"
    )

