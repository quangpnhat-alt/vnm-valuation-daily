from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from .schemas import (
    DAILY_FX_REQUIRED_COLUMNS,
    DAILY_INPUT_COST_REQUIRED_COLUMNS,
    VNM_DAILY_MARKET_REQUIRED_COLUMNS,
    require_columns,
)


TICKER = "VNM"


ANCHOR_REQUIRED_COLUMNS: list[str] = [
    "ticker",
    "anchor_fair_value",
    "anchor_currency",
]


@dataclass(frozen=True)
class Signals:
    rate_signal: float
    fx_signal: float
    input_cost_signal: float
    relative_valuation_signal: float
    sentiment_signal: float


def _to_ts(value: Any) -> pd.Timestamp:
    try:
        return pd.Timestamp(value).normalize()
    except Exception as e:
        raise ValueError(f"Invalid as_of_date: {value!r}") from e


def _normalize_date_col(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_datetime(df[col], errors="coerce").dt.normalize()


def _pick_row_for_date(
    df: pd.DataFrame,
    *,
    date_col: str,
    target_date: pd.Timestamp,
    extra_filter: pd.Series | None = None,
    df_name: str,
) -> pd.Series:
    d = df.copy()
    d["_date_norm"] = _normalize_date_col(d, date_col)
    if d["_date_norm"].isna().all():
        raise ValueError(f"{df_name}.{date_col} cannot be parsed as dates")

    mask = d["_date_norm"] == target_date
    if extra_filter is not None:
        mask = mask & extra_filter
    subset = d.loc[mask]
    if subset.empty:
        available_min = d["_date_norm"].min()
        available_max = d["_date_norm"].max()
        raise ValueError(
            f"No {df_name} row found for {target_date.date()} "
            f"(available range: {available_min.date()}..{available_max.date()})"
        )
    return subset.iloc[-1]


STALE_ANCHOR_MAX_AGE_DAYS = 365


def _pick_latest_valid_anchor_row(
    df: pd.DataFrame,
    *,
    date_col: str,
    as_of_date: pd.Timestamp,
    max_age_days: int = STALE_ANCHOR_MAX_AGE_DAYS,
) -> pd.Series:
    """
    Pick the latest anchor row with date_col <= as_of_date.
    Reject if no such row, or if the selected anchor date is older than max_age_days vs as_of_date.
    """
    as_of = as_of_date.normalize()
    d = df.copy()
    d["_date_norm"] = _normalize_date_col(d, date_col)
    d = d.dropna(subset=["_date_norm"])
    if d.empty:
        raise ValueError(f"anchor_df.{date_col} cannot be parsed as dates")

    eligible = d[d["_date_norm"] <= as_of]
    if eligible.empty:
        earliest = d["_date_norm"].min()
        latest = d["_date_norm"].max()
        raise ValueError(
            f"No anchor_df row on or before {as_of.date()} "
            f"(available anchor dates: {earliest.date()}..{latest.date()})"
        )

    max_date = eligible["_date_norm"].max()
    tied = eligible[eligible["_date_norm"] == max_date]
    row = tied.iloc[-1]

    anchor_day = pd.Timestamp(row["_date_norm"]).normalize()
    age_days = (as_of - anchor_day).days
    if age_days > max_age_days:
        raise ValueError(
            f"Anchor is stale: anchor {date_col}={anchor_day.date()} is more than {max_age_days} days "
            f"before as_of_date={as_of.date()}. Refresh anchor inputs."
        )

    out = row.drop(labels=["_date_norm"])
    return out


def _bounded(x: float, *, low: float, high: float) -> float:
    return float(max(low, min(high, x)))


def _pct_change(series: pd.Series, periods: int) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return s.pct_change(periods=periods)


def _compute_fx_signals(as_of_date: pd.Timestamp, fx_df: pd.DataFrame) -> tuple[float, float]:
    """
    Returns (rate_signal, fx_signal).

    MVP approach:
    - Use USD/VND if available; otherwise first available pair for the date range.
    - rate_signal: short-term move (5D pct change) inverted (VND weakening -> negative)
    - fx_signal: deviation vs 20D moving average inverted (weaker VND -> negative)
    """
    fx = fx_df.copy()
    fx["date_norm"] = _normalize_date_col(fx, "date")
    fx = fx.dropna(subset=["date_norm"])

    if fx.empty:
        return 0.0, 0.0

    preferred = fx[(fx["base_ccy"].astype(str).str.upper() == "USD") & (fx["quote_ccy"].astype(str).str.upper() == "VND")]
    pair = preferred if not preferred.empty else fx

    pair = pair.sort_values("date_norm")
    pair = pair.set_index("date_norm")
    rates = pd.to_numeric(pair["rate"], errors="coerce").dropna()
    if rates.empty:
        return 0.0, 0.0

    # Align to as_of_date: use last available on/before date for signals.
    rates = rates.loc[:as_of_date]
    if rates.empty:
        return 0.0, 0.0

    last = float(rates.iloc[-1])
    ma20 = float(rates.rolling(20, min_periods=5).mean().iloc[-1]) if len(rates) >= 5 else float("nan")
    chg5 = float(_pct_change(rates, 5).iloc[-1]) if len(rates) >= 6 else float("nan")

    # If USD/VND rises, VND weakens -> negative for VNM (import costs, macro risk).
    rate_signal = 0.0 if pd.isna(chg5) else _bounded(-chg5, low=-0.05, high=0.05) / 0.05  # scale to [-1, 1]

    fx_dev = 0.0 if (pd.isna(ma20) or ma20 == 0) else (last / ma20 - 1.0)
    fx_signal = _bounded(-fx_dev, low=-0.05, high=0.05) / 0.05  # scale to [-1, 1]

    return float(rate_signal), float(fx_signal)


def _compute_input_cost_signal(as_of_date: pd.Timestamp, input_cost_df: pd.DataFrame) -> float:
    """
    MVP: use `item == "raw_milk"` if present, otherwise the first item.
    Signal is negative when costs are rising (30D pct change).
    """
    costs = input_cost_df.copy()
    costs["date_norm"] = _normalize_date_col(costs, "date")
    costs = costs.dropna(subset=["date_norm"])
    if costs.empty:
        return 0.0

    costs["item_u"] = costs["item"].astype(str).str.lower()
    milk = costs[costs["item_u"] == "raw_milk"]
    series_df = milk if not milk.empty else costs
    series_df = series_df.sort_values("date_norm").set_index("date_norm")
    s = pd.to_numeric(series_df["cost"], errors="coerce").dropna()
    s = s.loc[:as_of_date]
    if len(s) < 2:
        return 0.0

    chg30 = float(_pct_change(s, 30).iloc[-1]) if len(s) >= 31 else float(_pct_change(s, 5).iloc[-1])
    if pd.isna(chg30):
        return 0.0

    # Rising costs -> negative; bound to +/-10%
    return float(_bounded(-chg30, low=-0.10, high=0.10) / 0.10)


def _compute_relative_valuation_signal(close: float, anchor_fair_value: float) -> float:
    """
    Positive when price is below anchor fair value.
    """
    if anchor_fair_value <= 0:
        return 0.0
    gap = anchor_fair_value / close - 1.0 if close > 0 else 0.0
    # Bound to +/-30% gap, scale to [-1, 1]
    return float(_bounded(gap, low=-0.30, high=0.30) / 0.30)


def _compute_sentiment_signal(anchor_row: pd.Series) -> float:
    """
    Optional sentiment in anchor_df:
    - `sentiment_score` expected in [-1, 1]
    - or `sentiment` as a string: negative/neutral/positive
    """
    if "sentiment_score" in anchor_row.index:
        v = pd.to_numeric(anchor_row.get("sentiment_score"), errors="coerce")
        if pd.notna(v):
            return float(_bounded(float(v), low=-1.0, high=1.0))

    if "sentiment" in anchor_row.index:
        s = str(anchor_row.get("sentiment") or "").strip().lower()
        mapping = {"negative": -1.0, "bearish": -1.0, "neutral": 0.0, "positive": 1.0, "bullish": 1.0}
        if s in mapping:
            return float(mapping[s])

    return 0.0


def _combine_adjustment(signals: Signals) -> float:
    """
    Weighted, bounded combination of component signals.
    Returns adjustment_pct in decimal form (e.g. 0.05 == +5%).
    """
    weights = {
        "rate_signal": 0.15,
        "fx_signal": 0.15,
        "input_cost_signal": 0.20,
        "relative_valuation_signal": 0.40,
        "sentiment_signal": 0.10,
    }
    score = (
        weights["rate_signal"] * signals.rate_signal
        + weights["fx_signal"] * signals.fx_signal
        + weights["input_cost_signal"] * signals.input_cost_signal
        + weights["relative_valuation_signal"] * signals.relative_valuation_signal
        + weights["sentiment_signal"] * signals.sentiment_signal
    )
    # Map score (roughly [-1, 1]) to adjustment_pct, capped for safety.
    return float(_bounded(score * 0.20, low=-0.25, high=0.25))


def _pick_anchor_row(anchor_df: pd.DataFrame, *, as_of_date: pd.Timestamp) -> pd.Series:
    """
    Accepts anchor_df with either:
    - a date column: `as_of_date`, `date`, or `valuation_date` (latest row with date <= as_of_date;
      must not be older than STALE_ANCHOR_MAX_AGE_DAYS vs as_of_date)
    - or no date column (single latest row for ticker)
    """
    a = anchor_df.copy()
    a["ticker_u"] = a["ticker"].astype(str).str.upper()
    a = a[a["ticker_u"] == TICKER]
    if a.empty:
        raise ValueError(f"anchor_df has no rows for ticker {TICKER}")

    date_col: str | None = None
    if "as_of_date" in a.columns:
        date_col = "as_of_date"
    elif "date" in a.columns:
        date_col = "date"
    elif "valuation_date" in a.columns:
        date_col = "valuation_date"

    if date_col is None:
        return a.iloc[-1]

    return _pick_latest_valid_anchor_row(a, date_col=date_col, as_of_date=as_of_date)


def _coerce_bool_like(v: Any) -> bool | None:
    """Parse explicit anchor_validated cell; None if unset so caller can use placeholder rules."""
    if v is None:
        return None
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    if isinstance(v, (int, float)) and not pd.isna(v):
        if int(v) == 1:
            return True
        if int(v) == 0:
            return False
    if isinstance(v, str):
        s = v.strip().lower()
        if s == "":
            return None
        if s in ("true", "1", "yes", "y"):
            return True
        if s in ("false", "0", "no", "n"):
            return False
    if pd.isna(v):
        return None
    return None


def is_anchor_row_validated_for_production(row: pd.Series) -> bool:
    """
    True only if this anchor row may drive anchor_adjusted mode.
    - If anchor_validated is set to a boolean-like value, it wins.
    - Else if source or notes contains 'placeholder' (case-insensitive), not validated.
    - Else validated (backward compatible for rows without flags).
    """
    if "anchor_validated" in row.index:
        parsed = _coerce_bool_like(row.get("anchor_validated"))
        if parsed is not None:
            return parsed
    notes = str(row.get("notes", "") or "").lower()
    source = str(row.get("source", "") or "").lower()
    if "placeholder" in notes or "placeholder" in source:
        return False
    return True


def _anchor_failure_meta(exc: ValueError) -> tuple[str, str]:
    """
    Map anchor-related ValueError to (anchor_status, anchor_error_message).
    anchor_status is one of: missing, stale, invalid, unvalidated.
    """
    msg = str(exc).strip()
    if msg.startswith("Anchor is stale:") or "Anchor is stale:" in msg:
        return "stale", msg
    if "not validated for anchor_adjusted" in msg:
        return "unvalidated", msg
    if "No anchor_df row on or before" in msg:
        return "missing", msg
    if "anchor_df has no rows for ticker" in msg:
        return "missing", msg
    if "anchor_df." in msg and "cannot be parsed as dates" in msg:
        return "invalid", msg
    if "anchor_fair_value is invalid" in msg:
        return "invalid", msg
    return "missing", msg


def run_daily_valuation(
    as_of_date: str | pd.Timestamp,
    market_df: pd.DataFrame,
    fx_df: pd.DataFrame,
    input_cost_df: pd.DataFrame,
    anchor_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Run the V1 rule-based valuation for VNM for a single `as_of_date`.

    Returns a one-row DataFrame for ticker VNM.
    """
    as_of = _to_ts(as_of_date)

    require_columns(market_df, VNM_DAILY_MARKET_REQUIRED_COLUMNS, df_name="market_df")
    require_columns(fx_df, DAILY_FX_REQUIRED_COLUMNS, df_name="fx_df")
    require_columns(input_cost_df, DAILY_INPUT_COST_REQUIRED_COLUMNS, df_name="input_cost_df")
    require_columns(anchor_df, ANCHOR_REQUIRED_COLUMNS, df_name="anchor_df")

    m = market_df.copy()
    m["ticker_u"] = m["ticker"].astype(str).str.upper()
    m = m[m["ticker_u"] == TICKER]
    if m.empty:
        raise ValueError(f"market_df has no rows for ticker {TICKER}")
    m_row = _pick_row_for_date(m, date_col="date", target_date=as_of, df_name="market_df")

    close = float(pd.to_numeric(m_row.get("close"), errors="coerce"))
    if pd.isna(close) or close <= 0:
        raise ValueError(f"market_df close is invalid for {as_of.date()}: {m_row.get('close')!r}")
    currency = str(m_row.get("currency") or "VND")

    anchor_used = True
    anchor_status = "used"
    anchor_error_message = ""
    valuation_mode = "anchor_adjusted"
    a_row: pd.Series
    try:
        a_row = _pick_anchor_row(anchor_df, as_of_date=as_of)
        anchor_fair_value = float(pd.to_numeric(a_row.get("anchor_fair_value"), errors="coerce"))
        if pd.isna(anchor_fair_value) or anchor_fair_value <= 0:
            raise ValueError(f"anchor_df anchor_fair_value is invalid: {a_row.get('anchor_fair_value')!r}")
        anchor_currency = str(a_row.get("anchor_currency") or currency)
        if not is_anchor_row_validated_for_production(a_row):
            raise ValueError(
                "Anchor row is not validated for anchor_adjusted mode "
                "(set anchor_validated=true or remove 'placeholder' from source/notes)."
            )
    except ValueError as e:
        anchor_used = False
        anchor_status, anchor_error_message = _anchor_failure_meta(e)
        valuation_mode = "market_fallback"
        a_row = pd.Series(dtype=object)
        anchor_fair_value = float("nan")
        anchor_currency = currency

    rate_signal, fx_signal = _compute_fx_signals(as_of, fx_df)
    input_cost_signal = _compute_input_cost_signal(as_of, input_cost_df)
    if anchor_used:
        relative_valuation_signal = _compute_relative_valuation_signal(close=close, anchor_fair_value=anchor_fair_value)
        sentiment_signal = _compute_sentiment_signal(a_row)
    else:
        relative_valuation_signal = 0.0
        sentiment_signal = 0.0

    signals = Signals(
        rate_signal=rate_signal,
        fx_signal=fx_signal,
        input_cost_signal=input_cost_signal,
        relative_valuation_signal=relative_valuation_signal,
        sentiment_signal=sentiment_signal,
    )
    adjustment_pct = _combine_adjustment(signals)

    if anchor_used:
        final_fair_value = anchor_fair_value * (1.0 + adjustment_pct)
    else:
        final_fair_value = close * (1.0 + adjustment_pct)
    upside_downside_pct = final_fair_value / close - 1.0

    out = pd.DataFrame(
        [
            {
                "as_of_date": as_of.date().isoformat(),
                "ticker": TICKER,
                "close": close,
                "currency": currency,
                "anchor_fair_value": anchor_fair_value,
                "anchor_currency": anchor_currency,
                "anchor_used": anchor_used,
                "anchor_status": anchor_status,
                "anchor_error_message": anchor_error_message,
                "valuation_mode": valuation_mode,
                "adjustment_pct": adjustment_pct,
                "final_fair_value": final_fair_value,
                "upside_downside_pct": upside_downside_pct,
                "rate_signal": signals.rate_signal,
                "fx_signal": signals.fx_signal,
                "input_cost_signal": signals.input_cost_signal,
                "relative_valuation_signal": signals.relative_valuation_signal,
                "sentiment_signal": signals.sentiment_signal,
            }
        ]
    )
    return out

