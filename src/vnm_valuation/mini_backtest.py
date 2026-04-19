"""
Offline multi-date sweep over deterministic inputs (audit CSV). Not used by production pipeline.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from vnm_valuation.deterministic_inputs import (
    DEFAULT_BACKTEST_CLOSE,
    REVIEWED_ANCHOR_FAIR_VALUE,
    REVIEWED_FAIR_2025_03_31,
    REVIEWED_FAIR_2025_09_30,
    fx_and_cost_dataframes_wide,
    reviewed_snapshot_timeline_dataframe,
)
from vnm_valuation.valuation import run_daily_valuation

DEFAULT_SWEEP_DATES: list[str] = [
    "2025-04-01",
    "2025-07-15",
    "2025-10-01",
    "2026-01-15",
    "2026-04-16",
    "2028-06-01",
]

DEFAULT_TIMELINE_SWEEP_DATES: list[str] = [
    "2025-04-10",
    "2025-07-15",
    "2025-10-15",
    "2026-01-15",
    "2026-04-16",
    "2028-06-01",
]


def latest_anchor_valuation_date_on_or_before(anchor_df: pd.DataFrame, as_of: str) -> str | None:
    """
    Latest `valuation_date` on/before `as_of` (audit helper; does not apply stale/validation rules).
    """
    if anchor_df.empty or "valuation_date" not in anchor_df.columns:
        return None
    vd = pd.to_datetime(anchor_df["valuation_date"], errors="coerce")
    as_ts = pd.Timestamp(as_of).normalize()
    sub = anchor_df.loc[vd.notna() & (vd <= as_ts)]
    if sub.empty:
        return None
    best = vd.loc[sub.index].max()
    row = sub.loc[vd.loc[sub.index] == best].iloc[-1]
    ts = pd.Timestamp(row["valuation_date"])
    return ts.date().isoformat()


def run_mini_backtest(
    as_of_dates: list[str] | None = None,
    *,
    close: float | None = None,
    anchor_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Run `run_daily_valuation` for each as-of date; return one row per date with audit columns.
    """
    dates = list(as_of_dates) if as_of_dates is not None else list(DEFAULT_SWEEP_DATES)
    c = float(close) if close is not None else float(DEFAULT_BACKTEST_CLOSE)
    anchor = reviewed_snapshot_timeline_dataframe() if anchor_df is None else anchor_df.copy()
    fx_df, input_cost_df = fx_and_cost_dataframes_wide()

    rows: list[dict[str, object]] = []
    for d in dates:
        market_df = pd.DataFrame([{"date": d, "ticker": "VNM", "close": c, "currency": "VND"}])
        out = run_daily_valuation(d, market_df, fx_df, input_cost_df, anchor)
        r = out.iloc[0]
        cand = latest_anchor_valuation_date_on_or_before(anchor, d)

        err = r.get("anchor_error_message")
        rows.append(
            {
                "as_of_date": d,
                "selected_anchor_date": cand if cand is not None else "",
                "anchor_used": bool(r["anchor_used"]),
                "anchor_status": str(r["anchor_status"]),
                "valuation_mode": str(r["valuation_mode"]),
                "close": float(r["close"]),
                "anchor_fair_value": r["anchor_fair_value"],
                "adjustment_pct": float(r["adjustment_pct"]),
                "relative_valuation_signal": float(r["relative_valuation_signal"]),
                "sentiment_signal": float(r["sentiment_signal"]),
                "final_fair_value": float(r["final_fair_value"]),
                "anchor_error_message": "" if pd.isna(err) or err is None else str(err),
            }
        )

    return pd.DataFrame(rows)


def run_timeline_backtest(
    as_of_dates: list[str] | None = None,
    *,
    close: float | None = None,
    anchor_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Same as `run_mini_backtest` but defaults to `DEFAULT_TIMELINE_SWEEP_DATES` (audit CSV export).
    """
    dates = list(as_of_dates) if as_of_dates is not None else list(DEFAULT_TIMELINE_SWEEP_DATES)
    return run_mini_backtest(as_of_dates=dates, close=close, anchor_df=anchor_df)


def write_mini_backtest_csv(path: Path | str, df: pd.DataFrame) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)
    return p.resolve()


def expected_anchor_fair_value_for_selected_date(selected_iso: str) -> float | None:
    """Maps selected snapshot date to blended fair value for test assertions."""
    m = {
        "2025-03-31": REVIEWED_FAIR_2025_03_31,
        "2025-09-30": REVIEWED_FAIR_2025_09_30,
        "2026-03-31": REVIEWED_ANCHOR_FAIR_VALUE,
    }
    return m.get(selected_iso)
