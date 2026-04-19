"""
Offline anchor timeline coverage / freshness audit (CSV). Not used by production pipeline.

Uses the same stale-age rule as valuation (`STALE_ANCHOR_MAX_AGE_DAYS`); does not change it.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from vnm_valuation.deterministic_inputs import (
    DEFAULT_BACKTEST_CLOSE,
    fx_and_cost_dataframes_wide,
    reviewed_snapshot_timeline_dataframe,
)
from vnm_valuation.mini_backtest import latest_anchor_valuation_date_on_or_before
from vnm_valuation.valuation import STALE_ANCHOR_MAX_AGE_DAYS, run_daily_valuation

DEFAULT_FRESHNESS_AUDIT_DATES: list[str] = [
    "2024-04-15",
    "2024-10-15",
    "2025-04-10",
    "2025-10-15",
    "2026-04-16",
    "2027-04-15",
    "2028-06-01",
]


def _selected_anchor_row(anchor_df: pd.DataFrame, selected_iso: str | None) -> pd.Series | None:
    if not selected_iso:
        return None
    vd = pd.to_datetime(anchor_df["valuation_date"], errors="coerce")
    m = anchor_df.loc[vd == pd.Timestamp(selected_iso).normalize()]
    if m.empty:
        return None
    return m.iloc[-1]


def _next_reviewed_anchor_date(anchor_df: pd.DataFrame, selected_iso: str | None) -> str | None:
    """Smallest valuation_date strictly after `selected_iso`, if any."""
    if not selected_iso:
        return None
    vd = pd.to_datetime(anchor_df["valuation_date"], errors="coerce").dropna()
    sel = pd.Timestamp(selected_iso).normalize()
    after = vd[vd > sel]
    if after.empty:
        return None
    nxt = after.min()
    return nxt.date().isoformat()


def _coverage_bucket(
    *,
    anchor_exists: bool,
    anchor_age_days: float | None,
    is_stale_by_status: bool,
    anchor_used: bool,
    anchor_status: str,
) -> str:
    if not anchor_exists:
        return "no_anchor"
    if is_stale_by_status:
        return "stale"
    if anchor_used:
        if anchor_age_days is None or anchor_age_days <= 180:
            return "fresh"
        return "aging"
    return f"other_{anchor_status}"


def run_anchor_freshness_audit(
    as_of_dates: list[str] | None = None,
    *,
    close: float | None = None,
    anchor_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    For each as-of date: selection, age vs `STALE_ANCHOR_MAX_AGE_DAYS`, and valuation outcome columns.
    """
    dates = list(as_of_dates) if as_of_dates is not None else list(DEFAULT_FRESHNESS_AUDIT_DATES)
    c = float(close) if close is not None else float(DEFAULT_BACKTEST_CLOSE)
    anchor = reviewed_snapshot_timeline_dataframe() if anchor_df is None else anchor_df.copy()
    fx_df, input_cost_df = fx_and_cost_dataframes_wide()
    cutoff = int(STALE_ANCHOR_MAX_AGE_DAYS)

    rows: list[dict[str, object]] = []
    for d in dates:
        market_df = pd.DataFrame([{"date": d, "ticker": "VNM", "close": c, "currency": "VND"}])
        out = run_daily_valuation(d, market_df, fx_df, input_cost_df, anchor)
        r = out.iloc[0]

        selected = latest_anchor_valuation_date_on_or_before(anchor, d)
        anchor_exists = selected is not None
        sel_row = _selected_anchor_row(anchor, selected)

        validated: bool | float | None = None
        if sel_row is not None and "anchor_validated" in sel_row.index:
            v = sel_row.get("anchor_validated")
            validated = bool(v) if not (isinstance(v, float) and pd.isna(v)) else None
        elif sel_row is not None:
            validated = True

        anchor_age_days: float | None = None
        if selected:
            anchor_age_days = float((pd.Timestamp(d).normalize() - pd.Timestamp(selected).normalize()).days)

        # Match production: stale is whatever `run_daily_valuation` reports (same rule as valuation.py).
        is_stale = str(r["anchor_status"]) == "stale"

        next_d = _next_reviewed_anchor_date(anchor, selected)
        days_until_next: float | None = None
        if next_d:
            days_until_next = float((pd.Timestamp(next_d).normalize() - pd.Timestamp(d).normalize()).days)

        err = r.get("anchor_error_message")
        rows.append(
            {
                "as_of_date": d,
                "selected_anchor_date": selected if selected is not None else "",
                "anchor_exists": anchor_exists,
                "anchor_validated": validated if validated is not None else "",
                "anchor_age_days": anchor_age_days if anchor_age_days is not None else "",
                "stale_cutoff_days": cutoff,
                "is_stale": is_stale,
                "anchor_status": str(r["anchor_status"]),
                "valuation_mode": str(r["valuation_mode"]),
                "anchor_used": bool(r["anchor_used"]),
                "anchor_error_message": "" if pd.isna(err) or err is None else str(err),
                "next_reviewed_anchor_date": next_d if next_d else "",
                "days_until_next_reviewed_anchor": days_until_next if days_until_next is not None else "",
                "coverage_bucket": _coverage_bucket(
                    anchor_exists=anchor_exists,
                    anchor_age_days=anchor_age_days,
                    is_stale_by_status=is_stale,
                    anchor_used=bool(r["anchor_used"]),
                    anchor_status=str(r["anchor_status"]),
                ),
            }
        )

    return pd.DataFrame(rows)


def write_freshness_audit_csv(path: Path | str, df: pd.DataFrame) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)
    return p.resolve()
