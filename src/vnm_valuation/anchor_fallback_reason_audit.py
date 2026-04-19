"""
Offline anchor fallback reason audit (CSV). Not used by production pipeline.

Classifies outcomes using `run_daily_valuation` outputs only — no parallel validation/stale rules.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from vnm_valuation.deterministic_inputs import (
    DEFAULT_BACKTEST_CLOSE,
    REVIEWED_ANCHOR_FAIR_VALUE,
    fx_and_cost_dataframes_wide,
    reviewed_snapshot_timeline_dataframe,
)
from vnm_valuation.mini_backtest import latest_anchor_valuation_date_on_or_before
from vnm_valuation.valuation import run_daily_valuation


def fallback_reason_from_anchor_status(anchor_status: str) -> str:
    """
    Normalized audit label derived from production `anchor_status`.

    Maps `invalid` and any unknown status to `other_error`.
    """
    s = str(anchor_status).strip().lower()
    if s == "used":
        return "used"
    if s == "stale":
        return "stale"
    if s == "unvalidated":
        return "unvalidated"
    if s == "missing":
        return "missing"
    return "other_error"


def _selected_anchor_row(anchor_df: pd.DataFrame, selected_iso: str | None) -> pd.Series | None:
    if not selected_iso or anchor_df.empty or "valuation_date" not in anchor_df.columns:
        return None
    vd = pd.to_datetime(anchor_df["valuation_date"], errors="coerce")
    m = anchor_df.loc[vd == pd.Timestamp(selected_iso).normalize()]
    if m.empty:
        return None
    return m.iloc[-1]


def fixture_unvalidated_single_row() -> pd.DataFrame:
    """One on-or-before row with explicit `anchor_validated=False` (audit-only; not production data)."""
    return pd.DataFrame(
        [
            {
                "ticker": "VNM",
                "anchor_fair_value": float(REVIEWED_ANCHOR_FAIR_VALUE),
                "anchor_currency": "VND",
                "valuation_date": "2026-03-31",
                "anchor_validated": False,
                "source": "audit_fixture_unvalidated",
                "notes": "Deterministic unvalidated scenario for fallback reason audit",
            }
        ]
    )


def fixture_future_anchor_only() -> pd.DataFrame:
    """Only a future-dated anchor vs a mid-2026 as_of → missing on or before (audit-only)."""
    return pd.DataFrame(
        [
            {
                "ticker": "VNM",
                "anchor_fair_value": 72000.0,
                "anchor_currency": "VND",
                "valuation_date": "2027-06-30",
                "anchor_validated": True,
                "source": "audit_fixture_future_only",
                "notes": "Forward scenario only; no row on or before 2026-04-16",
            }
        ]
    )


def default_fallback_reason_scenarios() -> list[tuple[str, str, pd.DataFrame]]:
    """(scenario_name, as_of_date, anchor_df) — explicit deterministic cases."""
    return [
        ("reviewed_used_2025q1", "2025-04-10", reviewed_snapshot_timeline_dataframe().copy()),
        ("reviewed_stale_2028", "2028-06-01", reviewed_snapshot_timeline_dataframe().copy()),
        ("unvalidated_anchor_only", "2026-04-16", fixture_unvalidated_single_row()),
        ("no_anchor_available", "2026-04-16", fixture_future_anchor_only()),
    ]


def run_anchor_fallback_reason_audit(
    scenarios: list[tuple[str, str, pd.DataFrame]] | None = None,
    *,
    close: float | None = None,
    scenario_group: str = "deterministic_audit",
) -> pd.DataFrame:
    """
    One row per scenario: selection helper + `run_daily_valuation` columns + `fallback_reason`.
    """
    items = scenarios if scenarios is not None else default_fallback_reason_scenarios()
    c = float(close) if close is not None else float(DEFAULT_BACKTEST_CLOSE)
    fx_df, input_cost_df = fx_and_cost_dataframes_wide()

    rows: list[dict[str, object]] = []
    for scenario_name, as_of, anchor in items:
        anchor_df = anchor.copy()
        market_df = pd.DataFrame([{"date": as_of, "ticker": "VNM", "close": c, "currency": "VND"}])
        out = run_daily_valuation(as_of, market_df, fx_df, input_cost_df, anchor_df)
        r = out.iloc[0]

        selected = latest_anchor_valuation_date_on_or_before(anchor_df, as_of)
        sel_row = _selected_anchor_row(anchor_df, selected)

        validated: object = ""
        if sel_row is not None and "anchor_validated" in sel_row.index:
            v = sel_row.get("anchor_validated")
            validated = bool(v) if not (isinstance(v, float) and pd.isna(v)) else ""
        elif sel_row is not None:
            validated = True

        anchor_age_days: float | str = ""
        if selected:
            anchor_age_days = float(
                (pd.Timestamp(as_of).normalize() - pd.Timestamp(selected).normalize()).days
            )

        status = str(r["anchor_status"])
        err = r.get("anchor_error_message")
        rows.append(
            {
                "scenario_name": scenario_name,
                "scenario_group": scenario_group,
                "as_of_date": as_of,
                "selected_anchor_date": selected if selected is not None else "",
                "anchor_validated": validated,
                "anchor_age_days": anchor_age_days,
                "close": float(r["close"]),
                "final_fair_value": float(r["final_fair_value"]),
                "anchor_used": bool(r["anchor_used"]),
                "anchor_status": status,
                "valuation_mode": str(r["valuation_mode"]),
                "fallback_reason": fallback_reason_from_anchor_status(status),
                "anchor_error_message": "" if pd.isna(err) or err is None else str(err),
            }
        )

    return pd.DataFrame(rows)


def write_fallback_reason_audit_csv(path: Path | str, df: pd.DataFrame) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)
    return p.resolve()
