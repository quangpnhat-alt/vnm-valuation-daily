"""
Historical sweep: run valuation per calendar day and upsert rows into the shared Excel workbook.

Export-only; does not change valuation rules. Reuses `upsert_valuation_row_excel` for idempotency.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd

from vnm_valuation.excel_daily_export import (
    load_worksheet_str_grid,
    read_excel_workbook,
    upsert_valuation_row_excel,
    write_all_sheets,
)
from vnm_valuation.google_sheets_publish import PublishPlan, build_publish_row_dict, compute_publish_plan
from vnm_valuation.mini_backtest import latest_anchor_valuation_date_on_or_before
from vnm_valuation.valuation import run_daily_valuation

DEFAULT_HISTORY_START = "2015-01-01"


@dataclass
class BackfillStats:
    attempted_dates: int = 0
    valuation_ok: int = 0
    valuation_skipped: int = 0
    excel_bootstrap: int = 0
    excel_append: int = 0
    excel_update: int = 0
    dry_run_would_bootstrap: int = 0
    dry_run_would_append: int = 0
    dry_run_would_update: int = 0
    skipped_dates: list[str] = field(default_factory=list)


def _simulate_grid_after_plan(grid: list[list[str]] | None, plan: PublishPlan) -> list[list[str]]:
    """Apply a publish plan to an in-memory string grid (for dry-run accounting only)."""
    if plan.action == "bootstrap":
        return [list(plan.header), list(plan.row_values)]
    assert grid is not None and len(grid) >= 1
    if plan.action == "update":
        assert plan.target_row_1based is not None
        idx = plan.target_row_1based - 1
        g = [list(r) for r in grid]
        g[idx] = list(plan.row_values)
        return g
    return [list(r) for r in grid] + [list(plan.row_values)]


def sort_daily_valuation_sheet(path: Path, sheet_name: str) -> None:
    """Sort the target sheet by `as_of_date` ascending; preserve other sheets."""
    path = Path(path)
    if not path.is_file():
        return
    book = read_excel_workbook(path)
    if sheet_name not in book:
        return
    df = book[sheet_name]
    if df.empty or "as_of_date" not in df.columns:
        return
    df = df.copy()
    df["_sort"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df = df.sort_values("_sort", na_position="last").drop(columns="_sort")
    book[sheet_name] = df
    write_all_sheets(path, book)


def backfill_vnm_history_to_excel(
    *,
    start_date: str,
    end_date: str,
    market_df: pd.DataFrame,
    fx_df: pd.DataFrame,
    input_cost_df: pd.DataFrame,
    anchor_df: pd.DataFrame,
    excel_path: Path,
    worksheet: str,
    dry_run: bool,
    limit: int | None,
    run_valuation: Callable[..., pd.DataFrame] = run_daily_valuation,
) -> BackfillStats:
    """
    For each calendar day in [start_date, end_date], run valuation and upsert one row (unless dry_run).

    Days where `run_valuation` raises (e.g. no market row) are counted as skipped, not fatal.
    """
    stats = BackfillStats()
    dr = pd.date_range(start=start_date, end=end_date, freq="D")
    dates = [d.date().isoformat() for d in dr]
    if limit is not None:
        dates = dates[: int(limit)]

    sim_grid: list[list[str]] | None = None
    if dry_run and excel_path.is_file():
        sim_grid = load_worksheet_str_grid(excel_path, worksheet)

    for as_of in dates:
        stats.attempted_dates += 1
        try:
            result_df = run_valuation(as_of, market_df, fx_df, input_cost_df, anchor_df)
        except (ValueError, KeyError) as e:
            stats.valuation_skipped += 1
            if len(stats.skipped_dates) < 300:
                msg = str(e)
                if len(msg) > 100:
                    msg = msg[:97] + "..."
                stats.skipped_dates.append(f"{as_of} ({msg})")
            continue
        except Exception:
            raise

        if len(result_df) != 1:
            stats.valuation_skipped += 1
            if len(stats.skipped_dates) < 300:
                stats.skipped_dates.append(f"{as_of} (expected 1 row, got {len(result_df)})")
            continue

        stats.valuation_ok += 1
        row = result_df.iloc[0]
        selected = latest_anchor_valuation_date_on_or_before(anchor_df, as_of)
        row_dict = build_publish_row_dict(row, selected_anchor_date=selected)

        if dry_run:
            plan = compute_publish_plan(sim_grid, row_dict)
            if plan.action == "bootstrap":
                stats.dry_run_would_bootstrap += 1
            elif plan.action == "update":
                stats.dry_run_would_update += 1
            else:
                stats.dry_run_would_append += 1
            sim_grid = _simulate_grid_after_plan(sim_grid, plan)
            continue

        token, _plan = upsert_valuation_row_excel(excel_path, worksheet, row_dict)
        if token == "bootstrap":
            stats.excel_bootstrap += 1
        elif token == "update":
            stats.excel_update += 1
        else:
            stats.excel_append += 1

    if not dry_run and stats.valuation_ok > 0:
        sort_daily_valuation_sheet(excel_path, worksheet)

    return stats
