"""
Export one daily VNM valuation row to a local Excel workbook (parallel to Google Sheets).

From project root (after `pip install -e .`):

  python scripts/export_vnm_daily_to_excel.py --as-of-date 2026-04-16
  python scripts/export_vnm_daily_to_excel.py --dry-run --as-of-date 2026-04-16
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

from vnm_valuation.config import load_config
from vnm_valuation.excel_daily_export import (
    compute_excel_dry_run_plan,
    repo_default_excel_path,
    upsert_valuation_row_excel,
)
from vnm_valuation.google_sheets_publish import build_publish_row_dict
from vnm_valuation.io_local import read_parquet
from vnm_valuation.mini_backtest import latest_anchor_valuation_date_on_or_before
from vnm_valuation.valuation import run_daily_valuation


def _resolve_input_paths(processed_dir: Path) -> dict[str, Path]:
    processed_dir = Path(processed_dir)
    paths = {
        "market": processed_dir / "vnm_daily_market.parquet",
        "fx": processed_dir / "daily_fx.parquet",
        "input_cost": processed_dir / "daily_input_cost.parquet",
        "anchor": processed_dir / "vnm_anchor_valuation.parquet",
    }
    missing = [str(p) for p in paths.values() if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing required input file(s) in processed_dir:\n- " + "\n- ".join(missing)
        )
    return paths


def _load_inputs(input_paths: dict[str, Path]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    market_df = read_parquet(input_paths["market"])
    fx_df = read_parquet(input_paths["fx"])
    input_cost_df = read_parquet(input_paths["input_cost"])
    anchor_df = read_parquet(input_paths["anchor"])
    return market_df, fx_df, input_cost_df, anchor_df


def _latest_market_date(market_df: pd.DataFrame) -> str:
    if "date" not in market_df.columns:
        raise ValueError("market_df must contain a 'date' column to infer latest date")
    dates = pd.to_datetime(market_df["date"], errors="coerce").dropna()
    if dates.empty:
        raise ValueError("market_df.date could not be parsed (no valid dates)")
    return dates.max().normalize().date().isoformat()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run local daily valuation and upsert one row into an Excel workbook (idempotent by date+ticker)."
    )
    parser.add_argument("--as-of-date", dest="as_of_date", default=None, help="YYYY-MM-DD (default: latest market date)")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output .xlsx path (default: output/vnm_daily_valuation.xlsx under repo root)",
    )
    parser.add_argument(
        "--worksheet",
        default="daily_valuation",
        help="Worksheet name (default: daily_valuation)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print row payload and planned action; do not write the workbook.",
    )
    args = parser.parse_args()

    cfg = load_config()
    input_paths = _resolve_input_paths(cfg.paths.processed_dir)
    market_df, fx_df, input_cost_df, anchor_df = _load_inputs(input_paths)

    as_of_date = args.as_of_date or _latest_market_date(market_df)
    result_df = run_daily_valuation(as_of_date, market_df, fx_df, input_cost_df, anchor_df)
    if len(result_df) != 1:
        raise SystemExit(f"Expected one-row valuation output, got {len(result_df)} rows")

    row = result_df.iloc[0]
    selected = latest_anchor_valuation_date_on_or_before(anchor_df, as_of_date)
    row_dict = build_publish_row_dict(row, selected_anchor_date=selected)

    root = _repo_root()
    out_path = Path(args.output) if args.output is not None else repo_default_excel_path(root)
    if not out_path.is_absolute():
        out_path = (root / out_path).resolve()

    if args.dry_run:
        plan = compute_excel_dry_run_plan(out_path, args.worksheet, row_dict)
        print("dry_run: row payload (JSON):")
        print(json.dumps(row_dict, indent=2, sort_keys=True))
        print(f"dry_run: output would be {out_path}")
        print(f"dry_run: worksheet={args.worksheet!r} planned_action={plan.action}")
        print("dry_run: no file written.")
        return 0

    token, _plan = upsert_valuation_row_excel(out_path, args.worksheet, row_dict)
    print(
        f"OK: excel={out_path.as_posix()} worksheet={args.worksheet!r} action={token} "
        f"as_of_date={row_dict['as_of_date']} ticker={row_dict['ticker']}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, FileNotFoundError, OSError) as e:
        print(f"error: {e}", file=sys.stderr)
        raise SystemExit(1) from e
