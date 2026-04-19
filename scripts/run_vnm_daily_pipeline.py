"""
Run daily VNM valuation once, then export to Excel (default) and optionally Google Sheets.

From project root (after `pip install -e .`):

  python scripts/run_vnm_daily_pipeline.py --as-of-date 2026-04-16
  python scripts/run_vnm_daily_pipeline.py --dry-run --as-of-date 2026-04-16
  python scripts/run_vnm_daily_pipeline.py --to-gsheet --dry-run --as-of-date 2026-04-16
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from vnm_valuation.config import load_config
from vnm_valuation.daily_pipeline import DailyExportOptions, run_daily_exports
from vnm_valuation.excel_daily_export import repo_default_excel_path
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


def _print_summary(row_dict: dict[str, str]) -> None:
    print("=== Valuation summary ===")
    print(f"as_of_date:            {row_dict.get('as_of_date', '')}")
    print(f"ticker:                {row_dict.get('ticker', '')}")
    print(f"valuation_mode:        {row_dict.get('valuation_mode', '')}")
    print(f"anchor_status:         {row_dict.get('anchor_status', '')}")
    print(f"selected_anchor_date:  {row_dict.get('selected_anchor_date', '')}")
    print(f"final_fair_value:      {row_dict.get('final_fair_value', '')}")
    print()


def _print_export_result(result: object) -> None:
    print("=== Export targets ===")
    print(f"Excel:          {result.excel_status}")
    print(f"Google Sheets:  {result.gsheet_status}")
    if result.gsheet_error and result.gsheet_status not in ("skipped",):
        print(f"  (detail: {result.gsheet_error})")
    for w in result.warnings:
        print(f"Warning: {w}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run daily valuation once, then export to Excel (default) and optionally Google Sheets."
    )
    parser.add_argument("--as-of-date", dest="as_of_date", default=None, help="YYYY-MM-DD (default: latest market date)")
    ex = parser.add_mutually_exclusive_group()
    ex.add_argument("--excel", action="store_true", help="Export to Excel (default when neither flag is given)")
    ex.add_argument("--no-excel", action="store_true", help="Skip Excel export")
    parser.add_argument(
        "--to-gsheet",
        action="store_true",
        help="Also export to Google Sheets (requires GOOGLE_SERVICE_ACCOUNT_JSON and VNM_GSHEET_SPREADSHEET_ID).",
    )
    parser.add_argument(
        "--best-effort-gsheet",
        action="store_true",
        help="If Google Sheets fails or is misconfigured, continue after Excel (non-zero still if Excel fails).",
    )
    parser.add_argument(
        "--excel-output",
        type=Path,
        default=None,
        help="Excel .xlsx path (default: output/vnm_daily_valuation.xlsx under repo root)",
    )
    parser.add_argument("--excel-worksheet", default="daily_valuation", help="Excel worksheet name")
    parser.add_argument(
        "--gsheet-worksheet",
        default=None,
        help="Google Sheets worksheet/tab (default: env VNM_GSHEET_WORKSHEET or daily_valuation)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute valuation and show planned exports; do not write Excel or call Google APIs.",
    )
    args = parser.parse_args(argv)

    do_excel = False if args.no_excel else True

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

    _print_summary(row_dict)

    root = _repo_root()
    excel_path: Path | None = None
    if do_excel or (args.dry_run and not args.no_excel):
        excel_path = Path(args.excel_output) if args.excel_output is not None else repo_default_excel_path(root)
        if not excel_path.is_absolute():
            excel_path = (root / excel_path).resolve()

    opt = DailyExportOptions(
        do_excel=do_excel,
        excel_path=excel_path,
        excel_worksheet=args.excel_worksheet,
        to_gsheet=args.to_gsheet,
        gsheet_worksheet=args.gsheet_worksheet,
        dry_run=args.dry_run,
        best_effort_gsheet=args.best_effort_gsheet,
    )

    result = run_daily_exports(row_dict, opt)
    _print_export_result(result)

    if not args.dry_run:
        print("OK: pipeline complete.")
    else:
        print("OK: dry-run complete (no files written, no Google API calls).")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, FileNotFoundError, OSError, ImportError) as e:
        print(f"error: {e}", file=sys.stderr)
        raise SystemExit(1) from e
