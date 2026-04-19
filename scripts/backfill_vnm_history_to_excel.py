"""
Backfill historical daily VNM valuation rows into the same Excel workbook as the daily exporter.

From project root (after `pip install -e .`):

  python scripts/backfill_vnm_history_to_excel.py --start-date 2015-01-01 --limit 5 --dry-run
  python scripts/backfill_vnm_history_to_excel.py --start-date 2026-04-14 --end-date 2026-04-16
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from vnm_valuation.config import load_config
from vnm_valuation.excel_daily_export import repo_default_excel_path
from vnm_valuation.excel_history_backfill import DEFAULT_HISTORY_START, backfill_vnm_history_to_excel
from vnm_valuation.io_local import read_parquet


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


def _print_summary(stats: object, *, excel_path: Path, dry_run: bool) -> None:
    print()
    print("=== Backfill summary ===")
    print(f"output:               {excel_path.as_posix()}")
    print(f"dry_run:              {dry_run}")
    print(f"attempted_dates:      {stats.attempted_dates}")
    print(f"valuation_ok:         {stats.valuation_ok}")
    print(f"valuation_skipped:    {stats.valuation_skipped}")
    if dry_run:
        print(f"would_bootstrap:      {stats.dry_run_would_bootstrap}")
        print(f"would_append:         {stats.dry_run_would_append}")
        print(f"would_update:         {stats.dry_run_would_update}")
    else:
        print(f"excel_bootstrap:      {stats.excel_bootstrap}")
        print(f"excel_append:         {stats.excel_append}")
        print(f"excel_update:         {stats.excel_update}")
    if stats.skipped_dates:
        show = stats.skipped_dates[:12]
        for s in show:
            print(f"  skipped: {s}")
        if len(stats.skipped_dates) > 12:
            print(f"  ... and {len(stats.skipped_dates) - 12} more skipped")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill daily VNM valuations into the shared Excel workbook (idempotent by as_of_date+ticker)."
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_HISTORY_START,
        help=f"First calendar date (default: {DEFAULT_HISTORY_START})",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Last calendar date inclusive (default: latest market date in processed data)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Excel .xlsx path (default: output/vnm_daily_valuation.xlsx under repo root)",
    )
    parser.add_argument("--worksheet", default="daily_valuation", help="Worksheet name")
    parser.add_argument("--dry-run", action="store_true", help="Do not write the workbook; print summary only.")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N dates from the range (testing).")
    args = parser.parse_args()

    cfg = load_config()
    input_paths = _resolve_input_paths(cfg.paths.processed_dir)
    market_df, fx_df, input_cost_df, anchor_df = _load_inputs(input_paths)

    end_date = args.end_date or _latest_market_date(market_df)
    root = _repo_root()
    excel_path = Path(args.output) if args.output is not None else repo_default_excel_path(root)
    if not excel_path.is_absolute():
        excel_path = (root / excel_path).resolve()

    dr = pd.date_range(start=args.start_date, end=end_date, freq="D")
    dates = [d.date().isoformat() for d in dr]
    if args.limit is not None:
        dates = dates[: int(args.limit)]

    print(
        f"Resolved range: {args.start_date} .. {end_date} ({len(dates)} calendar day(s))"
        + (f", limit={args.limit}" if args.limit is not None else "")
    )

    stats = backfill_vnm_history_to_excel(
        start_date=args.start_date,
        end_date=end_date,
        market_df=market_df,
        fx_df=fx_df,
        input_cost_df=input_cost_df,
        anchor_df=anchor_df,
        excel_path=excel_path,
        worksheet=args.worksheet,
        dry_run=args.dry_run,
        limit=args.limit,
    )
    _print_summary(stats, excel_path=excel_path, dry_run=args.dry_run)
    if args.dry_run:
        print("OK: dry-run complete (workbook not modified).")
    else:
        print("OK: backfill complete.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, FileNotFoundError, OSError) as e:
        print(f"error: {e}", file=sys.stderr)
        raise SystemExit(1) from e
