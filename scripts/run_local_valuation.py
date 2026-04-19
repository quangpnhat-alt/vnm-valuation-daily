from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from vnm_valuation.config import load_config
from vnm_valuation.io_local import read_parquet, write_csv, write_parquet
from vnm_valuation.valuation import run_daily_valuation


def resolve_input_paths(processed_dir: Path) -> dict[str, Path]:
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


def load_inputs(input_paths: dict[str, Path]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    market_df = read_parquet(input_paths["market"])
    fx_df = read_parquet(input_paths["fx"])
    input_cost_df = read_parquet(input_paths["input_cost"])
    anchor_df = read_parquet(input_paths["anchor"])
    return market_df, fx_df, input_cost_df, anchor_df


def save_outputs(result_df: pd.DataFrame, output_dir: Path) -> tuple[Path, Path]:
    output_dir = Path(output_dir)
    out_parquet = output_dir / "vnm_daily_valuation.parquet"
    out_csv = output_dir / "vnm_daily_valuation.csv"
    write_parquet(result_df, out_parquet)
    write_csv(result_df, out_csv)
    return out_parquet, out_csv


def _latest_market_date(market_df: pd.DataFrame) -> str:
    if "date" not in market_df.columns:
        raise ValueError("market_df must contain a 'date' column to infer latest date")
    dates = pd.to_datetime(market_df["date"], errors="coerce").dropna()
    if dates.empty:
        raise ValueError("market_df.date could not be parsed (no valid dates)")
    return dates.max().normalize().date().isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local VNM daily valuation (MVP).")
    parser.add_argument("--as-of-date", dest="as_of_date", default=None, help="YYYY-MM-DD (default: latest market date)")
    args = parser.parse_args()

    cfg = load_config()
    input_paths = resolve_input_paths(cfg.paths.processed_dir)
    market_df, fx_df, input_cost_df, anchor_df = load_inputs(input_paths)

    as_of_date = args.as_of_date or _latest_market_date(market_df)
    result_df = run_daily_valuation(as_of_date, market_df, fx_df, input_cost_df, anchor_df)

    if len(result_df) != 1:
        raise ValueError(f"Expected one-row valuation output, got {len(result_df)} rows")

    out_parquet, out_csv = save_outputs(result_df, cfg.paths.output_dir)

    ticker = str(result_df.loc[result_df.index[0], "ticker"])
    date = str(result_df.loc[result_df.index[0], "as_of_date"])
    fair = float(result_df.loc[result_df.index[0], "final_fair_value"])
    print(
        f"OK: {ticker} {date} final_fair_value={fair:.4f} | "
        f"parquet={out_parquet.as_posix()} csv={out_csv.as_posix()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

