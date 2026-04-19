from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

from vnm_valuation.schemas import VNM_DAILY_MARKET_REQUIRED_COLUMNS, require_columns


TICKER = "VNM"


def _snake_case(name: str) -> str:
    s = name.strip()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()


def _find_first_existing(raw_dir: Path, candidates: Iterable[str]) -> Path | None:
    for name in candidates:
        p = raw_dir / name
        if p.exists() and p.is_file():
            return p
    return None


def _auto_detect_raw_csv(raw_dir: Path) -> Path:
    raw_dir = Path(raw_dir)
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir.resolve()}")

    preferred = _find_first_existing(
        raw_dir,
        candidates=[
            "vnm_daily_market.csv",
            "vnm_market.csv",
            "vnm_prices.csv",
        ],
    )
    if preferred is not None:
        return preferred

    csvs = sorted(raw_dir.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(
            "No suitable raw CSV found in data/raw/. "
            "Expected e.g. vnm_daily_market.csv or vnm_prices.csv"
        )
    return csvs[0]


def _choose_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in cols:
            return cols[c.lower()]
    return None


def _normalize_market_df(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize column names to snake_case.
    rename = {c: _snake_case(str(c)) for c in df.columns}
    d = df.rename(columns=rename).copy()

    # Map common variants -> canonical names.
    date_col = _choose_column(d, ["date", "trading_date", "trade_date", "day", "datetime", "time"])
    close_col = _choose_column(d, ["close", "close_price", "last", "price", "px_last", "adj_close"])
    ticker_col = _choose_column(d, ["ticker", "symbol", "code"])

    if date_col is None:
        raise ValueError("Raw market CSV must include a date column (e.g. date/trading_date)")
    if close_col is None:
        raise ValueError("Raw market CSV must include a close column (e.g. close/last/price)")

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(d[date_col], errors="coerce").dt.normalize()
    out["ticker"] = d[ticker_col].astype(str).str.upper() if ticker_col is not None else TICKER
    out["close"] = pd.to_numeric(d[close_col], errors="coerce")

    # Optional keepers if present (already normalized to snake_case).
    optional_map = {
        "traded_value": ["traded_value", "turnover", "value", "trading_value"],
        "trailing_pe": ["trailing_pe", "pe", "p_e", "pe_ttm"],
        "trailing_ev_ebitda": ["trailing_ev_ebitda", "ev_ebitda", "ev_to_ebitda", "ev_ebitda_ttm"],
        "sentiment_score": ["sentiment_score", "sentiment", "news_sentiment_score"],
    }
    present_cols = set(d.columns.astype(str))
    for canonical, cands in optional_map.items():
        found = None
        for c in cands:
            if c in present_cols:
                found = c
                break
        if found is not None:
            out[canonical] = pd.to_numeric(d[found], errors="coerce")

    # Filter to VNM only (builder is VNM-specific).
    out["ticker"] = out["ticker"].astype(str).str.upper()
    out = out[out["ticker"] == TICKER]

    # Basic cleanup
    out = out.dropna(subset=["date", "ticker", "close"])
    out["date"] = out["date"].dt.date.astype(str)

    out = out.sort_values(["date", "ticker"], ascending=True)
    out = out.drop_duplicates(subset=["date", "ticker"], keep="last").reset_index(drop=True)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Build processed VNM daily market data from raw CSV.")
    parser.add_argument(
        "--input",
        dest="input_csv",
        default=None,
        help="Optional path to raw CSV (default: auto-detect in data/raw/).",
    )
    args = parser.parse_args()

    raw_dir = Path("data") / "raw"
    input_csv = Path(args.input_csv) if args.input_csv else _auto_detect_raw_csv(raw_dir)
    if not input_csv.exists():
        raise FileNotFoundError(f"Raw input CSV not found: {input_csv}")

    df_raw = pd.read_csv(input_csv)
    market_df = _normalize_market_df(df_raw)
    require_columns(market_df, VNM_DAILY_MARKET_REQUIRED_COLUMNS, df_name="vnm_daily_market")

    out_path = Path("data") / "processed" / "vnm_daily_market.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    market_df.to_parquet(out_path, index=False)

    print(f"OK: wrote {len(market_df)} rows to {out_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

