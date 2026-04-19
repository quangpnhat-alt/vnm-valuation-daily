from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

from vnm_valuation.schemas import DAILY_FX_REQUIRED_COLUMNS, require_columns


DEFAULT_BASE = "USD"
DEFAULT_QUOTE = "VND"


def _snake_case(name: str) -> str:
    s = str(name).strip()
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


def _auto_detect_raw_file(raw_dir: Path) -> Path:
    raw_dir = Path(raw_dir)
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir.resolve()}")

    preferred = _find_first_existing(
        raw_dir,
        candidates=[
            "daily_fx.csv",
            "daily_fx.xlsx",
            "fx.csv",
            "fx.xlsx",
            "usd_vnd.csv",
            "usd_vnd.xlsx",
        ],
    )
    if preferred is not None:
        return preferred

    files = sorted(list(raw_dir.glob("*.csv")) + list(raw_dir.glob("*.xlsx")) + list(raw_dir.glob("*.xls")))
    if not files:
        raise FileNotFoundError(
            "No suitable raw FX input found in data/raw/. "
            "Expected e.g. daily_fx.csv or usd_vnd.xlsx"
        )
    return files[0]


def _read_raw(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported raw FX file type: {path.name}")


def _choose_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {str(c).lower(): str(c) for c in df.columns}
    for c in candidates:
        if c.lower() in cols:
            return cols[c.lower()]
    return None


def _parse_pair(v: object) -> tuple[str, str] | None:
    if v is None:
        return None
    s = str(v).strip().upper()
    if not s:
        return None
    s = s.replace("/", "").replace("-", "").replace("_", "")
    # Common formats: USDVND, USDVND=X, USDVNDFIX (we just take first 6 if plausible)
    m = re.search(r"([A-Z]{3})([A-Z]{3})", s)
    if not m:
        return None
    return m.group(1), m.group(2)


def _normalize_fx_df(df: pd.DataFrame) -> pd.DataFrame:
    d = df.rename(columns={c: _snake_case(c) for c in df.columns}).copy()

    date_col = _choose_column(d, ["date", "trading_date", "trade_date", "day", "datetime", "time"])
    rate_col = _choose_column(d, ["rate", "fx_rate", "close", "value", "mid", "last"])
    base_col = _choose_column(d, ["base_ccy", "base_currency", "base", "ccy_base"])
    quote_col = _choose_column(d, ["quote_ccy", "quote_currency", "quote", "ccy_quote", "terms_ccy"])
    pair_col = _choose_column(d, ["pair", "currency_pair", "ccy_pair", "symbol", "ticker"])

    if date_col is None:
        raise ValueError("Raw FX file must include a date column (e.g. date/trading_date)")
    if rate_col is None:
        raise ValueError("Raw FX file must include a rate column (e.g. rate/fx_rate/close)")

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(d[date_col], errors="coerce").dt.normalize()
    out["rate"] = pd.to_numeric(d[rate_col], errors="coerce")

    # Determine base/quote
    if base_col is not None and quote_col is not None:
        out["base_ccy"] = d[base_col].astype(str).str.upper()
        out["quote_ccy"] = d[quote_col].astype(str).str.upper()
    elif pair_col is not None:
        parsed = d[pair_col].map(_parse_pair)
        out["base_ccy"] = parsed.map(lambda x: x[0] if x else None)
        out["quote_ccy"] = parsed.map(lambda x: x[1] if x else None)
    else:
        out["base_ccy"] = DEFAULT_BASE
        out["quote_ccy"] = DEFAULT_QUOTE

    # Optional passthrough
    if "source" in d.columns:
        out["source"] = d["source"].astype(str)

    out = out.dropna(subset=["date", "rate", "base_ccy", "quote_ccy"])
    out["base_ccy"] = out["base_ccy"].astype(str).str.upper()
    out["quote_ccy"] = out["quote_ccy"].astype(str).str.upper()

    # Prefer USD/VND if multiple pairs exist
    preferred = out[(out["base_ccy"] == DEFAULT_BASE) & (out["quote_ccy"] == DEFAULT_QUOTE)]
    if not preferred.empty:
        out = preferred

    out["date"] = out["date"].dt.date.astype(str)
    out = out.sort_values(["date", "base_ccy", "quote_ccy"], ascending=True)
    out = out.drop_duplicates(subset=["date", "base_ccy", "quote_ccy"], keep="last").reset_index(drop=True)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Build processed daily FX rates from raw CSV/Excel.")
    parser.add_argument(
        "--input",
        dest="input_path",
        default=None,
        help="Optional path to raw FX file (default: auto-detect in data/raw/).",
    )
    args = parser.parse_args()

    raw_dir = Path("data") / "raw"
    input_path = Path(args.input_path) if args.input_path else _auto_detect_raw_file(raw_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"Raw FX input not found: {input_path}")

    raw_df = _read_raw(input_path)
    fx_df = _normalize_fx_df(raw_df)
    require_columns(fx_df, DAILY_FX_REQUIRED_COLUMNS, df_name="daily_fx")

    out_path = Path("data") / "processed" / "daily_fx.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fx_df.to_parquet(out_path, index=False)

    print(f"OK: wrote {len(fx_df)} rows to {out_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

