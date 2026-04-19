from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

from vnm_valuation.schemas import DAILY_INPUT_COST_REQUIRED_COLUMNS, require_columns


DEFAULT_CURRENCY = "USD"


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
            "daily_input_cost.csv",
            "daily_input_cost.xlsx",
            "input_cost.csv",
            "input_cost.xlsx",
            "commodity_costs.csv",
            "commodity_costs.xlsx",
        ],
    )
    if preferred is not None:
        return preferred

    files = sorted(list(raw_dir.glob("*.csv")) + list(raw_dir.glob("*.xlsx")) + list(raw_dir.glob("*.xls")))
    if not files:
        raise FileNotFoundError(
            "No suitable raw input-cost file found in data/raw/. "
            "Expected e.g. daily_input_cost.csv or input_cost.xlsx"
        )
    return files[0]


def _read_raw(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported raw input-cost file type: {path.name}")


def _choose_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {str(c).lower(): str(c) for c in df.columns}
    for c in candidates:
        if c.lower() in cols:
            return cols[c.lower()]
    return None


def _normalize_item(value: object) -> str:
    s = _snake_case("" if value is None else str(value))
    # Map common synonyms to raw_milk so valuation.py can pick it up.
    synonyms = {
        "milk": "raw_milk",
        "rawmilk": "raw_milk",
        "raw_milk_price": "raw_milk",
        "milk_raw": "raw_milk",
        "whole_milk": "raw_milk",
        "skim_milk": "raw_milk",
    }
    return synonyms.get(s, s)


def _to_long_format(d: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize to the long format expected by the pipeline:
      date, item, cost, currency (+ optional unit, source)

    Supports:
    - long raw format (already has item+cost columns)
    - wide raw format (cost columns like raw_milk/sugar/soybean -> melt)
    """
    date_col = _choose_column(d, ["date", "trading_date", "trade_date", "day", "datetime", "time"])
    if date_col is None:
        raise ValueError("Raw input-cost data must include a date column (e.g. date/trading_date)")

    item_col = _choose_column(d, ["item", "input", "commodity", "name"])
    cost_col = _choose_column(d, ["cost", "price", "value", "close", "rate"])
    currency_col = _choose_column(d, ["currency", "ccy"])
    unit_col = _choose_column(d, ["unit", "uom"])
    source_col = _choose_column(d, ["source", "provider"])

    # Normalize date first
    d2 = d.copy()
    d2["_date"] = pd.to_datetime(d2[date_col], errors="coerce").dt.normalize()

    if item_col is not None and cost_col is not None:
        out = pd.DataFrame(
            {
                "date": d2["_date"],
                "item": d2[item_col].map(_normalize_item),
                "cost": pd.to_numeric(d2[cost_col], errors="coerce"),
                "currency": (d2[currency_col] if currency_col is not None else DEFAULT_CURRENCY),
            }
        )
        if unit_col is not None:
            out["unit"] = d2[unit_col].astype(str)
        if source_col is not None:
            out["source"] = d2[source_col].astype(str)
        return out

    # Wide format: melt likely cost columns.
    reserved = {c for c in [date_col, currency_col, unit_col, source_col] if c is not None}
    candidate_cost_cols = [c for c in d2.columns if c not in reserved and c != "_date"]
    if not candidate_cost_cols:
        raise ValueError(
            "Raw input-cost data does not look like long format (item+cost) or wide format (cost columns). "
            "Add columns like item/cost or commodity columns such as raw_milk, sugar, soybean."
        )

    melted = d2.melt(
        id_vars=["_date"] + ([currency_col] if currency_col is not None else []) + ([unit_col] if unit_col is not None else []) + ([source_col] if source_col is not None else []),
        value_vars=candidate_cost_cols,
        var_name="item",
        value_name="cost",
    )
    out = pd.DataFrame(
        {
            "date": melted["_date"],
            "item": melted["item"].map(_normalize_item),
            "cost": pd.to_numeric(melted["cost"], errors="coerce"),
            "currency": (melted[currency_col] if currency_col is not None else DEFAULT_CURRENCY),
        }
    )
    if unit_col is not None:
        out["unit"] = melted[unit_col].astype(str)
    if source_col is not None:
        out["source"] = melted[source_col].astype(str)
    return out


def _clean_and_validate(df_long: pd.DataFrame) -> pd.DataFrame:
    out = df_long.copy()
    out["currency"] = out["currency"].astype(str).str.upper()

    out = out.dropna(subset=["date", "item", "cost", "currency"])
    out = out[out["item"].astype(str).str.len() > 0]

    # Normalize date to ISO string for consistency with other builders.
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype(str)

    out = out.sort_values(["date", "item"], ascending=True)
    out = out.drop_duplicates(subset=["date", "item", "currency"], keep="last").reset_index(drop=True)

    require_columns(out, DAILY_INPUT_COST_REQUIRED_COLUMNS, df_name="daily_input_cost")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Build processed daily input cost data from raw CSV/Excel.")
    parser.add_argument(
        "--input",
        dest="input_path",
        default=None,
        help="Optional path to raw input-cost file (default: auto-detect in data/raw/).",
    )
    args = parser.parse_args()

    raw_dir = Path("data") / "raw"
    input_path = Path(args.input_path) if args.input_path else _auto_detect_raw_file(raw_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"Raw input-cost file not found: {input_path}")

    raw_df = _read_raw(input_path)
    raw_df = raw_df.rename(columns={c: _snake_case(c) for c in raw_df.columns})

    long_df = _to_long_format(raw_df)
    out_df = _clean_and_validate(long_df)

    out_path = Path("data") / "processed" / "daily_input_cost.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out_path, index=False)

    has_raw_milk = (out_df["item"].astype(str) == "raw_milk").any()
    print(f"OK: wrote {len(out_df)} rows to {out_path.resolve()} (raw_milk_present={has_raw_milk})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

