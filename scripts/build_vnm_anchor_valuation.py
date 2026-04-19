from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from vnm_valuation.valuation import is_anchor_row_validated_for_production

TICKER = "VNM"
ANCHOR_CURRENCY = "VND"


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
            "vnm_anchor_valuation.csv",
            "vnm_anchor_valuation.xlsx",
            "vnm_valuation.csv",
            "vnm_valuation.xlsx",
            "anchor_valuation.csv",
            "anchor_valuation.xlsx",
        ],
    )
    if preferred is not None:
        return preferred

    files = sorted(list(raw_dir.glob("*.csv")) + list(raw_dir.glob("*.xlsx")) + list(raw_dir.glob("*.xls")))
    if not files:
        raise FileNotFoundError(
            "No suitable raw valuation input found in data/raw/. "
            "Expected e.g. vnm_anchor_valuation.csv or vnm_anchor_valuation.xlsx"
        )
    return files[0]


def _read_raw(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported raw valuation file type: {path.name}")


def _col(df: pd.DataFrame, *candidates: str) -> str | None:
    cols = {str(c).lower(): str(c) for c in df.columns}
    for c in candidates:
        if c.lower() in cols:
            return cols[c.lower()]
    return None


def _to_float(v: Any) -> float | None:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return None
    return float(x)


def _row_has_usable_anchor_inputs(row: pd.Series) -> bool:
    for k in ("dcf_value", "ev_ebitda_value", "pe_value"):
        if _to_float(row.get(k)) is not None:
            return True
    return False


def _compute_anchor_value(row: pd.Series) -> tuple[float, dict[str, float]]:
    """
    Compute anchor fair value from available method values.
    Default weights:
      DCF 50%, EV/EBITDA 30%, PE 20%
    If only one usable value is available, use it directly.
    """
    dcf = _to_float(row.get("dcf_value"))
    ev = _to_float(row.get("ev_ebitda_value"))
    pe = _to_float(row.get("pe_value"))

    weights = {"dcf_value": 0.50, "ev_ebitda_value": 0.30, "pe_value": 0.20}
    available = {k: v for k, v in {"dcf_value": dcf, "ev_ebitda_value": ev, "pe_value": pe}.items() if v is not None}

    if not available:
        raise ValueError(
            "No usable anchor inputs found. Provide at least one of: dcf_value, ev_ebitda_value, pe_value "
            "(columns will be normalized to snake_case)."
        )

    if len(available) == 1:
        k, v = next(iter(available.items()))
        return float(v), {k: 1.0}

    w_sum = sum(weights[k] for k in available.keys())
    combined = sum(available[k] * (weights[k] / w_sum) for k in available.keys())
    used_weights = {k: float(weights[k] / w_sum) for k in available.keys()}
    return float(combined), used_weights


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    rename = {c: _snake_case(c) for c in df.columns}
    d = df.rename(columns=rename).copy()

    # Standardize method value column names if provided under common variants.
    mapping = {
        "valuation_date": ["valuation_date", "date", "as_of_date", "asof", "as_of"],
        "dcf_value": ["dcf_value", "dcf", "dcf_fair_value", "dcf_equity_value", "dcf_price"],
        "ev_ebitda_value": [
            "ev_ebitda_value",
            "ev_ebitda",
            "ev_to_ebitda_value",
            "ev_ebitda_fair_value",
            "ev_ebitda_price",
        ],
        "pe_value": ["pe_value", "pe", "p_e_value", "pe_fair_value", "pe_price"],
        "sentiment_score": ["sentiment_score", "sentiment", "news_sentiment_score"],
        "sentiment": ["sentiment_text", "sentiment_label", "sentiment_str", "sentiment"],
    }

    out = pd.DataFrame(index=d.index)

    # valuation_date (optional)
    vd_col = _col(d, *mapping["valuation_date"])
    if vd_col is not None:
        out["valuation_date"] = pd.to_datetime(d[vd_col], errors="coerce").dt.normalize()

    for canon in ["dcf_value", "ev_ebitda_value", "pe_value"]:
        c = _col(d, *mapping[canon])
        if c is not None:
            out[canon] = pd.to_numeric(d[c], errors="coerce")

    # sentiment fields (optional)
    s_score_col = _col(d, *mapping["sentiment_score"])
    if s_score_col is not None:
        out["sentiment_score"] = pd.to_numeric(d[s_score_col], errors="coerce")

    s_col = _col(d, *mapping["sentiment"])
    if s_col is not None:
        out["sentiment"] = d[s_col].astype(str)

    for extra in ("ticker", "source", "notes", "anchor_validated"):
        if extra in d.columns:
            out[extra] = d[extra]

    return out


def _validate_output_df(df: pd.DataFrame) -> None:
    required = ["ticker", "anchor_fair_value", "anchor_currency", "valuation_date"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Output is missing required columns: {missing}. Present columns: {list(df.columns)}")

    for i, v in enumerate(pd.to_numeric(df["anchor_fair_value"], errors="coerce")):
        if pd.isna(v) or float(v) <= 0:
            raise ValueError(f"anchor_fair_value must be positive on row {i}, got {df['anchor_fair_value'].iloc[i]!r}")


def _row_ticker(row: pd.Series) -> str:
    t = row.get("ticker")
    if t is None or (isinstance(t, float) and pd.isna(t)) or str(t).strip() == "":
        return TICKER
    return str(t).strip().upper()


def main() -> int:
    parser = argparse.ArgumentParser(description="Build processed VNM anchor valuation from raw CSV/Excel.")
    parser.add_argument(
        "--input",
        dest="input_path",
        default=None,
        help="Optional path to raw valuation file (default: auto-detect in data/raw/).",
    )
    parser.add_argument(
        "--output",
        dest="output_path",
        default=None,
        help="Output parquet path (default: data/processed/vnm_anchor_valuation.parquet).",
    )
    args = parser.parse_args()

    raw_dir = Path("data") / "raw"
    input_path = Path(args.input_path) if args.input_path else _auto_detect_raw_file(raw_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"Raw valuation input not found: {input_path}")

    raw_df = _read_raw(input_path)
    norm = _normalize(raw_df)

    if "valuation_date" not in norm.columns:
        raise ValueError(
            "Multi-row anchor time series requires a date column: valuation_date, date, as_of_date, or as_of."
        )

    norm = norm.dropna(subset=["valuation_date"])
    if norm.empty:
        raise ValueError("No rows with a parseable valuation_date after normalization.")

    norm = norm.sort_values("valuation_date", ascending=True)
    norm = norm.drop_duplicates(subset=["valuation_date"], keep="last")

    dropped_unusable = 0
    out_rows: list[dict[str, Any]] = []
    for _, row in norm.iterrows():
        if not _row_has_usable_anchor_inputs(row):
            dropped_unusable += 1
            continue

        anchor_fair_value, used_weights = _compute_anchor_value(row)

        out_row: dict[str, Any] = {
            "ticker": _row_ticker(row),
            "anchor_fair_value": float(anchor_fair_value),
            "anchor_currency": ANCHOR_CURRENCY,
            "valuation_date": pd.Timestamp(row["valuation_date"]).date().isoformat(),
            "anchor_validated": bool(is_anchor_row_validated_for_production(row)),
            "anchor_method_weights": used_weights,
        }

        for c in ["dcf_value", "ev_ebitda_value", "pe_value", "sentiment_score"]:
            if c in row.index and pd.notna(row.get(c)):
                v = pd.to_numeric(row.get(c), errors="coerce")
                if pd.notna(v):
                    out_row[c] = float(v)

        if "sentiment" in row.index:
            s = str(row.get("sentiment") or "").strip()
            if s:
                out_row["sentiment"] = s

        for c in ("source", "notes"):
            if c in row.index and row.get(c) is not None and str(row.get(c)).strip() != "":
                out_row[c] = str(row.get(c)).strip()

        out_rows.append(out_row)

    if dropped_unusable:
        print(
            f"WARNING: dropped {dropped_unusable} row(s) with no usable dcf_value / ev_ebitda_value / pe_value.",
            file=sys.stderr,
        )

    if not out_rows:
        raise ValueError(
            "All rows were unusable: no row had at least one of dcf_value, ev_ebitda_value, pe_value. "
            "Fix raw input or add method columns."
        )

    out_df = pd.DataFrame(out_rows)
    out_df = out_df.sort_values("valuation_date", ascending=True).reset_index(drop=True)
    _validate_output_df(out_df)

    out_path = Path(args.output_path) if args.output_path else Path("data") / "processed" / "vnm_anchor_valuation.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out_path, index=False)

    n = len(out_df)
    print(f"OK: wrote {n} row(s) to {out_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
