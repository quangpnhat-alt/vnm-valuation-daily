from __future__ import annotations

from pathlib import Path

import pandas as pd


END_DATE = pd.Timestamp("2026-04-18")
N_DAYS = 45  # >= 40, gives enough buffer for rolling windows


def _date_range(end_date: pd.Timestamp, n_days: int) -> pd.DatetimeIndex:
    # Daily calendar days (not business days) to keep it simple and predictable.
    start = (end_date.normalize() - pd.Timedelta(days=n_days - 1)).normalize()
    return pd.date_range(start=start, end=end_date.normalize(), freq="D")


def _make_market_df(dates: pd.DatetimeIndex) -> pd.DataFrame:
    # VNM close (VND) with mild uptrend + gentle waves.
    t = pd.Series(range(len(dates)), index=dates, dtype="float64")
    close = 72000 + 120 * t + 900 * (pd.Series(dates.dayofyear, index=dates) % 7 - 3)
    close = close.clip(lower=60000)

    # Optional debug fields (not required by schemas.py, but useful).
    sentiment_score = ((t / t.max()) * 0.6 - 0.3).clip(-1, 1)  # drifts from -0.3 to +0.3
    traded_value = (close * (2.0e6 + 2.5e4 * t)).round(0)  # VND notionals
    trailing_pe = (18.0 + 0.02 * t + (t % 10) * 0.03).round(2)
    trailing_ev_ebitda = (11.0 + 0.015 * t + (t % 14) * 0.02).round(2)

    df = pd.DataFrame(
        {
            "date": dates.date.astype(str),
            "ticker": "VNM",
            "close": close.round(0).astype("float64"),
            "currency": "VND",
            "sentiment_score": sentiment_score.round(3).astype("float64"),
            "traded_value": traded_value.astype("float64"),
            "trailing_pe": trailing_pe.astype("float64"),
            "trailing_ev_ebitda": trailing_ev_ebitda.astype("float64"),
        }
    )
    return df


def _make_fx_df(dates: pd.DatetimeIndex) -> pd.DataFrame:
    # USD/VND with small drift + cycles so 5D/20D signals are non-zero.
    t = pd.Series(range(len(dates)), index=dates, dtype="float64")
    rate = 24500 + 6.5 * t + 40 * ((pd.Series(dates.dayofyear, index=dates) % 9) - 4)
    df = pd.DataFrame(
        {
            "date": dates.date.astype(str),
            "base_ccy": "USD",
            "quote_ccy": "VND",
            "rate": rate.round(2).astype("float64"),
            "source": "sample",
        }
    )
    return df


def _make_input_cost_df(dates: pd.DatetimeIndex) -> pd.DataFrame:
    # Costs in USD; raw_milk is the primary item valuation.py will use.
    t = pd.Series(range(len(dates)), index=dates, dtype="float64")
    raw_milk = 0.48 + 0.0009 * t + 0.01 * ((pd.Series(dates.dayofyear, index=dates) % 8) - 3.5)
    sugar = 0.52 + 0.0006 * t + 0.008 * ((pd.Series(dates.dayofyear, index=dates) % 10) - 4.5)
    soybean = 0.44 + 0.0007 * t + 0.007 * ((pd.Series(dates.dayofyear, index=dates) % 11) - 5)

    rows = []
    for item, series in [
        ("raw_milk", raw_milk),
        ("sugar", sugar),
        ("soybean", soybean),
    ]:
        rows.append(
            pd.DataFrame(
                {
                    "date": dates.date.astype(str),
                    "item": item,
                    "cost": series.round(4).astype("float64"),
                    "currency": "USD",
                    "unit": "kg",
                    "source": "sample",
                }
            )
        )

    return pd.concat(rows, ignore_index=True)


def _make_anchor_df(market_df: pd.DataFrame) -> pd.DataFrame:
    # One-row anchor: fair value ~ 12-18% above latest close for debugging.
    latest_close = float(pd.to_numeric(market_df["close"], errors="coerce").dropna().iloc[-1])
    anchor_fair_value = round(latest_close * 1.15, 0)
    return pd.DataFrame(
        [
            {
                "ticker": "VNM",
                "anchor_fair_value": float(anchor_fair_value),
                "anchor_currency": "VND",
                "sentiment_score": 0.2,
            }
        ]
    )


def main() -> int:
    processed_dir = Path("data") / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    dates = _date_range(END_DATE, N_DAYS)
    market_df = _make_market_df(dates)
    fx_df = _make_fx_df(dates)
    input_cost_df = _make_input_cost_df(dates)
    anchor_df = _make_anchor_df(market_df)

    out_market = processed_dir / "vnm_daily_market.parquet"
    out_fx = processed_dir / "daily_fx.parquet"
    out_cost = processed_dir / "daily_input_cost.parquet"
    out_anchor = processed_dir / "vnm_anchor_valuation.parquet"

    market_df.to_parquet(out_market, index=False)
    fx_df.to_parquet(out_fx, index=False)
    input_cost_df.to_parquet(out_cost, index=False)
    anchor_df.to_parquet(out_anchor, index=False)

    for p in [out_market, out_fx, out_cost, out_anchor]:
        print(str(p.resolve()))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

