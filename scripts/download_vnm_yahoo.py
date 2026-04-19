"""
Tải dữ liệu lịch sử VNM từ Yahoo Finance (ticker VNM.VN), lưu CSV chuẩn hóa cho pipeline.

Cần cài: pip install yfinance pandas
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

try:
    import yfinance as yf
except ImportError as e:
    raise ImportError(
        "Thiếu thư viện yfinance. Chạy: python -m pip install yfinance"
    ) from e

YAHOO_SYMBOL = "VNM.VN"
TICKER_OUT = "VNM"
START = "2015-01-01"
OUT_PATH = Path("data") / "raw" / "vnm_daily_market.csv"


def main() -> int:
    # Yahoo Finance `end` là exclusive — cộng 1 ngày để lấy đủ đến hôm nay.
    end_exclusive = (date.today() + timedelta(days=1)).isoformat()
    t = yf.Ticker(YAHOO_SYMBOL)
    df = t.history(start=START, end=end_exclusive, auto_adjust=False)

    if df is None or df.empty:
        raise RuntimeError(
            f"Không tải được dữ liệu cho {YAHOO_SYMBOL} "
            f"(khoảng {START} đến {date.today().isoformat()}). "
            "Kiểm tra kết nối mạng hoặc ticker."
        )

    # Chuẩn hóa cột (yfinance: Open, High, Low, Close, Adj Close, Volume)
    rename = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }
    missing = [c for c in rename if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"Dữ liệu trả về thiếu cột: {missing}. Có sẵn: {list(df.columns)}"
        )

    out = df[list(rename.keys())].rename(columns=rename).copy()
    idx = pd.DatetimeIndex(out.index)
    if idx.tz is not None:
        idx = idx.tz_convert(None)
    idx = idx.normalize()
    out.insert(0, "date", idx.strftime("%Y-%m-%d"))
    out["ticker"] = TICKER_OUT

    cols = [
        "date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
    ]
    out = out[cols]
    out = out.sort_values("date", ascending=True)
    out = out.drop_duplicates(subset=["date", "ticker"], keep="last").reset_index(drop=True)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_PATH, index=False)

    print(f"Wrote {len(out)} rows -> {OUT_PATH.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
