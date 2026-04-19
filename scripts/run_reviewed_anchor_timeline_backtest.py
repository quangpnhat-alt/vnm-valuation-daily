"""
Write `output/reviewed_anchor_timeline_backtest.csv` (deterministic reviewed-anchor timeline sweep).

Run from project root (with `pip install -e .`):

  python scripts/run_reviewed_anchor_timeline_backtest.py
"""

from __future__ import annotations

from pathlib import Path

from vnm_valuation.mini_backtest import run_timeline_backtest, write_mini_backtest_csv


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    df = run_timeline_backtest()
    out = write_mini_backtest_csv(repo / "output" / "reviewed_anchor_timeline_backtest.csv", df)
    print(f"OK: wrote {len(df)} row(s) to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
