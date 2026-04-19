"""
Write `output/reviewed_anchor_mini_backtest.csv` using deterministic fixtures (no repo data/ I/O).

Run from project root (with package on PYTHONPATH or `pip install -e .`):

  python scripts/run_reviewed_anchor_mini_backtest.py
"""

from __future__ import annotations

from pathlib import Path

from vnm_valuation.mini_backtest import run_mini_backtest, write_mini_backtest_csv


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    df = run_mini_backtest()
    out = write_mini_backtest_csv(repo / "output" / "reviewed_anchor_mini_backtest.csv", df)
    print(f"OK: wrote {len(df)} row(s) to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
