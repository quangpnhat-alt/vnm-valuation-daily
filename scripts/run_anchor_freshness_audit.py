"""
Write `output/anchor_freshness_audit.csv` (deterministic anchor coverage / lag audit).

Run from project root (with `pip install -e .`):

  python scripts/run_anchor_freshness_audit.py
"""

from __future__ import annotations

from pathlib import Path

from vnm_valuation.anchor_freshness_audit import run_anchor_freshness_audit, write_freshness_audit_csv


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    df = run_anchor_freshness_audit()
    out = write_freshness_audit_csv(repo / "output" / "anchor_freshness_audit.csv", df)
    print(f"OK: wrote {len(df)} row(s) to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
