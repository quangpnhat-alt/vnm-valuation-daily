"""
Write `output/anchor_fallback_reason_audit.csv` (deterministic fallback reason audit).

Run from project root (with `pip install -e .`):

  python scripts/run_anchor_fallback_reason_audit.py
"""

from __future__ import annotations

from pathlib import Path

from vnm_valuation.anchor_fallback_reason_audit import (
    run_anchor_fallback_reason_audit,
    write_fallback_reason_audit_csv,
)


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    df = run_anchor_fallback_reason_audit()
    out = write_fallback_reason_audit_csv(repo / "output" / "anchor_fallback_reason_audit.csv", df)
    print(f"OK: wrote {len(df)} row(s) to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
