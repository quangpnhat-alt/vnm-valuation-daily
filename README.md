# VNM Daily Valuation (Local MVP)

Local daily valuation MVP for **VNM (Vinamilk)**. This repo is **local-first** (pandas + parquet + YAML) and is designed to run end-to-end on a laptop, publishing to Google Sheets only as a final optional step.

## What this repo will do (MVP)
- Load local input data (prices, FX if needed, shares outstanding, etc.)
- Produce a daily valuation table saved locally (parquet)
- Publish the final table to Google Sheets

## Quickstart
Run from the project root (after editable install):

```bash
python -m pip install -e .
python -m pytest -q
python scripts/create_sample_inputs.py
python scripts/run_local_valuation.py --as-of-date 2026-04-18
```

Notes:
- `scripts/run_local_valuation.py` **requires processed input parquet files** in `data/processed/`.
- `scripts/create_sample_inputs.py` is a **temporary bootstrap step** for demo/testing; replace it with real data ingestion later.

## Anchor audit utilities

Deterministic **offline** exports that exercise anchor timeline selection, staleness, and fallback behavior using fixed inputs; each run writes a CSV under `output/` for quick inspection. From the project root, with the package installed (`pip install -e .`), run:

| Utility | Command | Output CSV | What it checks |
|---------|---------|------------|----------------|
| Reviewed timeline mini-backtest | `python scripts/run_reviewed_anchor_timeline_backtest.py` | `output/reviewed_anchor_timeline_backtest.csv` | Multi-date sweep on the reviewed anchor timeline vs `run_daily_valuation` (selection, modes, errors). |
| Anchor freshness audit | `python scripts/run_anchor_freshness_audit.py` | `output/anchor_freshness_audit.csv` | Which anchor applies per date, age vs cutoff, and stale coverage. |
| Anchor fallback reason audit | `python scripts/run_anchor_fallback_reason_audit.py` | `output/anchor_fallback_reason_audit.csv` | Normalized fallback reasons (e.g. stale, unvalidated, missing) from real valuation outcomes. |

CSVs under `output/` are generated artifacts—delete or re-run anytime. For governance detail and field meanings, see **`docs/ANCHOR_GOVERNANCE.md`**.

## Folders
- `config/`: YAML configuration
- `data/raw/`: raw inputs (manual drops or pulls)
- `data/processed/`: cleaned canonical parquet
- `data/output/`: daily outputs and exports
- `src/vnm_valuation/`: library code
- `scripts/`: runnable entrypoints
