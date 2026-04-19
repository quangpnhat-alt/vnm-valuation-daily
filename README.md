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

## Folders
- `config/`: YAML configuration
- `data/raw/`: raw inputs (manual drops or pulls)
- `data/processed/`: cleaned canonical parquet
- `data/output/`: daily outputs and exports
- `src/vnm_valuation/`: library code
- `scripts/`: runnable entrypoints
