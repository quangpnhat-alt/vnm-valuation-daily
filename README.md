# VNM Daily Valuation (Local MVP)

Local daily valuation MVP for **VNM (Vinamilk)**. This repo is **local-first** (pandas + parquet + YAML) and is designed to run end-to-end on a laptop. Optional **exports** include Google Sheets (when configured) and a **local Excel workbook** as a parallel or offline-friendly path.

## What this repo will do (MVP)
- Load local input data (prices, FX if needed, shares outstanding, etc.)
- Produce a daily valuation table saved locally (parquet)
- Optionally export the daily row to **Google Sheets** and/or **Excel** (`output/vnm_daily_valuation.xlsx` by default)

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

## Google Sheets (optional publish)

Separate **export** step: push one daily valuation row to a spreadsheet (does not change valuation logic). Configure with environment variables—**never commit** credential JSON or spreadsheet IDs.

| Variable | Purpose |
|----------|---------|
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Filesystem path to the service account JSON key |
| `VNM_GSHEET_SPREADSHEET_ID` | Target spreadsheet ID |
| `VNM_GSHEET_WORKSHEET` | Worksheet/tab name (optional; default `daily_valuation`) |

```bash
python scripts/publish_vnm_daily_to_gsheet.py --as-of-date 2026-04-16
python scripts/publish_vnm_daily_to_gsheet.py --dry-run --as-of-date 2026-04-16   # local valuation only; no API calls
```

Requires the same processed inputs under `data/processed/` as `scripts/run_local_valuation.py`. Writes are **idempotent** by `as_of_date` + `ticker` (updates an existing row or appends). See **`docs/ANCHOR_GOVERNANCE.md`** for details.

## Excel (local daily export)

Parallel **file-based** export: one row per run into a workbook under **`output/`** (default **`output/vnm_daily_valuation.xlsx`**, worksheet **`daily_valuation`**). No cloud credentials; safe for daily runs when Sheets is unavailable. Same idempotency rule as Sheets (`as_of_date` + `ticker`).

```bash
python scripts/export_vnm_daily_to_excel.py --as-of-date 2026-04-16
python scripts/export_vnm_daily_to_excel.py --dry-run --as-of-date 2026-04-16
```

Use `--output PATH` and `--worksheet NAME` to override defaults.

## Daily pipeline (valuation + exports)

Single entrypoint: run **`run_daily_valuation` once**, then export (Excel **on** by default; Google Sheets **only** with `--to-gsheet`). Same processed inputs as `run_local_valuation.py`.

**Excel only (typical local daily):**

```bash
python scripts/run_vnm_daily_pipeline.py --as-of-date 2026-04-16
```

**Excel + Google Sheets** (requires env vars; use `--dry-run` to preview without writes/API calls):

```bash
python scripts/run_vnm_daily_pipeline.py --as-of-date 2026-04-16 --to-gsheet
python scripts/run_vnm_daily_pipeline.py --dry-run --as-of-date 2026-04-16 --to-gsheet
```

Use `--no-excel` to skip the workbook, `--best-effort-gsheet` to keep Excel success if Sheets fails or is misconfigured. See **`docs/ANCHOR_GOVERNANCE.md`**.

## Folders
- `config/`: YAML configuration
- `data/raw/`: raw inputs (manual drops or pulls)
- `data/processed/`: cleaned canonical parquet
- `data/output/`: daily outputs and exports
- `src/vnm_valuation/`: library code
- `scripts/`: runnable entrypoints
