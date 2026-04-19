## Scope (MVP)
- **Single asset**: VNM (Vinamilk) only.
- **Local-first**: read/write local files (CSV/Parquet/YAML). No databases.
- **Output**: Google Sheets is used only as the final publishing step.

## Tech choices
- **Language**: Python 3.11+
- **Core libs**: pandas, pyarrow (parquet), pyyaml, gspread (+ google-auth)
- **File formats**
  - **Config**: YAML in `config/`
  - **Data**: Parquet in `data/processed/` (canonical), raw inputs in `data/raw/`
  - **Outputs**: Parquet + a final push to Google Sheets

## Project structure conventions
- **Library code**: `src/vnm_valuation/` (no notebooks-as-source)
- **Runnable scripts**: `scripts/` (thin wrappers that call library code)
- **Data folders**
  - `data/raw/`: immutable-ish raw pulls / manual drops
  - `data/processed/`: cleaned + standardized parquet
  - `data/output/`: daily valuation results + exports

## Pipeline rules
- **Deterministic runs**: a run is keyed by `as_of_date` (YYYY-MM-DD) and is reproducible from inputs.
- **Idempotent writes**: re-running the same `as_of_date` overwrites outputs for that date.
- **Schema stability**: processed parquet schemas should be stable; version schema changes explicitly.

## Coding standards (minimal)
- **Small functions**: prefer pure transforms that accept/return DataFrames.
- **Explicit I/O**: keep file I/O at the edges; core logic should be testable without filesystem.
- **No secrets in repo**: credentials go in local ignored files (see `.gitignore`).
