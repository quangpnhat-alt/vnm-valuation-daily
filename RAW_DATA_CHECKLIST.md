# RAW_DATA_CHECKLIST

## VNM market raw input
- Suggested filename: `data/raw/vnm_daily_market.csv` (or `data/raw/vnm_prices.csv`)
- Acceptable formats: **CSV**
- Minimum expected columns:
  - `date`
  - `close` (or `last` / `price` / `adj_close`)
  - `ticker` (or `symbol`) (optional; builder defaults to `VNM` if missing)
- Optional columns (kept if present):
  - `traded_value`, `trailing_pe`, `trailing_ev_ebitda`, `sentiment_score`
- Builder script: `scripts/build_vnm_daily_market.py`

## VNM anchor valuation raw input
- Suggested filename: `data/raw/vnm_anchor_valuation.xlsx` (or `.csv`)
- Acceptable formats: **CSV, Excel (`.xlsx`/`.xls`)**
- Minimum expected columns (at least one of these must exist):
  - `dcf_value` (or `dcf`)
  - `target_pe` (or `pe_target`)
  - `target_ev_ebitda` (or `ev_ebitda_target`)
  - `fair_value` (generic fallback)
- Optional columns:
  - `date`, `source`, `notes`
- Builder script: `scripts/build_vnm_anchor_valuation.py`

## FX raw input
- Suggested filename: `data/raw/usd_vnd_fx.csv`
- Acceptable formats: **CSV**
- Minimum expected columns:
  - `date`
  - `usd_vnd` (or `fx`, `exchange_rate`, `rate`)
- Optional columns:
  - `source`
- Builder script: `scripts/build_fx_input.py`

## Input-cost raw input
- Suggested filename: `data/raw/vnm_input_costs.xlsx` (or `.csv`)
- Acceptable formats: **CSV, Excel (`.xlsx`/`.xls`)**
- Minimum expected columns:
  - `date`
  - at least one cost-related field such as:
    - `milk_powder_price`
    - `skim_milk_powder_price`
    - `whole_milk_powder_price`
    - `sugar_price`
    - `feed_cost_index`
    - `packaging_cost_index`
- Optional columns:
  - `source`, `notes`
- Builder script: `scripts/build_input_costs.py`
