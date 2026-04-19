## Working now
- Local end-to-end MVP runs for **VNM only**.
- Current local flow: **`pytest` → `scripts/create_sample_inputs.py` → `scripts/run_local_valuation.py`**.
- Outputs are written locally to `data/output/` as:
  - `vnm_daily_valuation.parquet`
  - `vnm_daily_valuation.csv`

## Still using sample data
- Sample-data replacement is **partially complete**:
  - ✅ `scripts/build_vnm_daily_market.py` now builds `data/processed/vnm_daily_market.parquet`
  - ✅ `scripts/build_vnm_anchor_valuation.py` now builds `data/processed/vnm_anchor_valuation.parquet`
- Still sample/placeholder today:
  - `daily_fx.parquet` (until `scripts/build_daily_fx.py` is used)
  - `daily_input_cost.parquet` (until `scripts/build_daily_input_cost.py` exists/used)
- `scripts/create_sample_inputs.py` remains a **bootstrap/demo** helper and should shrink over time as builders replace it.

## Next 3 implementation tasks
1. Implement `scripts/run_local_pipeline.py` to run the full local flow (build inputs → run valuation → write outputs) with one command.
2. Use `scripts/build_daily_fx.py` to replace `daily_fx.parquet` with real USD/VND daily data (enough history for 5D/20D signals).
3. Implement `scripts/build_daily_input_cost.py` to replace `daily_input_cost.parquet` with real local input cost series (at least `raw_milk`).

