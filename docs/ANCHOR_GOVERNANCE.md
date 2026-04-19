# Anchor governance (raw → processed → valuation)

This project uses a **multi-row** anchor time series in `data/processed/vnm_anchor_valuation.parquet`. Valuation picks the **latest row with `valuation_date ≤ as_of_date`**, then applies **stale protection** and **validation** before `anchor_adjusted` mode.

**Nothing in this document changes valuation formulas or stale-date rules** — it documents how to operate them safely.

---

## Raw anchor CSV schema

Recommended columns (headers):

| Column | Required | Description |
|--------|----------|-------------|
| `valuation_date` | Yes | Snapshot effective date (ISO `YYYY-MM-DD`). Also accepts column name `date` in raw files (normalized to `valuation_date`). |
| `ticker` | Optional | Should be `VNM` (defaulted if missing in builder). |
| `dcf_value` | At least one | Fair value from DCF (same unit as price, typically VND/share). |
| `ev_ebitda_value` | At least one | Fair value from EV/EBITDA. |
| `pe_value` | At least one | Fair value from P/E. |
| `source` | Recommended | Traceability (workbook name, file, version). |
| `notes` | Recommended | Assumptions, scope, review notes. |
| `anchor_validated` | Recommended for production | Explicit trust flag (see below). |

The builder blends `dcf_value` / `ev_ebitda_value` / `pe_value` with weights **50% / 30% / 20%** when all three exist (unchanged business rule).

---

## Meaning of `anchor_validated`

- **`true`** (or `1`, `yes`, `y`): this row **may** drive **`anchor_adjusted`** valuation, if it is also **not stale** and successfully selected.
- **`false`** (or `0`, `no`, `n`): row is treated as **not approved** → valuation falls back to **`market_fallback`** for that run (same as other anchor failures from the app’s perspective).

If **`anchor_validated` is missing or empty**, validation falls back to **rules**:

- If **`placeholder`** appears in **`notes` or `source`** (case-insensitive) → **not validated**.
- Otherwise → **validated** (backward compatibility for old files).

**Best practice:** set **`anchor_validated=false`** on drafts; set **`anchor_validated=true`** only after review.

---

## Workflow: draft → review → promote

| Step | Owner / action | What to do |
|------|------------------|------------|
| 1 | Analyst | Add or edit rows in `data/raw/vnm_anchor_valuation.csv` with **`anchor_validated=false`** for drafts, or leave notes/source in clearly draft form. |
| 2 | Reviewer | Walk the **review checklist** (below); confirm units (VND/share), model version, and reasonableness vs market and external references. |
| 3 | Analyst | For an approved snapshot: set **`anchor_validated=true`**, update **`source`** / **`notes`** so they read as **production** (remove unintended **placeholder** wording if you rely on keyword validation). |
| 4 | Build | Rebuild processed parquet (**required** after any raw change). |
| 5 | Verify | Run valuation for a realistic **`as_of_date`**; confirm **`anchor_adjusted`** vs **`market_fallback`** (see verification table). |
| 6 | Optional | Run **`pytest`** so anchor regression, e2e, timeline mini-backtest, freshness audit, and fallback reason audit tests stay green (`tests/test_reviewed_anchor_*.py`, `tests/test_anchor_fallback_e2e.py`, `tests/test_reviewed_anchor_timeline_backtest.py`, `tests/test_anchor_freshness_audit.py`, `tests/test_anchor_fallback_reason_audit.py`). |

---

## Review checklist (per snapshot)

Use this **before** setting **`anchor_validated=true`**:

- [ ] **`valuation_date`** matches the model freeze / reporting cut you intend (e.g. quarter-end).
- [ ] **`dcf_value`**, **`ev_ebitda_value`**, **`pe_value`** use the **same unit** (typically VND/share) and the same share count / dilution assumptions across methods.
- [ ] **`source`** names a specific artefact (workbook name, version, path, or ticket).
- [ ] **`notes`** record material assumptions and, if fair value is far from spot, **why** (judgment, not only the model).
- [ ] **External sense-check** (e.g. vs broker range or your house view) is **noted** when used.
- [ ] No accidental **`placeholder`** text in **`notes`** / **`source`** unless the row must stay **non-production**.
- [ ] After promotion, **rebuild** parquet and spot-check one **`as_of_date`** (and CI tests if applicable).

---

## Required fields before setting `anchor_validated=true`

- **`valuation_date`** correct for the model freeze.
- At least **one** of `dcf_value`, `ev_ebitda_value`, `pe_value` populated and **consistent units** with market price (VND/share).
- **`source`** identifying the model artefact.
- **Reviewer accountability** — either in `notes` or a separate column your team adds operationally (e.g. `reviewed_by`).

---

## Rebuild processed anchor parquet

From the project root (after `pip install -e .` if you use package imports):

```bash
python scripts/build_vnm_anchor_valuation.py
```

Optional paths:

```bash
python scripts/build_vnm_anchor_valuation.py --input data/raw/vnm_anchor_valuation.csv
python scripts/build_vnm_anchor_valuation.py --input data/raw/vnm_anchor_valuation.csv --output data/processed/vnm_anchor_valuation.parquet
```

Default output: `data/processed/vnm_anchor_valuation.parquet` (one row per `valuation_date`, sorted ascending).

---

## Verify `anchor_adjusted` vs `market_fallback`

After running the daily pipeline or `scripts/run_local_valuation.py`, open **`data/output/vnm_daily_valuation.csv`** (or the printed summary line) and check:

| Field | `anchor_adjusted` | `market_fallback` |
|-------|-------------------|-------------------|
| `valuation_mode` | `anchor_adjusted` | `market_fallback` |
| `anchor_used` | `True` | `False` |
| `anchor_status` | `used` | e.g. `stale`, `unvalidated`, `missing`, … |
| `anchor_fair_value` | numeric | often empty / NaN |
| `anchor_error_message` | empty | explains fallback when not used |

Example run:

```bash
python scripts/run_local_valuation.py --as-of-date 2026-04-16
```

**Fallback smoke-checks (non-exhaustive):** placeholder in **`notes`** / **`source`**, explicit **`anchor_validated=false`**, anchor older than **365** days vs `as_of_date`, or no row **on or before** `as_of_date` → expect **`market_fallback`** with a non-empty **`anchor_error_message`**.

---

## Deterministic anchor audit exports (summary)

These are **offline** utilities: fixed inputs (see `src/vnm_valuation/deterministic_inputs.py`), **`run_daily_valuation`** where applicable, and CSVs under **`output/`** (typically git-ignored—re-run to refresh). They do **not** replace the production pipeline or change valuation rules.

| Utility | What it is for | Run | Output CSV |
|---------|----------------|-----|------------|
| **Reviewed timeline mini-backtest** | Broad date sweep on the **reviewed** anchor timeline: selection, **`anchor_used`**, **`valuation_mode`**, messages — same spirit as a multi-date regression check. | `python scripts/run_reviewed_anchor_timeline_backtest.py` | `output/reviewed_anchor_timeline_backtest.csv` |
| **Anchor freshness audit** | **Coverage and lag**: which anchor is latest on/before each date, **`anchor_age_days`** vs **`stale_cutoff_days`**, and whether production reports **`stale`**. | `python scripts/run_anchor_freshness_audit.py` | `output/anchor_freshness_audit.csv` |
| **Anchor fallback reason audit** | **Why fallback**: named scenarios (including fixtures for unvalidated / missing) with a normalized **`fallback_reason`** bucket aligned to **`anchor_status`**. | `python scripts/run_anchor_fallback_reason_audit.py` | `output/anchor_fallback_reason_audit.csv` |

Implementation references: `src/vnm_valuation/mini_backtest.py` (timeline sweep), `src/vnm_valuation/anchor_freshness_audit.py`, `src/vnm_valuation/anchor_fallback_reason_audit.py`. Tests: `tests/test_reviewed_anchor_timeline_backtest.py`, `tests/test_anchor_freshness_audit.py`, `tests/test_anchor_fallback_reason_audit.py`.

---

## Reviewed timeline mini backtest (deterministic export)

Offline sweep over fixed **`as_of_date`** values using deterministic market / FX / input-cost inputs and the reviewed anchor timeline (aligned with **`data/raw/vnm_anchor_valuation.csv`**, including reviewed snapshots from **2024** onward). Writes one **audit CSV** per run so you can confirm **latest-on-or-before** selection and **stale** / **validation** behavior without opening pipeline outputs under **`data/processed/`** or **`data/output/`**.

From the project root (after `pip install -e .` if needed):

```bash
python scripts/run_reviewed_anchor_timeline_backtest.py
```

Output: **`output/reviewed_anchor_timeline_backtest.csv`** (generated; `*.csv` under **`output/`** is git-ignored—re-run the command to refresh).

Key columns:

| Column | Role |
|--------|------|
| `as_of_date` | Run date. |
| `selected_anchor_date` | Latest anchor **`valuation_date`** on or before `as_of_date` (audit helper). |
| `anchor_used` | `True` if anchor-adjusted path succeeded. |
| `anchor_status` | `used`, or e.g. `stale`, `unvalidated`, `missing`. |
| `valuation_mode` | `anchor_adjusted` or `market_fallback`. |
| `anchor_error_message` | Empty if anchor used; otherwise explains fallback. |

**Stale fallback example:** If the only reviewed snapshot on or before `as_of_date` is **2026-03-31** but that date is more than **365** days before `as_of_date` (e.g. a far-future run with no newer row), the CSV still lists **`selected_anchor_date`** = **2026-03-31**, but **`valuation_mode`** = **`market_fallback`**, **`anchor_used`** = false, **`anchor_status`** = **`stale`**, and **`anchor_error_message`** states the stale rule.

Regression: `tests/test_reviewed_anchor_timeline_backtest.py`.

---

## Anchor gap / freshness audit (deterministic export)

Focused **coverage and lag** check: for fixed **`as_of_date`** values, records **which reviewed anchor** is selected (latest on or before the date), **age in days** vs `as_of_date`, **`stale_cutoff_days`** (from `STALE_ANCHOR_MAX_AGE_DAYS` in `valuation.py`), and the same **`anchor_status` / `valuation_mode` / `anchor_used` / `anchor_error_message`** fields as a real run — without asserting valuation performance.

From the project root (after `pip install -e .` if needed):

```bash
python scripts/run_anchor_freshness_audit.py
```

Output: **`output/anchor_freshness_audit.csv`** (generated; re-run to refresh).

How to read it:

- **`anchor_age_days`**: calendar days from **`selected_anchor_date`** to **`as_of_date`** (deterministic given the timeline).
- **`is_stale`**: `True` when **`anchor_status`** is **`stale`** (same outcome as production for that run — anchor older than the cutoff vs `as_of_date`).
- **`stale_cutoff_days`**: copy of the max-age constant for the audit row (compare to **`anchor_age_days`**).

Regression: `tests/test_anchor_freshness_audit.py`.

---

## Anchor fallback reason audit (deterministic export)

Offline **why did we fall back?** sweep: named scenarios (reviewed used, reviewed stale, explicit unvalidated fixture, future-only / missing-on-or-before) each run **`run_daily_valuation`** once. The CSV adds **`fallback_reason`**, a small normalized label derived from **`anchor_status`** (`used`, `stale`, `unvalidated`, `missing`, or `other_error` for unexpected statuses such as `invalid`). It does **not** re-implement validation or stale rules.

From the project root (after `pip install -e .` if needed):

```bash
python scripts/run_anchor_fallback_reason_audit.py
```

Output: **`output/anchor_fallback_reason_audit.csv`** (generated; re-run to refresh).

How to read it:

- **`fallback_reason`**: audit bucket aligned with **`anchor_status`** from the same row (`used` when the anchor path succeeded; otherwise matches **`stale` / `unvalidated` / `missing`** when valuation reports those statuses).
- **`anchor_error_message`**: empty when **`fallback_reason`** is **`used`**; otherwise the same explanation string production would surface.

Regression: `tests/test_anchor_fallback_reason_audit.py`.

---

## Example raw CSV rows

**Draft (not for production anchor_adjusted until promoted):**

```csv
valuation_date,ticker,dcf_value,ev_ebitda_value,pe_value,source,notes,anchor_validated
2026-03-31,VNM,103000,106000,105000,Q1-2026 workbook,DRAFT — pending review,false
```

**Validated (ready for anchor_adjusted if not stale):**

```csv
valuation_date,ticker,dcf_value,ev_ebitda_value,pe_value,source,notes,anchor_validated
2026-03-31,VNM,103000,106000,105000,Q1-2026 workbook,Approved by FP&A 2026-04-20; model v3,true
```

---

## Google Sheets publish (optional export)

`scripts/publish_vnm_daily_to_gsheet.py` runs **`run_daily_valuation`** on local processed inputs (same as `run_local_valuation.py`) and upserts one row into a configured spreadsheet. This is an **operational export layer** only; it does not change valuation, stale, or validation rules. Set **`GOOGLE_SERVICE_ACCOUNT_JSON`**, **`VNM_GSHEET_SPREADSHEET_ID`**, and optionally **`VNM_GSHEET_WORKSHEET`**; do not commit credentials or IDs. Rows include **`valuation_mode`**, **`anchor_status`**, and **`anchor_error_message`** so **`market_fallback`** runs remain visible in the sheet.

---

## Excel workbook export (optional, local)

`scripts/export_vnm_daily_to_excel.py` performs the same **upsert-by-(as_of_date, ticker)** pattern into **`output/vnm_daily_valuation.xlsx`** (by default), without cloud credentials—useful as a parallel or offline-friendly export. It does not change valuation rules.

`scripts/run_vnm_daily_pipeline.py` runs valuation once, then exports (Excel by default, optional `--to-gsheet`) without duplicating the valuation step.

`scripts/backfill_vnm_history_to_excel.py` sweeps a date range into the same Excel workbook (default start **2015-01-01**), upserting each successful day; days without market data are skipped in the summary.

---

## Related code (read-only reference)

- Selection + stale + validation: `src/vnm_valuation/valuation.py`
- Raw → processed build: `scripts/build_vnm_anchor_valuation.py`
- Tests: `tests/test_reviewed_anchor_regression.py`, `tests/test_reviewed_anchor_e2e.py`, `tests/test_anchor_fallback_e2e.py`, `tests/test_reviewed_anchor_timeline_backtest.py`
- Deterministic mini backtest: `src/vnm_valuation/mini_backtest.py`, `scripts/run_reviewed_anchor_timeline_backtest.py`
- Anchor freshness audit: `src/vnm_valuation/anchor_freshness_audit.py`, `scripts/run_anchor_freshness_audit.py`
- Anchor fallback reason audit: `src/vnm_valuation/anchor_fallback_reason_audit.py`, `scripts/run_anchor_fallback_reason_audit.py`
- Google Sheets publish: `src/vnm_valuation/google_sheets_publish.py`, `scripts/publish_vnm_daily_to_gsheet.py`
- Excel export: `src/vnm_valuation/excel_daily_export.py`, `scripts/export_vnm_daily_to_excel.py`
- Daily pipeline (valuation + exports): `src/vnm_valuation/daily_pipeline.py`, `scripts/run_vnm_daily_pipeline.py`
- Excel history backfill: `src/vnm_valuation/excel_history_backfill.py`, `scripts/backfill_vnm_history_to_excel.py`
