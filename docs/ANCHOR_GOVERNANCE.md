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
| 6 | Optional | Run **`pytest`** so anchor regression, e2e, and timeline mini-backtest tests stay green (`tests/test_reviewed_anchor_*.py`, `tests/test_anchor_fallback_e2e.py`, `tests/test_reviewed_anchor_timeline_backtest.py`). |

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

## Reviewed timeline mini backtest (deterministic export)

Offline sweep over fixed **`as_of_date`** values using deterministic market / FX / input-cost inputs and the reviewed anchor timeline (aligned with **`data/raw/vnm_anchor_valuation.csv`**). Writes one **audit CSV** per run so you can confirm **latest-on-or-before** selection and **stale** / **validation** behavior without opening pipeline outputs under **`data/processed/`** or **`data/output/`**.

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

## Related code (read-only reference)

- Selection + stale + validation: `src/vnm_valuation/valuation.py`
- Raw → processed build: `scripts/build_vnm_anchor_valuation.py`
- Tests: `tests/test_reviewed_anchor_regression.py`, `tests/test_reviewed_anchor_e2e.py`, `tests/test_anchor_fallback_e2e.py`, `tests/test_reviewed_anchor_timeline_backtest.py`
- Deterministic mini backtest: `src/vnm_valuation/mini_backtest.py`, `scripts/run_reviewed_anchor_timeline_backtest.py`
