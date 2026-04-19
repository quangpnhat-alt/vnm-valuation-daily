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

## Draft → review → promote

1. **Draft:** Add or edit rows in `data/raw/vnm_anchor_valuation.csv` with **`anchor_validated=false`** and/or draft wording in `notes`/`source` (avoid promoting until reviewed).
2. **Review:** Use the checklist below; confirm units, sources, and reasonableness vs market.
3. **Promote:** Set **`anchor_validated=true`** for the approved snapshot row; update `notes`/`source` so they do not contain unintended draft markers (e.g. remove `placeholder` text if you rely on keyword rules).
4. **Rebuild** processed parquet (required).
5. **Verify** a test run uses **`anchor_adjusted`** (see below).

---

## Required fields before setting `anchor_validated=true`

- **`valuation_date`** correct for the model freeze.
- At least **one** of `dcf_value`, `ev_ebitda_value`, `pe_value` populated and **consistent units** with market price (VND/share).
- **`source`** identifying the model artefact.
- **Reviewer accountability** — either in `notes` or a separate column your team adds operationally (e.g. `reviewed_by`).

---

## Review checklist (one snapshot)

- [ ] `valuation_date` matches the intended reporting / model freeze.
- [ ] Method inputs share the same unit convention (VND/share).
- [ ] `source` points to a specific workbook/file/version.
- [ ] Large gaps vs market price are **acknowledged** in `notes` (judgment call, not automatic).
- [ ] **`anchor_validated=true`** only after sign-off.
- [ ] Rebuild processed parquet and spot-check one `as_of_date`.

---

## Rebuild processed anchor parquet

From the project root (after `pip install -e .` if you use package imports):

```bash
python scripts/build_vnm_anchor_valuation.py
```

Optional input path:

```bash
python scripts/build_vnm_anchor_valuation.py --input data/raw/vnm_anchor_valuation.csv
```

Output: `data/processed/vnm_anchor_valuation.parquet` (one row per `valuation_date`, sorted ascending).

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
