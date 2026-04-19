"""High-level checks for deterministic anchor freshness audit export."""

from __future__ import annotations

from vnm_valuation.anchor_freshness_audit import DEFAULT_FRESHNESS_AUDIT_DATES, run_anchor_freshness_audit
from vnm_valuation.valuation import STALE_ANCHOR_MAX_AGE_DAYS


def test_anchor_freshness_audit_shape_and_freshness_rules():
    df = run_anchor_freshness_audit()
    assert len(df) == len(DEFAULT_FRESHNESS_AUDIT_DATES)
    assert list(df["as_of_date"]) == DEFAULT_FRESHNESS_AUDIT_DATES
    assert "anchor_error_message" in df.columns
    assert int(df["stale_cutoff_days"].iloc[0]) == STALE_ANCHOR_MAX_AGE_DAYS

    by = df.set_index("as_of_date")

    assert by.loc["2024-04-15", "selected_anchor_date"] == "2024-03-31"
    assert by.loc["2024-10-15", "selected_anchor_date"] == "2024-09-30"
    assert by.loc["2025-04-10", "selected_anchor_date"] == "2025-03-31"
    assert by.loc["2025-10-15", "selected_anchor_date"] == "2025-09-30"
    assert by.loc["2026-04-16", "selected_anchor_date"] == "2026-03-31"
    assert by.loc["2027-04-15", "selected_anchor_date"] == "2026-03-31"

    for d in ["2024-04-15", "2024-10-15", "2025-04-10", "2025-10-15", "2026-04-16"]:
        row = by.loc[d]
        assert bool(row["anchor_used"]) is True
        assert row["anchor_status"] == "used"
        assert row["valuation_mode"] == "anchor_adjusted"
        assert bool(row["is_stale"]) is False
        assert float(row["anchor_age_days"]) >= 0

    late = by.loc["2028-06-01"]
    assert bool(late["is_stale"]) is True
    assert bool(late["anchor_used"]) is False
    assert late["anchor_status"] == "stale"
    assert late["valuation_mode"] == "market_fallback"
    assert str(late["anchor_error_message"] or "").lower().find("stale") >= 0

    mid = by.loc["2027-04-15"]
    assert bool(mid["is_stale"]) is True
    assert bool(mid["anchor_used"]) is False
    assert mid["anchor_status"] == "stale"
