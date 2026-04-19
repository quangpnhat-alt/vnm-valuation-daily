"""High-level checks for deterministic anchor fallback reason audit export."""

from __future__ import annotations

from vnm_valuation.anchor_fallback_reason_audit import (
    default_fallback_reason_scenarios,
    fallback_reason_from_anchor_status,
    run_anchor_fallback_reason_audit,
)


def test_fallback_reason_mapping_helpers():
    assert fallback_reason_from_anchor_status("used") == "used"
    assert fallback_reason_from_anchor_status("stale") == "stale"
    assert fallback_reason_from_anchor_status("unvalidated") == "unvalidated"
    assert fallback_reason_from_anchor_status("missing") == "missing"
    assert fallback_reason_from_anchor_status("invalid") == "other_error"
    assert fallback_reason_from_anchor_status("weird") == "other_error"


def test_anchor_fallback_reason_audit_default_scenarios():
    scenarios = default_fallback_reason_scenarios()
    df = run_anchor_fallback_reason_audit(scenarios)
    assert len(df) == len(scenarios)
    assert set(df["scenario_name"]) == {
        "reviewed_used_2025q1",
        "reviewed_stale_2028",
        "unvalidated_anchor_only",
        "no_anchor_available",
    }

    by = df.set_index("scenario_name")

    u = by.loc["reviewed_used_2025q1"]
    assert u["fallback_reason"] == "used"
    assert u["anchor_status"] == "used"
    assert bool(u["anchor_used"]) is True
    assert str(u["anchor_error_message"] or "") == ""

    s = by.loc["reviewed_stale_2028"]
    assert s["fallback_reason"] == "stale"
    assert s["anchor_status"] == "stale"
    assert bool(s["anchor_used"]) is False
    assert "stale" in str(s["anchor_error_message"]).lower()

    uv = by.loc["unvalidated_anchor_only"]
    assert uv["fallback_reason"] == "unvalidated"
    assert uv["anchor_status"] == "unvalidated"
    assert bool(uv["anchor_used"]) is False
    assert "validated" in str(uv["anchor_error_message"]).lower()

    miss = by.loc["no_anchor_available"]
    assert miss["fallback_reason"] == "missing"
    assert miss["anchor_status"] == "missing"
    assert bool(miss["anchor_used"]) is False
    assert "on or before" in str(miss["anchor_error_message"]).lower()

    reasons = set(df["fallback_reason"].tolist())
    assert {"used", "stale", "unvalidated", "missing"}.issubset(reasons)
