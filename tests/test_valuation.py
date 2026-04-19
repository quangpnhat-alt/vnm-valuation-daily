import pandas as pd

from vnm_valuation.valuation import run_daily_valuation


def _base_inputs(as_of: str):
    market_df = pd.DataFrame(
        [
            {"date": as_of, "ticker": "VNM", "close": 100.0, "currency": "VND"},
        ]
    )
    fx_df = pd.DataFrame(
        [
            {"date": as_of, "base_ccy": "USD", "quote_ccy": "VND", "rate": 25000.0},
        ]
    )
    input_cost_df = pd.DataFrame(
        [
            {"date": as_of, "item": "raw_milk", "cost": 1.0, "currency": "USD"},
        ]
    )
    anchor_df = pd.DataFrame(
        [
            {"ticker": "VNM", "anchor_fair_value": 120.0, "anchor_currency": "VND"},
        ]
    )
    return market_df, fx_df, input_cost_df, anchor_df


def test_run_daily_valuation_returns_one_row():
    as_of = "2026-01-15"
    market_df, fx_df, input_cost_df, anchor_df = _base_inputs(as_of)
    out = run_daily_valuation(as_of, market_df, fx_df, input_cost_df, anchor_df)

    assert isinstance(out, pd.DataFrame)
    assert len(out) == 1
    assert out.loc[0, "ticker"] == "VNM"
    assert out.loc[0, "as_of_date"] == as_of


def test_outputs_include_core_fields_and_math_is_consistent():
    as_of = "2026-01-15"
    market_df, fx_df, input_cost_df, anchor_df = _base_inputs(as_of)
    out = run_daily_valuation(as_of, market_df, fx_df, input_cost_df, anchor_df)

    for col in [
        "close",
        "anchor_fair_value",
        "adjustment_pct",
        "final_fair_value",
        "upside_downside_pct",
        "anchor_used",
        "anchor_status",
        "anchor_error_message",
        "valuation_mode",
    ]:
        assert col in out.columns

    assert bool(out.loc[0, "anchor_used"]) is True
    assert out.loc[0, "anchor_status"] == "used"
    assert out.loc[0, "anchor_error_message"] == ""
    assert out.loc[0, "valuation_mode"] == "anchor_adjusted"

    close = float(out.loc[0, "close"])
    anchor = float(out.loc[0, "anchor_fair_value"])
    adj = float(out.loc[0, "adjustment_pct"])
    fair = float(out.loc[0, "final_fair_value"])
    upside = float(out.loc[0, "upside_downside_pct"])

    assert fair == anchor * (1.0 + adj)
    assert upside == fair / close - 1.0


def test_stale_anchor_fallback_metadata():
    as_of = "2026-01-15"
    market_df, fx_df, input_cost_df, _ = _base_inputs(as_of)
    anchor_df = pd.DataFrame(
        [
            {
                "ticker": "VNM",
                "anchor_fair_value": 120.0,
                "anchor_currency": "VND",
                "valuation_date": "2015-06-01",
            },
        ]
    )
    out = run_daily_valuation(as_of, market_df, fx_df, input_cost_df, anchor_df)
    assert bool(out.loc[0, "anchor_used"]) is False
    assert out.loc[0, "anchor_status"] == "stale"
    assert "Anchor is stale:" in str(out.loc[0, "anchor_error_message"])
    assert out.loc[0, "valuation_mode"] == "market_fallback"
    assert float(out.loc[0, "relative_valuation_signal"]) == 0.0
    assert float(out.loc[0, "sentiment_signal"]) == 0.0
    c = float(out.loc[0, "close"])
    adj = float(out.loc[0, "adjustment_pct"])
    assert float(out.loc[0, "final_fair_value"]) == c * (1.0 + adj)


def test_missing_anchor_on_or_before_fallback_metadata():
    as_of = "2026-01-15"
    market_df, fx_df, input_cost_df, _ = _base_inputs(as_of)
    anchor_df = pd.DataFrame(
        [
            {
                "ticker": "VNM",
                "anchor_fair_value": 120.0,
                "anchor_currency": "VND",
                "valuation_date": "2030-01-01",
            },
        ]
    )
    out = run_daily_valuation(as_of, market_df, fx_df, input_cost_df, anchor_df)
    assert bool(out.loc[0, "anchor_used"]) is False
    assert out.loc[0, "anchor_status"] == "missing"
    assert "on or before" in str(out.loc[0, "anchor_error_message"])
    assert out.loc[0, "valuation_mode"] == "market_fallback"
    assert float(out.loc[0, "relative_valuation_signal"]) == 0.0


def test_validated_anchor_explicit_true_overrides_placeholder_text():
    as_of = "2026-01-15"
    market_df, fx_df, input_cost_df, _ = _base_inputs(as_of)
    anchor_df = pd.DataFrame(
        [
            {
                "ticker": "VNM",
                "anchor_fair_value": 120.0,
                "anchor_currency": "VND",
                "valuation_date": as_of,
                "anchor_validated": True,
                "notes": "contains placeholder word but signed off",
                "source": "model",
            },
        ]
    )
    out = run_daily_valuation(as_of, market_df, fx_df, input_cost_df, anchor_df)
    assert bool(out.loc[0, "anchor_used"]) is True
    assert out.loc[0, "anchor_status"] == "used"
    assert out.loc[0, "valuation_mode"] == "anchor_adjusted"


def test_anchor_validated_false_market_fallback():
    as_of = "2026-01-15"
    market_df, fx_df, input_cost_df, _ = _base_inputs(as_of)
    anchor_df = pd.DataFrame(
        [
            {
                "ticker": "VNM",
                "anchor_fair_value": 120.0,
                "anchor_currency": "VND",
                "valuation_date": as_of,
                "anchor_validated": False,
                "notes": "approved numbers",
                "source": "workbook",
            },
        ]
    )
    out = run_daily_valuation(as_of, market_df, fx_df, input_cost_df, anchor_df)
    assert bool(out.loc[0, "anchor_used"]) is False
    assert out.loc[0, "anchor_status"] == "unvalidated"
    assert out.loc[0, "valuation_mode"] == "market_fallback"


def test_unvalidated_placeholder_in_notes_market_fallback():
    as_of = "2026-01-15"
    market_df, fx_df, input_cost_df, _ = _base_inputs(as_of)
    anchor_df = pd.DataFrame(
        [
            {
                "ticker": "VNM",
                "anchor_fair_value": 120.0,
                "anchor_currency": "VND",
                "valuation_date": as_of,
                "notes": "placeholder series — not for production",
                "source": "draft",
            },
        ]
    )
    out = run_daily_valuation(as_of, market_df, fx_df, input_cost_df, anchor_df)
    assert bool(out.loc[0, "anchor_used"]) is False
    assert out.loc[0, "anchor_status"] == "unvalidated"
    assert "not validated for anchor_adjusted" in str(out.loc[0, "anchor_error_message"]).lower()
    assert out.loc[0, "valuation_mode"] == "market_fallback"
    c = float(out.loc[0, "close"])
    adj = float(out.loc[0, "adjustment_pct"])
    assert float(out.loc[0, "final_fair_value"]) == c * (1.0 + adj)


def test_invalid_anchor_fair_value_fallback_metadata():
    as_of = "2026-01-15"
    market_df, fx_df, input_cost_df, _ = _base_inputs(as_of)
    anchor_df = pd.DataFrame(
        [
            {"ticker": "VNM", "anchor_fair_value": 0.0, "anchor_currency": "VND"},
        ]
    )
    out = run_daily_valuation(as_of, market_df, fx_df, input_cost_df, anchor_df)
    assert bool(out.loc[0, "anchor_used"]) is False
    assert out.loc[0, "anchor_status"] == "invalid"
    assert "anchor_fair_value is invalid" in str(out.loc[0, "anchor_error_message"])
    assert out.loc[0, "valuation_mode"] == "market_fallback"


def test_sentiment_score_is_used_if_present():
    as_of = "2026-01-15"
    market_df, fx_df, input_cost_df, anchor_df = _base_inputs(as_of)
    anchor_df = anchor_df.assign(sentiment_score=1.0)

    out = run_daily_valuation(as_of, market_df, fx_df, input_cost_df, anchor_df)
    assert "sentiment_signal" in out.columns
    assert float(out.loc[0, "sentiment_signal"]) == 1.0


def test_missing_required_columns_raises_clear_error():
    as_of = "2026-01-15"
    market_df, fx_df, input_cost_df, anchor_df = _base_inputs(as_of)
    market_df = market_df.drop(columns=["close"])

    try:
        run_daily_valuation(as_of, market_df, fx_df, input_cost_df, anchor_df)
        assert False, "Expected an exception for missing columns"
    except ValueError as e:
        assert "market_df is missing required columns" in str(e)

