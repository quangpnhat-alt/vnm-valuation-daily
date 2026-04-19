"""Offline tests for Excel daily export (temp .xlsx files only)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from vnm_valuation.excel_daily_export import (
    compute_excel_dry_run_plan,
    load_worksheet_str_grid,
    repo_default_excel_path,
    upsert_valuation_row_excel,
)
from vnm_valuation.google_sheets_publish import PUBLISH_COLUMNS


def _row(as_of: str = "2026-04-16", ticker: str = "VNM", **kwargs: str) -> dict[str, str]:
    d = {k: "" for k in PUBLISH_COLUMNS}
    d.update(
        {
            "as_of_date": as_of,
            "ticker": ticker,
            "close": "61300",
            "valuation_mode": "anchor_adjusted",
            "anchor_status": "used",
            "selected_anchor_date": "2026-03-31",
            "anchor_used": "TRUE",
            "anchor_error_message": "",
            "final_fair_value": "70000",
            "relative_valuation_signal": "0.1",
            "sentiment_signal": "0",
            "adjustment_pct": "0.02",
            "published_at_utc": "2026-04-19T12:00:00Z",
        }
    )
    d.update(kwargs)
    return d


def test_bootstrap_new_workbook(tmp_path: Path) -> None:
    p = tmp_path / "out.xlsx"
    row = _row()
    token, plan = upsert_valuation_row_excel(p, "daily", row)
    assert token == "bootstrap"
    assert plan.action == "bootstrap"
    assert p.is_file()
    g = load_worksheet_str_grid(p, "daily")
    assert g is not None
    assert len(g) == 2
    assert g[0][:3] == ["as_of_date", "ticker", "close"]


def test_append_second_date(tmp_path: Path) -> None:
    p = tmp_path / "out.xlsx"
    upsert_valuation_row_excel(p, "daily", _row(as_of="2026-04-16"))
    token, _ = upsert_valuation_row_excel(p, "daily", _row(as_of="2026-04-17", final_fair_value="71000"))
    assert token == "append"
    g = load_worksheet_str_grid(p, "daily")
    assert g is not None
    assert len(g) == 3
    assert g[2][0] == "2026-04-17"


def test_update_same_as_of_and_ticker(tmp_path: Path) -> None:
    p = tmp_path / "out.xlsx"
    upsert_valuation_row_excel(p, "daily", _row(final_fair_value="70000"))
    token, _ = upsert_valuation_row_excel(p, "daily", _row(final_fair_value="99999"))
    assert token == "update"
    g = load_worksheet_str_grid(p, "daily")
    assert g is not None
    assert len(g) == 2
    ffv_idx = g[0].index("final_fair_value")
    assert float(g[1][ffv_idx]) == pytest.approx(99999.0)


def test_preserves_other_worksheet(tmp_path: Path) -> None:
    p = tmp_path / "book.xlsx"
    with pd.ExcelWriter(p, engine="openpyxl") as w:
        pd.DataFrame([{"x": 1}]).to_excel(w, sheet_name="meta", index=False)
    token, _ = upsert_valuation_row_excel(p, "daily", _row())
    assert token == "bootstrap"
    xl = pd.ExcelFile(p, engine="openpyxl")
    assert "meta" in xl.sheet_names
    assert "daily" in xl.sheet_names


def test_dry_run_plan(tmp_path: Path) -> None:
    p = tmp_path / "nope.xlsx"
    plan = compute_excel_dry_run_plan(p, "daily", _row())
    assert plan.action == "bootstrap"
    assert not p.exists()


def test_repo_default_excel_path() -> None:
    r = Path("/repo")
    assert repo_default_excel_path(r) == Path("/repo/output/vnm_daily_valuation.xlsx")
