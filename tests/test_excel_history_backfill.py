"""Tests for historical Excel backfill (temp files, mocks)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from vnm_valuation.excel_history_backfill import backfill_vnm_history_to_excel, sort_daily_valuation_sheet
from vnm_valuation.excel_daily_export import read_excel_workbook


def _fake_valuation_row(as_of: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "as_of_date": as_of,
                "ticker": "VNM",
                "close": 61000.0,
                "currency": "VND",
                "valuation_mode": "anchor_adjusted",
                "anchor_used": True,
                "anchor_status": "used",
                "anchor_error_message": "",
                "final_fair_value": 70000.0,
                "relative_valuation_signal": 0.0,
                "sentiment_signal": 0.0,
                "adjustment_pct": 0.0,
            }
        ]
    )


def test_backfill_writes_multiple_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "book.xlsx"
    dates = ["2026-04-14", "2026-04-15", "2026-04-16"]
    it = iter(dates)

    def _run(as_of, *a, **k):
        return _fake_valuation_row(next(it))

    monkeypatch.setattr(
        "vnm_valuation.excel_history_backfill.latest_anchor_valuation_date_on_or_before",
        lambda *a, **k: "2026-03-31",
    )

    stats = backfill_vnm_history_to_excel(
        start_date=dates[0],
        end_date=dates[-1],
        market_df=MagicMock(),
        fx_df=MagicMock(),
        input_cost_df=MagicMock(),
        anchor_df=MagicMock(),
        excel_path=p,
        worksheet="daily",
        dry_run=False,
        limit=None,
        run_valuation=_run,
    )
    assert stats.valuation_ok == 3
    assert stats.excel_bootstrap >= 1
    assert p.is_file()
    book = read_excel_workbook(p)
    assert len(book["daily"]) == 3


def test_rerun_backfill_updates_not_duplicates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "book.xlsx"

    def _run(as_of, *a, **k):
        return _fake_valuation_row(as_of)

    monkeypatch.setattr(
        "vnm_valuation.excel_history_backfill.latest_anchor_valuation_date_on_or_before",
        lambda *a, **k: "2026-03-31",
    )

    backfill_vnm_history_to_excel(
        start_date="2026-04-16",
        end_date="2026-04-16",
        market_df=MagicMock(),
        fx_df=MagicMock(),
        input_cost_df=MagicMock(),
        anchor_df=MagicMock(),
        excel_path=p,
        worksheet="daily",
        dry_run=False,
        limit=None,
        run_valuation=_run,
    )
    stats2 = backfill_vnm_history_to_excel(
        start_date="2026-04-16",
        end_date="2026-04-16",
        market_df=MagicMock(),
        fx_df=MagicMock(),
        input_cost_df=MagicMock(),
        anchor_df=MagicMock(),
        excel_path=p,
        worksheet="daily",
        dry_run=False,
        limit=None,
        run_valuation=_run,
    )
    assert stats2.excel_update >= 1
    assert len(read_excel_workbook(p)["daily"]) == 1


def test_preserves_other_sheet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "book.xlsx"
    with pd.ExcelWriter(p, engine="openpyxl") as w:
        pd.DataFrame([{"note": "keep"}]).to_excel(w, sheet_name="meta", index=False)

    monkeypatch.setattr(
        "vnm_valuation.excel_history_backfill.latest_anchor_valuation_date_on_or_before",
        lambda *a, **k: "2026-03-31",
    )

    backfill_vnm_history_to_excel(
        start_date="2026-04-16",
        end_date="2026-04-16",
        market_df=MagicMock(),
        fx_df=MagicMock(),
        input_cost_df=MagicMock(),
        anchor_df=MagicMock(),
        excel_path=p,
        worksheet="daily",
        dry_run=False,
        limit=None,
        run_valuation=lambda as_of, *a, **k: _fake_valuation_row(as_of),
    )
    xl = pd.ExcelFile(p, engine="openpyxl")
    assert "meta" in xl.sheet_names
    assert "daily" in xl.sheet_names


def test_dry_run_does_not_create_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "missing.xlsx"
    monkeypatch.setattr(
        "vnm_valuation.excel_history_backfill.latest_anchor_valuation_date_on_or_before",
        lambda *a, **k: "2026-03-31",
    )
    stats = backfill_vnm_history_to_excel(
        start_date="2026-04-16",
        end_date="2026-04-17",
        market_df=MagicMock(),
        fx_df=MagicMock(),
        input_cost_df=MagicMock(),
        anchor_df=MagicMock(),
        excel_path=p,
        worksheet="daily",
        dry_run=True,
        limit=None,
        run_valuation=lambda as_of, *a, **k: _fake_valuation_row(as_of),
    )
    assert not p.exists()
    assert stats.valuation_ok == 2
    assert stats.dry_run_would_bootstrap + stats.dry_run_would_append + stats.dry_run_would_update == 2


def test_sort_daily_sheet(tmp_path: Path) -> None:
    p = tmp_path / "s.xlsx"
    df = pd.DataFrame(
        [
            {"as_of_date": "2026-04-17", "ticker": "VNM", "close": 1.0},
            {"as_of_date": "2026-04-16", "ticker": "VNM", "close": 2.0},
        ]
    )
    with pd.ExcelWriter(p, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="daily", index=False)
    sort_daily_valuation_sheet(p, "daily")
    out = read_excel_workbook(p)["daily"]
    assert list(out["as_of_date"].astype(str))[:2] == ["2026-04-16", "2026-04-17"]
