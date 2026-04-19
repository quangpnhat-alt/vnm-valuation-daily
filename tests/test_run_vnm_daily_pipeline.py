"""Tests for daily export orchestration (offline; no Google API)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vnm_valuation.daily_pipeline import DailyExportOptions, run_daily_exports
from vnm_valuation.google_sheets_publish import PUBLISH_COLUMNS


def _row_dict() -> dict[str, str]:
    d = {k: "" for k in PUBLISH_COLUMNS}
    d.update(
        {
            "as_of_date": "2026-04-16",
            "ticker": "VNM",
            "close": "61300",
            "valuation_mode": "anchor_adjusted",
            "anchor_status": "used",
            "selected_anchor_date": "2026-03-31",
            "anchor_used": "TRUE",
            "anchor_error_message": "",
            "final_fair_value": "70000",
            "relative_valuation_signal": "0",
            "sentiment_signal": "0",
            "adjustment_pct": "0",
            "published_at_utc": "2026-01-01T00:00:00Z",
        }
    )
    return d


def test_dry_run_does_not_call_upsert_or_gsheet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"upsert": 0}

    def _no_upsert(*a, **k):
        called["upsert"] += 1
        raise AssertionError("should not write in dry_run")

    monkeypatch.setattr(
        "vnm_valuation.excel_daily_export.upsert_valuation_row_excel",
        _no_upsert,
    )

    p = tmp_path / "book.xlsx"
    r = run_daily_exports(
        _row_dict(),
        DailyExportOptions(
            do_excel=True,
            excel_path=p,
            excel_worksheet="daily",
            to_gsheet=True,
            dry_run=True,
        ),
    )
    assert r.excel_status.startswith("dry_run:")
    assert "dry_run" in r.gsheet_status or r.gsheet_status == "config-missing"
    assert called["upsert"] == 0


def test_excel_only_writes_once(tmp_path: Path) -> None:
    p = tmp_path / "out.xlsx"
    r = run_daily_exports(
        _row_dict(),
        DailyExportOptions(
            do_excel=True,
            excel_path=p,
            excel_worksheet="daily",
            to_gsheet=False,
            dry_run=False,
        ),
    )
    assert r.excel_status == "bootstrap"
    assert r.gsheet_status == "skipped"
    assert p.is_file()


def test_excel_and_gsheet_calls_both(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    p = tmp_path / "out.xlsx"
    calls = {"sheet": 0}

    monkeypatch.setattr(
        "vnm_valuation.excel_daily_export.upsert_valuation_row_excel",
        lambda *a, **k: ("bootstrap", MagicMock()),
    )

    def _fake_publish(ws, row_dict):
        calls["sheet"] += 1
        return "append", MagicMock()

    monkeypatch.setattr(
        "vnm_valuation.google_sheets_publish.publish_valuation_row_to_sheet",
        _fake_publish,
    )
    monkeypatch.setattr(
        "vnm_valuation.google_sheets_publish.open_worksheet",
        lambda *a, **k: MagicMock(),
    )
    monkeypatch.setattr(
        "vnm_valuation.google_sheets_publish.load_publish_settings_from_env",
        lambda **k: ("/fake.json", "abc123spreadsheetid", "daily", "etid"),
    )

    r = run_daily_exports(
        _row_dict(),
        DailyExportOptions(
            do_excel=True,
            excel_path=p,
            excel_worksheet="daily",
            to_gsheet=True,
            dry_run=False,
        ),
    )
    assert r.excel_status == "bootstrap"
    assert calls["sheet"] == 1
    assert r.gsheet_status == "append"


def test_best_effort_gsheet_continues_after_publish_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    p = tmp_path / "out.xlsx"
    monkeypatch.setattr(
        "vnm_valuation.excel_daily_export.upsert_valuation_row_excel",
        lambda *a, **k: ("ok", MagicMock()),
    )
    monkeypatch.setattr(
        "vnm_valuation.google_sheets_publish.load_publish_settings_from_env",
        lambda **k: ("/fake.json", "id", "daily", "suf"),
    )
    monkeypatch.setattr("vnm_valuation.google_sheets_publish.open_worksheet", lambda *a, **k: MagicMock())
    monkeypatch.setattr(
        "vnm_valuation.google_sheets_publish.publish_valuation_row_to_sheet",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("api down")),
    )

    r = run_daily_exports(
        _row_dict(),
        DailyExportOptions(
            do_excel=True,
            excel_path=p,
            to_gsheet=True,
            dry_run=False,
            best_effort_gsheet=True,
        ),
    )
    assert r.excel_status == "ok"
    assert r.gsheet_status == "failed"
    assert r.warnings
    assert "best-effort" in r.warnings[0].lower()


def test_best_effort_missing_gsheet_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    p = tmp_path / "out.xlsx"
    monkeypatch.setattr(
        "vnm_valuation.excel_daily_export.upsert_valuation_row_excel",
        lambda *a, **k: ("ok", MagicMock()),
    )
    monkeypatch.setattr(
        "vnm_valuation.google_sheets_publish.load_publish_settings_from_env",
        lambda **k: (_ for _ in ()).throw(ValueError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")),
    )

    r = run_daily_exports(
        _row_dict(),
        DailyExportOptions(
            do_excel=True,
            excel_path=p,
            to_gsheet=True,
            dry_run=False,
            best_effort_gsheet=True,
        ),
    )
    assert r.excel_status == "ok"
    assert r.gsheet_status == "config-missing"
