"""
Daily valuation export orchestration (Excel + optional Google Sheets). Does not run valuation logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class DailyExportOptions:
    """Options for one post-valuation export batch."""

    do_excel: bool = True
    excel_path: Path | None = None
    excel_worksheet: str = "daily_valuation"
    to_gsheet: bool = False
    gsheet_worksheet: str | None = None
    dry_run: bool = False
    best_effort_gsheet: bool = False


@dataclass
class DailyExportResult:
    excel_status: str = "skipped"
    gsheet_status: str = "skipped"
    gsheet_error: str | None = None
    warnings: list[str] = field(default_factory=list)


def run_daily_exports(row_dict: dict[str, str], opt: DailyExportOptions) -> DailyExportResult:
    """
    Run Excel and/or Google Sheets exports from a pre-built publish row dict.

    - `dry_run`: no filesystem writes, no Google API calls.
    - `best_effort_gsheet`: if Google Sheets fails after Excel succeeded, record error and do not re-raise.
    """
    from vnm_valuation.excel_daily_export import (
        compute_excel_dry_run_plan,
        upsert_valuation_row_excel,
    )
    from vnm_valuation.google_sheets_publish import (
        load_publish_settings_from_env,
        open_worksheet,
        publish_valuation_row_to_sheet,
    )

    out = DailyExportResult()
    excel_path = Path(opt.excel_path) if opt.excel_path is not None else None

    if opt.dry_run:
        if opt.do_excel and excel_path is not None:
            plan = compute_excel_dry_run_plan(excel_path, opt.excel_worksheet, row_dict)
            out.excel_status = f"dry_run:{plan.action}"
        elif opt.do_excel:
            out.excel_status = "skipped(no_path)"
        else:
            out.excel_status = "skipped"

        if opt.to_gsheet:
            try:
                load_publish_settings_from_env(worksheet_override=opt.gsheet_worksheet)
                out.gsheet_status = "dry_run:pending(no_api)"
            except ValueError as e:
                out.gsheet_status = "config-missing"
                out.gsheet_error = str(e)
        else:
            out.gsheet_status = "skipped"
        return out

    if opt.do_excel:
        if excel_path is None:
            raise ValueError("excel_path is required when do_excel is True and not dry_run")
        token, _ = upsert_valuation_row_excel(excel_path, opt.excel_worksheet, row_dict)
        out.excel_status = token
    else:
        out.excel_status = "skipped"

    if not opt.to_gsheet:
        out.gsheet_status = "skipped"
        return out

    try:
        cred_path, spreadsheet_id, ws_title, _suffix = load_publish_settings_from_env(
            worksheet_override=opt.gsheet_worksheet,
        )
    except ValueError as e:
        out.gsheet_error = str(e)
        if opt.best_effort_gsheet:
            out.gsheet_status = "config-missing"
            out.warnings.append(f"Google Sheets skipped (best-effort): {e}")
            return out
        raise

    try:
        worksheet = open_worksheet(cred_path, spreadsheet_id, ws_title)
        token, _plan = publish_valuation_row_to_sheet(worksheet, row_dict)
        out.gsheet_status = token
    except Exception as e:
        out.gsheet_error = str(e)
        if opt.best_effort_gsheet:
            out.gsheet_status = "failed"
            out.warnings.append(f"Google Sheets export failed (best-effort): {e}")
        else:
            raise
    return out
