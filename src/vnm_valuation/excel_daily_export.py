"""
Local Excel (.xlsx) export for daily VNM valuation rows (parallel to Google Sheets; no valuation logic).

Idempotent by (as_of_date, ticker): updates an existing row or appends.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from vnm_valuation.google_sheets_publish import (
    PublishPlan,
    build_publish_row_dict,
    compute_publish_plan,
)

DEFAULT_EXCEL_FILENAME = "vnm_daily_valuation.xlsx"
DEFAULT_WORKSHEET_NAME = "daily_valuation"


def repo_default_excel_path(repo_root: Path) -> Path:
    return Path(repo_root) / "output" / DEFAULT_EXCEL_FILENAME


def load_worksheet_str_grid(path: Path, sheet_name: str) -> list[list[str]] | None:
    """Return [header, ...rows] as strings, or None if file or sheet is missing."""
    if not path.is_file():
        return None
    with pd.ExcelFile(path, engine="openpyxl") as xl:
        if sheet_name not in xl.sheet_names:
            return None
        df = pd.read_excel(path, sheet_name=sheet_name, header=0, dtype=object)
    if df.shape[1] == 0:
        return None
    headers = [str(c) for c in df.columns]
    idx_as = headers.index("as_of_date") if "as_of_date" in headers else -1
    out: list[list[str]] = [headers]
    for _, row in df.iterrows():
        cells: list[str] = []
        for j, v in enumerate(row):
            if pd.isna(v):
                cells.append("")
            elif j == idx_as:
                ts = pd.to_datetime(v, errors="coerce")
                cells.append("" if pd.isna(ts) else ts.normalize().date().isoformat())
            elif isinstance(v, bool):
                cells.append("TRUE" if v else "FALSE")
            else:
                cells.append(str(v).strip())
        out.append(cells)
    return out


def row_dict_to_export_series(row_dict: dict[str, str], header: list[str]) -> pd.Series:
    """Typed values for Excel: bool for anchor_used, float for numerics, str for the rest."""
    data: dict[str, Any] = {}
    for col in header:
        raw = (row_dict.get(col, "") or "").strip()
        if col == "anchor_used":
            data[col] = raw.upper() in ("TRUE", "1", "YES")
        elif col in (
            "close",
            "final_fair_value",
            "relative_valuation_signal",
            "sentiment_signal",
            "adjustment_pct",
        ):
            if raw == "":
                data[col] = pd.NA
            else:
                try:
                    data[col] = float(raw)
                except ValueError:
                    data[col] = raw
        else:
            data[col] = raw
    return pd.Series(data, dtype=object)


def _read_all_sheets(path: Path) -> dict[str, pd.DataFrame]:
    with pd.ExcelFile(path, engine="openpyxl") as xl:
        return {sn: pd.read_excel(path, sheet_name=sn, dtype=object) for sn in xl.sheet_names}


def write_all_sheets(path: Path, book: dict[str, pd.DataFrame]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in book.items():
            df.to_excel(writer, sheet_name=name, index=False)


def upsert_valuation_row_excel(
    path: Path,
    sheet_name: str,
    row_dict: dict[str, str],
) -> tuple[str, PublishPlan]:
    """
    Upsert one row into path/sheet_name. Returns (action_token, plan).

    Preserves other worksheets in the workbook when path already exists.
    """
    path = Path(path)
    grid: list[list[str]] | None = None
    if path.is_file():
        grid = load_worksheet_str_grid(path, sheet_name)

    plan = compute_publish_plan(grid, row_dict)
    ser = row_dict_to_export_series(row_dict, plan.header)

    if not path.is_file():
        write_all_sheets(path, {sheet_name: pd.DataFrame([ser])})
        return "bootstrap", plan

    book = _read_all_sheets(path)

    if sheet_name not in book:
        book[sheet_name] = pd.DataFrame([ser])
        write_all_sheets(path, book)
        return "bootstrap", plan

    df = book[sheet_name]
    if plan.action == "bootstrap":
        book[sheet_name] = pd.DataFrame([ser])
        token = "bootstrap"
    elif plan.action == "update":
        as_target = pd.Timestamp(row_dict.get("as_of_date", "")).normalize()
        as_series = pd.to_datetime(df["as_of_date"], errors="coerce").dt.normalize()
        m = (as_series == as_target) & (df["ticker"].astype(str).str.strip() == row_dict.get("ticker", ""))
        if not m.any():
            raise RuntimeError("upsert: update plan but no matching row (internal inconsistency)")
        i = df.index[m][0]
        for c in plan.header:
            df.loc[i, c] = ser.get(c, pd.NA)
        book[sheet_name] = df
        token = "update"
    else:
        book[sheet_name] = pd.concat([df, pd.DataFrame([ser])], ignore_index=True)
        token = "append"

    write_all_sheets(path, book)
    return token, plan


def compute_excel_dry_run_plan(path: Path, sheet_name: str, row_dict: dict[str, str]) -> PublishPlan:
    """Plan only (no writes): same logic as upsert for existing vs missing file/sheet."""
    grid: list[list[str]] | None = None
    if path.is_file():
        grid = load_worksheet_str_grid(path, sheet_name)
    return compute_publish_plan(grid, row_dict)
