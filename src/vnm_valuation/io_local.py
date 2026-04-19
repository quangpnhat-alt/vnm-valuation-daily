from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def ensure_dir(dir_path: str | Path) -> Path:
    """
    Ensure a directory exists (create parents as needed).
    Returns the resolved Path.
    """
    p = Path(dir_path).expanduser()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _as_path(path: str | Path) -> Path:
    return Path(path).expanduser()


def _require_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if path.is_dir():
        raise IsADirectoryError(f"Expected a file path, got directory: {path}")


def read_csv(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    p = _as_path(path)
    _require_exists(p)
    return pd.read_csv(p, **kwargs)


def write_csv(df: pd.DataFrame, path: str | Path, **kwargs: Any) -> Path:
    p = _as_path(path)
    ensure_dir(p.parent)
    if "index" not in kwargs:
        kwargs["index"] = False
    df.to_csv(p, **kwargs)
    return p


def read_parquet(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    p = _as_path(path)
    _require_exists(p)
    return pd.read_parquet(p, **kwargs)


def write_parquet(df: pd.DataFrame, path: str | Path, **kwargs: Any) -> Path:
    p = _as_path(path)
    ensure_dir(p.parent)
    df.to_parquet(p, **kwargs)
    return p

