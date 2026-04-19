from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml


def _repo_root() -> Path:
    # project_root / src / vnm_valuation / config.py
    return Path(__file__).resolve().parents[2]


def _as_path(value: str | Path, *, base_dir: Path) -> Path:
    p = value if isinstance(value, Path) else Path(value)
    if not p.is_absolute():
        p = base_dir / p
    return p


def _require_non_empty_str(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing/invalid required field: {field_name}")
    return value.strip()


def _require_bool(value: Any, *, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"Missing/invalid required field: {field_name} (must be boolean)")


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a mapping/dict: {path}")
    return data


# --- Pydantic implementation (preferred if installed) ---
try:  # pragma: no cover
    from pydantic import BaseModel, Field

    try:  # Pydantic v2
        from pydantic import ConfigDict, field_validator, model_validator

        _PydanticV2 = True
    except Exception:  # Pydantic v1 fallback
        from pydantic import validator as field_validator  # type: ignore

        model_validator = None  # type: ignore
        ConfigDict = None  # type: ignore
        _PydanticV2 = False

    class ProjectConfig(BaseModel):
        ticker: str

        if _PydanticV2:
            model_config = ConfigDict(extra="allow")  # type: ignore[misc]

        @field_validator("ticker")  # type: ignore[misc]
        def _validate_ticker(cls, v: str) -> str:
            return _require_non_empty_str(v, field_name="project.ticker").upper()

    class PathsConfig(BaseModel):
        raw_dir: Path
        processed_dir: Path
        output_dir: Path

        if _PydanticV2:
            model_config = ConfigDict(extra="allow")  # type: ignore[misc]

        @field_validator("raw_dir", "processed_dir", "output_dir", mode="before")  # type: ignore[misc]
        def _to_path(cls, v: Any) -> Any:
            return v

    class ValuationConfig(BaseModel):
        # Intentionally minimal for MVP; extend as needed.
        base_currency: str = "VND"

        if _PydanticV2:
            model_config = ConfigDict(extra="allow")  # type: ignore[misc]

        @field_validator("base_currency")  # type: ignore[misc]
        def _validate_base_currency(cls, v: str) -> str:
            return _require_non_empty_str(v, field_name="valuation.base_currency").upper()

    class GoogleSheetsConfig(BaseModel):
        enabled: bool = False
        spreadsheet_id: str = ""
        worksheet_name: str = "daily_valuation"
        credentials_json_path: str = ""

        if _PydanticV2:
            model_config = ConfigDict(extra="allow")  # type: ignore[misc]

        @field_validator("enabled")  # type: ignore[misc]
        def _validate_enabled(cls, v: Any) -> bool:
            if isinstance(v, bool):
                return v
            raise ValueError("google_sheets.enabled must be boolean")

    class AppConfig(BaseModel):
        project: ProjectConfig
        ticker: str
        paths: PathsConfig
        valuation: ValuationConfig = Field(default_factory=ValuationConfig)
        google_sheets: GoogleSheetsConfig = Field(default_factory=GoogleSheetsConfig)

        if _PydanticV2:
            model_config = ConfigDict(extra="allow")  # type: ignore[misc]

        if _PydanticV2:

            @model_validator(mode="before")  # type: ignore[misc]
            def _preprocess(cls, data: Any) -> Any:
                if not isinstance(data, dict):
                    raise ValueError("Config must be a mapping/dict")
                project = data.get("project") or {}
                if isinstance(project, dict) and "ticker" in project and "ticker" not in data:
                    data["ticker"] = project.get("ticker")
                return data

            @model_validator(mode="after")  # type: ignore[misc]
            def _finalize(self) -> "AppConfig":
                root = _repo_root()
                self.paths.raw_dir = _as_path(self.paths.raw_dir, base_dir=root)
                self.paths.processed_dir = _as_path(self.paths.processed_dir, base_dir=root)
                self.paths.output_dir = _as_path(self.paths.output_dir, base_dir=root)
                self.ticker = _require_non_empty_str(self.ticker, field_name="ticker").upper()
                return self

        else:

            @field_validator("ticker", pre=True)  # type: ignore[misc]
            def _v1_ticker(cls, v: Any) -> str:
                return _require_non_empty_str(v, field_name="ticker").upper()

            @field_validator("paths", pre=False)  # type: ignore[misc]
            def _v1_paths(cls, v: PathsConfig) -> PathsConfig:
                root = _repo_root()
                v.raw_dir = _as_path(v.raw_dir, base_dir=root)
                v.processed_dir = _as_path(v.processed_dir, base_dir=root)
                v.output_dir = _as_path(v.output_dir, base_dir=root)
                return v

            @field_validator("project", pre=False)  # type: ignore[misc]
            def _v1_project(cls, v: ProjectConfig) -> ProjectConfig:
                _require_non_empty_str(v.ticker, field_name="project.ticker")
                return v

    def load_config(config_path: Optional[Path] = None) -> AppConfig:
        """
        Load and validate application configuration.

        - Default path: repo_root/config/config.yaml
        - Relative paths inside config are resolved against repo root.
        """
        path = config_path or (_repo_root() / "config" / "config.yaml")
        raw = _load_yaml(path)
        if "project" not in raw or not isinstance(raw.get("project"), dict):
            raise ValueError("Missing required section: project")
        if "paths" not in raw or not isinstance(raw.get("paths"), dict):
            raise ValueError("Missing required section: paths")
        # Mirror project.ticker into top-level ticker if user only set project.ticker.
        raw.setdefault("ticker", (raw.get("project") or {}).get("ticker"))
        return AppConfig.model_validate(raw) if _PydanticV2 else AppConfig.parse_obj(raw)  # type: ignore[attr-defined]

except Exception:  # pragma: no cover
    # --- Dataclasses fallback (no Pydantic installed) ---

    @dataclass(frozen=True)
    class ProjectConfig:
        ticker: str

    @dataclass(frozen=True)
    class PathsConfig:
        raw_dir: Path
        processed_dir: Path
        output_dir: Path

    @dataclass(frozen=True)
    class ValuationConfig:
        base_currency: str = "VND"

    @dataclass(frozen=True)
    class GoogleSheetsConfig:
        enabled: bool = False
        spreadsheet_id: str = ""
        worksheet_name: str = "daily_valuation"
        credentials_json_path: str = ""

    @dataclass(frozen=True)
    class AppConfig:
        project: ProjectConfig
        ticker: str
        paths: PathsConfig
        valuation: ValuationConfig
        google_sheets: GoogleSheetsConfig

    def load_config(config_path: Optional[Path] = None) -> AppConfig:
        """
        Load and validate application configuration.

        - Default path: repo_root/config/config.yaml
        - Relative paths inside config are resolved against repo root.
        """
        root = _repo_root()
        path = config_path or (root / "config" / "config.yaml")
        raw = _load_yaml(path)

        project_raw = raw.get("project")
        if not isinstance(project_raw, dict):
            raise ValueError("Missing required section: project")
        paths_raw = raw.get("paths")
        if not isinstance(paths_raw, dict):
            raise ValueError("Missing required section: paths")

        project_ticker = _require_non_empty_str(project_raw.get("ticker"), field_name="project.ticker").upper()
        ticker = _require_non_empty_str(raw.get("ticker", project_ticker), field_name="ticker").upper()

        raw_dir = _as_path(_require_non_empty_str(paths_raw.get("raw_dir"), field_name="paths.raw_dir"), base_dir=root)
        processed_dir = _as_path(
            _require_non_empty_str(paths_raw.get("processed_dir"), field_name="paths.processed_dir"),
            base_dir=root,
        )
        output_dir = _as_path(
            _require_non_empty_str(paths_raw.get("output_dir"), field_name="paths.output_dir"), base_dir=root
        )

        valuation_raw = raw.get("valuation") or {}
        if not isinstance(valuation_raw, dict):
            raise ValueError("valuation must be a mapping/dict if provided")
        base_currency = valuation_raw.get("base_currency", "VND")
        base_currency = _require_non_empty_str(base_currency, field_name="valuation.base_currency").upper()

        gs_raw = raw.get("google_sheets") or {}
        if not isinstance(gs_raw, dict):
            raise ValueError("google_sheets must be a mapping/dict if provided")
        enabled = gs_raw.get("enabled", False)
        enabled = _require_bool(enabled, field_name="google_sheets.enabled")

        google_sheets = GoogleSheetsConfig(
            enabled=enabled,
            spreadsheet_id=str(gs_raw.get("spreadsheet_id", "") or ""),
            worksheet_name=str(gs_raw.get("worksheet_name", "daily_valuation") or "daily_valuation"),
            credentials_json_path=str(gs_raw.get("credentials_json_path", "") or ""),
        )

        return AppConfig(
            project=ProjectConfig(ticker=project_ticker),
            ticker=ticker,
            paths=PathsConfig(raw_dir=raw_dir, processed_dir=processed_dir, output_dir=output_dir),
            valuation=ValuationConfig(base_currency=base_currency),
            google_sheets=google_sheets,
        )

