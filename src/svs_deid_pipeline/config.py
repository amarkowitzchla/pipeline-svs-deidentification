from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def _coerce_path(value: str | Path | None) -> Path | None:
    if value is None:
        return None
    return value if isinstance(value, Path) else Path(value)


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("Config YAML must contain a mapping at the top level.")
    return data


def _read_env(env: Mapping[str, str]) -> dict[str, Any]:
    return {
        "manifest": env.get("SVS_DEID_MANIFEST"),
        "out_dir": env.get("SVS_DEID_OUT_DIR"),
        "s3_bucket": env.get("SVS_DEID_S3_BUCKET"),
        "s3_prefix": env.get("SVS_DEID_S3_PREFIX"),
        "s3_region": env.get("SVS_DEID_S3_REGION"),
        "openslide_path": env.get("SVS_DEID_OPENSLIDE_PATH"),
        "log_level": env.get("SVS_DEID_LOG_LEVEL"),
        "workers": env.get("SVS_DEID_WORKERS"),
        "dry_run": _parse_bool(env.get("SVS_DEID_DRY_RUN")),
        "allow_partial": _parse_bool(env.get("SVS_DEID_ALLOW_PARTIAL")),
        "fail_fast": _parse_bool(env.get("SVS_DEID_FAIL_FAST")),
        "resume": _parse_bool(env.get("SVS_DEID_RESUME")),
        "keep_local": _parse_bool(env.get("SVS_DEID_KEEP_LOCAL")),
    }


@dataclass
class PipelineConfig:
    manifest: Path
    out_dir: Path
    s3_bucket: str | None = None
    s3_prefix: str | None = None
    s3_region: str | None = None
    openslide_path: Path | None = None
    log_level: str = "INFO"
    workers: int = 1
    dry_run: bool = False
    allow_partial: bool = False
    fail_fast: bool = False
    resume: bool = False
    keep_local: bool = True
    config_path: Path | None = None

    def validate(self) -> None:
        if not self.manifest:
            raise ValueError("Manifest path is required.")
        if not self.out_dir:
            raise ValueError("Output directory is required.")
        if self.workers < 1:
            raise ValueError("Workers must be >= 1.")


def load_config(
    *,
    manifest: str | Path | None,
    out_dir: str | Path | None,
    config_path: str | Path | None = None,
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
    s3_region: str | None = None,
    openslide_path: str | Path | None = None,
    log_level: str | None = None,
    workers: int | None = None,
    dry_run: bool | None = None,
    allow_partial: bool | None = None,
    fail_fast: bool | None = None,
    resume: bool | None = None,
    keep_local: bool | None = None,
    env: Mapping[str, str] | None = None,
) -> PipelineConfig:
    env_data = _read_env(env or os.environ)
    yaml_data: dict[str, Any] = {}
    config_path_obj = _coerce_path(config_path)
    if config_path_obj:
        yaml_data = _read_yaml(config_path_obj)

    merged: dict[str, Any] = {}
    merged.update({k: v for k, v in env_data.items() if v not in (None, "")})
    merged.update({k: v for k, v in yaml_data.items() if v not in (None, "")})

    cli_values = {
        "manifest": manifest,
        "out_dir": out_dir,
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "s3_region": s3_region,
        "openslide_path": openslide_path,
        "log_level": log_level,
        "workers": workers,
        "dry_run": dry_run,
        "allow_partial": allow_partial,
        "fail_fast": fail_fast,
        "resume": resume,
        "keep_local": keep_local,
    }
    merged.update({k: v for k, v in cli_values.items() if v not in (None, "")})

    keep_local_raw = merged.get("keep_local")
    keep_local_value = True if keep_local_raw is None else bool(keep_local_raw)

    config = PipelineConfig(
        manifest=_coerce_path(merged.get("manifest")),
        out_dir=_coerce_path(merged.get("out_dir")),
        s3_bucket=merged.get("s3_bucket"),
        s3_prefix=merged.get("s3_prefix"),
        s3_region=merged.get("s3_region"),
        openslide_path=_coerce_path(merged.get("openslide_path")),
        log_level=str(merged.get("log_level") or "INFO"),
        workers=int(merged.get("workers") or 1),
        dry_run=bool(merged.get("dry_run") or False),
        allow_partial=bool(merged.get("allow_partial") or False),
        fail_fast=bool(merged.get("fail_fast") or False),
        resume=bool(merged.get("resume") or False),
        keep_local=keep_local_value,
        config_path=config_path_obj,
    )
    config.validate()
    return config


def configure_openslide(openslide_path: Path | None) -> None:
    if os.name != "nt" or not openslide_path:
        return
    os.add_dll_directory(str(openslide_path))
