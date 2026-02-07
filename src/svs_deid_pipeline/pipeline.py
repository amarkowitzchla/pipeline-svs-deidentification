from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from . import __version__
from .config import PipelineConfig
from .deidentification import deidentify_one
from .s3 import upload_file_to_s3
from .submission import generate_metadata_file_record, write_submission_csv
from .utils import md5_checksum

REQUIRED_MANIFEST_COLUMNS = {"location", "rid", "specnum_formatted", "stain", "sample_id"}
EXPECTED_SVS_DEID_REMOTE = "https://github.com/pearcetm/svs-deidentifier"


def configure_logging(out_dir: Path, log_level: str) -> None:
    log_dir = out_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "run.log"
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )


def _check_svs_deidentifier_submodule() -> None:
    repo_root = Path.cwd()
    candidates = [
        repo_root / "svs-deidentifier",
        repo_root / "svs_deid_pipeline" / "svs-deidentifier",
    ]
    submodule_path = next((p for p in candidates if p.exists()), None)
    if not submodule_path:
        raise FileNotFoundError(
            "svs-deidentifier submodule not found. "
            "Expected at ./svs-deidentifier or ./svs_deid_pipeline/svs-deidentifier."
        )
    gitmodules_candidates = [
        repo_root / ".gitmodules",
        repo_root / "svs_deid_pipeline" / ".gitmodules",
    ]
    for gitmodules in gitmodules_candidates:
        if gitmodules.exists():
            contents = gitmodules.read_text(encoding="utf-8")
            if EXPECTED_SVS_DEID_REMOTE not in contents:
                raise ValueError(
                    "svs-deidentifier submodule URL does not match expected upstream."
                )
            break


def read_manifest(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = REQUIRED_MANIFEST_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Manifest missing required columns: {sorted(missing)}")
    df = df[df["location"].notna()].copy()
    manifest_dir = path.parent

    def _resolve_location(value: str) -> str:
        loc = Path(value)
        if loc.is_absolute():
            return str(loc)
        if loc.exists():
            return str(loc.resolve())
        return str((manifest_dir / loc))

    df["location"] = df["location"].astype(str).map(_resolve_location)
    return df


def _stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def destination_basename(row: pd.Series) -> str:
    source_key = f"{row['rid']}|{row['specnum_formatted']}"
    hashed = _stable_hash(source_key)[:16]
    return f"svs_{hashed}.svs"


def build_source_dest_df(df: pd.DataFrame, out_dir: Path) -> pd.DataFrame:
    svs_dir = out_dir / "svs"
    destinations = [str(svs_dir / destination_basename(row)) for _, row in df.iterrows()]
    return pd.DataFrame({"source": df["location"].astype(str), "destination": destinations})


def write_derived_csv(df: pd.DataFrame, out_dir: Path) -> Path:
    derived_dir = out_dir / "derived"
    derived_dir.mkdir(parents=True, exist_ok=True)
    derived_path = derived_dir / "source_destination.csv"
    df.to_csv(derived_path, index=False)
    return derived_path


def write_status_csv(status_rows: list[dict[str, str]], out_dir: Path) -> Path:
    status_dir = out_dir / "status"
    status_dir.mkdir(parents=True, exist_ok=True)
    status_path = status_dir / "status.csv"
    status_df = pd.DataFrame(status_rows)
    status_df.to_csv(status_path, index=False)
    return status_path


def write_run_journal(config: PipelineConfig, out_dir: Path, counts: dict[str, int]) -> Path:
    journal_path = out_dir / "run.json"
    payload = {
        "version": __version__,
        "manifest_name": config.manifest.name,
        "manifest_path_hash": _stable_hash(str(config.manifest)),
        "out_dir": str(out_dir),
        "dry_run": config.dry_run,
        "resume": config.resume,
        "keep_local": config.keep_local,
        "max_files": config.max_files,
        "workers": config.workers,
        "s3_bucket": config.s3_bucket,
        "s3_prefix": config.s3_prefix,
        "counts": counts,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    journal_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return journal_path


def write_s3_manifest(out_dir: Path, manifest_rows: list[dict[str, str]]) -> Path:
    s3_path = out_dir / "s3_manifest.csv"
    pd.DataFrame(manifest_rows).to_csv(s3_path, index=False)
    return s3_path


def _load_existing_status(out_dir: Path) -> dict[str, dict[str, str]]:
    status_path = out_dir / "status" / "status.csv"
    if not status_path.exists():
        return {}
    df = pd.read_csv(status_path)
    return {row["destination"]: row for _, row in df.iterrows()}


def _load_existing_s3_manifest(out_dir: Path) -> list[dict[str, str]]:
    s3_path = out_dir / "s3_manifest.csv"
    if not s3_path.exists():
        return []
    df = pd.read_csv(s3_path)
    return df.to_dict(orient="records")


def run_pipeline(config: PipelineConfig) -> dict[str, Path]:
    out_dir = config.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(out_dir, config.log_level)
    logger = logging.getLogger("svs_deid_pipeline")

    logger.info("Starting pipeline.")
    _check_svs_deidentifier_submodule()
    if not config.keep_local and not config.s3_bucket and not config.dry_run:
        raise ValueError("keep_local=False requires --s3-bucket for offloading outputs.")
    manifest_df = read_manifest(config.manifest)
    source_dest_df = build_source_dest_df(manifest_df, out_dir)

    derived_csv = write_derived_csv(source_dest_df, out_dir)

    if config.dry_run:
        status_csv = write_status_csv(
            [
                {
                    "destination": dest,
                    "source_hash": "",
                    "status": "planned",
                    "error": "",
                    "md5": "",
                    "upload_status": "not_requested",
                    "s3_uri": "",
                    "local_deleted": "no",
                }
                for dest in source_dest_df["destination"]
            ],
            out_dir,
        )
        run_journal = write_run_journal(config, out_dir, {"manifest_rows": len(manifest_df)})
        logger.info("Dry-run pipeline complete.")
        return {
            "derived_csv": derived_csv,
            "status_csv": status_csv,
            "run_journal": run_journal,
        }

    status_rows: list[dict[str, str]] = []
    existing_status = _load_existing_status(out_dir) if config.resume else {}
    s3_manifest_rows = _load_existing_s3_manifest(out_dir) if config.resume else []
    s3_manifest_index = {row.get("local_path"): row for row in s3_manifest_rows}
    submission_records: list[dict[str, str]] = []
    processed_count = 0

    manifest_lookup = {str(row["location"]): row for _, row in manifest_df.iterrows()}

    for _, row in source_dest_df.iterrows():
        source = str(row["source"])
        destination = str(row["destination"])
        source_hash = _stable_hash(source)
        previous = existing_status.get(destination)
        previous_uploaded = (previous is not None) and previous.get("upload_status") == "uploaded"
        local_exists = Path(destination).exists()

        if (
            config.resume
            and config.s3_bucket
            and destination in s3_manifest_index
            and not local_exists
            and not previous
        ):
            # ! I expect this will also throw a dtype error when resuming with s3
            # ! will leave for now
            status_rows.append(
                {
                    "destination": destination,
                    "source_hash": source_hash,
                    "status": "success",
                    "error": "",
                    "md5": "",
                    "upload_status": "uploaded",
                    "s3_uri": s3_manifest_index[destination]["s3_uri"],
                    "local_deleted": "yes",
                }
            )
            continue

        if (previous is not None) and previous.get("status") == "success":
            if config.s3_bucket and previous_uploaded:
                status_rows.append(previous)
                continue
            if not config.s3_bucket and local_exists:
                status_rows.append(previous)
                continue

        if config.max_files is not None and processed_count >= config.max_files:
            continue

        if (previous is not None) and previous.get("status") == "success":
            result = {
                "destination": destination,
                "status": "success",
                "error": previous.get("error", ""),
            }
        else:
            result = deidentify_one(source, destination, fail_fast=config.fail_fast)
            processed_count += 1

        result_destination = result["destination"]

        if result["status"] == "success":
            checksum = md5_checksum(result_destination)
            result["md5"] = checksum or ""
        else:
            result["md5"] = ""

        upload_status = "not_requested"
        s3_uri = ""
        local_deleted = "no"
        if config.s3_bucket and result["status"] == "success":
            key_prefix = config.s3_prefix.strip("/") if config.s3_prefix else ""
            key = f"{key_prefix}/{Path(result_destination).name}" if key_prefix else Path(result_destination).name
            if result_destination in s3_manifest_index:
                s3_uri = s3_manifest_index[result_destination]["s3_uri"]
                upload_status = "uploaded"
            else:
                s3_uri = upload_file_to_s3(
                    Path(result_destination),
                    config.s3_bucket,
                    key,
                    region=config.s3_region,
                )
                upload_status = "uploaded"
                entry = {"local_path": result_destination, "s3_uri": s3_uri}
                s3_manifest_rows.append(entry)
                s3_manifest_index[result_destination] = entry
        elif config.s3_bucket:
            upload_status = "pending"

        if result["status"] == "success":
            manifest_row = manifest_lookup.get(source)
            if manifest_row is not None:
                record = generate_metadata_file_record(
                    manifest_row,
                    result_destination,
                    openslide_path=config.openslide_path,
                )
                submission_records.append(record)

        if result["status"] == "success" and config.s3_bucket and not config.keep_local:
            Path(result_destination).unlink(missing_ok=True)
            local_deleted = "yes"

        status_row = pd.Series({
            "destination": result_destination,
            "source_hash": source_hash,
            "status": result["status"],
            "error": result["error"],
            "md5": result["md5"],
            "upload_status": upload_status,
            "s3_uri": s3_uri,
            "local_deleted": local_deleted,
        })
        # ! assuming we can change append on s3 resuming to series from dict,
        # ! can consider changing status_rows type_hint to series. Ignoring 
        # ! type error/warning for now though 
        status_rows.append(status_row) # type: ignore

        logger.info('iterating through status_rows')
        for v in status_rows:
            logger.info(f'status rows ({type(v)}) = {v}')
        
        
        write_status_csv(status_rows, out_dir)
        if s3_manifest_rows:
            write_s3_manifest(out_dir, s3_manifest_rows)

    status_csv = write_status_csv(status_rows, out_dir)

    submission_path = write_submission_csv(pd.DataFrame(submission_records), out_dir)

    run_journal = write_run_journal(
        config,
        out_dir,
        {
            "manifest_rows": len(manifest_df),
            "success": sum(1 for r in status_rows if r["status"] == "success"),
            "failed": sum(1 for r in status_rows if r["status"] != "success"),
        },
    )

    s3_manifest_path: Path | None = None
    if config.s3_bucket and s3_manifest_rows:
        s3_manifest_path = write_s3_manifest(out_dir, s3_manifest_rows)

    return {
        "derived_csv": derived_csv,
        "status_csv": status_csv,
        "run_journal": run_journal,
        "submission_csv": submission_path,
        "s3_manifest": s3_manifest_path,
    }
