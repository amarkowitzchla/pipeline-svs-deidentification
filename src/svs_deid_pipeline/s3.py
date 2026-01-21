from __future__ import annotations

import logging
import time
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger("svs_deid_pipeline.s3")


def upload_file_to_s3(
    local_path: Path,
    bucket: str,
    key: str,
    *,
    region: str | None = None,
    max_retries: int = 3,
    backoff_seconds: float = 1.0,
) -> str:
    client = boto3.client("s3", region_name=region)
    logger.info("Uploading %s to s3://%s/%s", local_path.name, bucket, key)
    attempts = 0
    while True:
        try:
            client.upload_file(str(local_path), bucket, key)
            logger.info("Uploaded %s to s3://%s/%s", local_path.name, bucket, key)
            return f"s3://{bucket}/{key}"
        except (BotoCoreError, ClientError) as exc:
            attempts += 1
            if attempts > max_retries:
                logger.error(
                    "S3 upload failed for %s after %d attempts.",
                    local_path.name,
                    attempts,
                )
                raise RuntimeError(f"S3 upload failed for {local_path.name}") from exc
            logger.warning(
                "Retrying S3 upload for %s (attempt %d/%d).",
                local_path.name,
                attempts,
                max_retries,
            )
            time.sleep(backoff_seconds * (2**(attempts - 1)))


def upload_directory_to_s3(
    local_dir: Path,
    bucket: str,
    prefix: str | None = None,
    *,
    region: str | None = None,
    max_retries: int = 3,
    backoff_seconds: float = 1.0,
) -> list[dict[str, str]]:
    client = boto3.client("s3", region_name=region)
    prefix = prefix.strip("/") if prefix else ""
    manifest: list[dict[str, str]] = []

    for path in sorted(local_dir.rglob("*")):
        if path.is_dir():
            continue
        relative = path.relative_to(local_dir).as_posix()
        key = f"{prefix}/{relative}" if prefix else relative
        attempts = 0
        while True:
            try:
                client.upload_file(str(path), bucket, key)
                break
            except (BotoCoreError, ClientError) as exc:
                attempts += 1
                if attempts > max_retries:
                    raise RuntimeError(f"S3 upload failed for {path.name}") from exc
                time.sleep(backoff_seconds * (2**(attempts - 1)))

        manifest.append(
            {
                "local_path": str(path),
                "s3_uri": f"s3://{bucket}/{key}",
            }
        )

    return manifest
