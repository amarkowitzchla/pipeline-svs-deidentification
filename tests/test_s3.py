from __future__ import annotations

from pathlib import Path

import boto3
import pytest

moto = pytest.importorskip("moto")
from svs_deid_pipeline.s3 import upload_directory_to_s3


@moto.mock_s3
def test_upload_directory_to_s3(tmp_path: Path) -> None:
    bucket = "test-bucket"
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=bucket)

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    file_path = data_dir / "file.txt"
    file_path.write_text("content", encoding="utf-8")

    manifest = upload_directory_to_s3(data_dir, bucket, prefix="runs/001")

    assert len(manifest) == 1
    obj = client.get_object(Bucket=bucket, Key="runs/001/file.txt")
    assert obj["Body"].read().decode("utf-8") == "content"
