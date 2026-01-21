from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from svs_deid_pipeline.pipeline import read_manifest


def test_read_manifest_missing_columns(tmp_path: Path) -> None:
    df = pd.DataFrame({"location": ["/data/a.svs"], "rid": ["RID001"]})
    path = tmp_path / "bad_manifest.csv"
    df.to_csv(path, index=False)

    with pytest.raises(ValueError):
        read_manifest(path)


def test_read_manifest_drops_empty_locations(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "location": ["/data/a.svs", None],
            "rid": ["RID001", "RID002"],
            "specnum_formatted": ["SPEC001", "SPEC002"],
            "stain": ["H&E", "H&E"],
        }
    )
    path = tmp_path / "manifest.csv"
    df.to_csv(path, index=False)

    result = read_manifest(path)
    assert len(result) == 1
