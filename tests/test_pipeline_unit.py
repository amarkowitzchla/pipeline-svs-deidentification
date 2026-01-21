from __future__ import annotations

from pathlib import Path

import pandas as pd

from svs_deid_pipeline.pipeline import build_source_dest_df, destination_basename


def test_destination_basename_is_deterministic() -> None:
    row = pd.Series({"rid": "RID001", "specnum_formatted": "SPEC001"})
    assert destination_basename(row) == destination_basename(row)


def test_build_source_dest_df(manifest_csv: Path, tmp_path: Path) -> None:
    df = pd.read_csv(manifest_csv)
    out_dir = tmp_path / "out"
    result = build_source_dest_df(df, out_dir)

    assert list(result.columns) == ["source", "destination"]
    assert result["destination"].str.contains("/svs/").all()
    assert result["destination"].str.endswith(".svs").all()
