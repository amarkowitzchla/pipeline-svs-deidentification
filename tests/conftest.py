from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture()
def manifest_csv(tmp_path: Path) -> Path:
    df = pd.DataFrame(
        {
            "location": ["/data/a.svs", "/data/b.svs"],
            "rid": ["RID001", "RID002"],
            "specnum_formatted": ["SPEC001", "SPEC002"],
            "stain": ["H&E", "H&E"],
        }
    )
    path = tmp_path / "manifest.csv"
    df.to_csv(path, index=False)
    return path
