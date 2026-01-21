# SVS De-Identification Pipeline

Pipeline for de-identifying SVS files from a manifest CSV with optional S3 upload.

## Prerequisites

- Python 3.10+
- OpenSlide libraries installed on your system
  - macOS: `brew install openslide`
  - Linux: `apt-get install openslide-tools libopenslide0`
  - Windows: install OpenSlide binaries and set `SVS_DEID_OPENSLIDE_PATH`
- AWS credentials (only if using S3): configure `~/.aws/credentials` or environment variables

## Venv + Installation

This repo contains the pipeline package. It also depends on the upstream de-identification approach from `svs-deidentifier`.

- Upstream project: https://github.com/pearcetm/svs-deidentifier
- This pipeline uses the same `source`/`destination` CSV contract and copy+strip behavior.

```
python -m venv .venv
source .venv/bin/activate
pip install -U pip setuptools wheel
cd svs_deid_pipeline
pip install -e .
```

### Link `svs-deidentifier` in your repo (recommended)

Add it as a submodule inside this repo (so the path is `svs_deid_pipeline/svs-deidentifier`):

```
git submodule add https://github.com/pearcetm/svs-deidentifier svs-deidentifier
git submodule update --init --recursive
```

If you prefer not to use submodules, clone it next to your repo and keep it as a sibling folder.

## Example input CSV

See `examples/example_manifest.csv` for a template (do not commit SVS files or PHI).

Example manifest CSV (required columns):

```
location,rid,specnum_formatted,stain
/path/to/slide1.svs,RID0001,SPEC0001,H&E
/path/to/slide2.svs,RID0002,SPEC0002,CD3
```

You may use relative paths; they are resolved from the current working directory or the manifest location:

```
location,rid,specnum_formatted,stain
data/70275.svs,RID0001,SPEC0001,H&E
data/70276.svs,RID0002,SPEC0002,
```

## How it works (implementation overview)

1. Read the manifest CSV (`location`, `rid`, `specnum_formatted`, `stain`).
2. Create a derived `source/destination` CSV for `svs-deidentifier`.
3. De-identify each SVS in copy mode (label + macro removed).
4. Optionally upload each de-identified SVS to S3.
5. Write per-file status (`status.csv`), run journal (`run.json`), and submission spreadsheet (`submission.csv`).
6. `--resume` continues from the last successful upload/status state.

## Run locally (dry-run)

```
svs_deid run --manifest examples/example_manifest.csv --out ./out --dry-run
```

## Run locally (de-identify + submission)

```
svs_deid run --manifest examples/example_manifest.csv --out ./out
```

## Run with S3

```
svs_deid run --manifest ../test_mani.csv --out ./out --s3-bucket my-bucket --s3-prefix runs/001 --dry-run
```

## Upload-only storage (no local SVS retention)

```
svs_deid run --manifest examples/example_manifest.csv --out ./out --s3-bucket my-bucket --s3-prefix runs/001 --no-local
```

## Resume a partial run

```
svs_deid run --manifest examples/example_manifest.csv --out ./out --resume
```

## Configuration (env vars)

- `SVS_DEID_MANIFEST`
- `SVS_DEID_OUT_DIR`
- `SVS_DEID_S3_BUCKET`
- `SVS_DEID_S3_PREFIX`
- `SVS_DEID_S3_REGION`
- `SVS_DEID_OPENSLIDE_PATH` (Windows only)
- `SVS_DEID_LOG_LEVEL`
- `SVS_DEID_WORKERS`
- `SVS_DEID_DRY_RUN`
- `SVS_DEID_ALLOW_PARTIAL`
- `SVS_DEID_FAIL_FAST`
- `SVS_DEID_RESUME`
- `SVS_DEID_KEEP_LOCAL`

## Tests

```
pytest -q --disable-warnings --maxfail=1 --cov=svs_deid_pipeline
pytest -m "not integration"
```

## Input/Output Contract

Manifest CSV columns (required):
- `location`
- `rid`
- `specnum_formatted`
- `stain`

Derived CSV:
- `source`
- `destination`

The derived CSV matches the `svs-deidentifier` expected format (per its README).

Outputs (per run):
- `out/derived/source_destination.csv`
- `out/status/status.csv`
- `out/run.json`
- `out/submission/submission.csv`
- `out/s3_manifest.csv` (if S3 upload is enabled)
