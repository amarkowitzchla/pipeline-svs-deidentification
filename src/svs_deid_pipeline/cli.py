from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from .config import load_config
from .pipeline import read_manifest, run_pipeline

app = typer.Typer(no_args_is_help=True)
console = Console()


@app.command("run")
def run_command(
    manifest: Path = typer.Option(..., "--manifest", exists=True, dir_okay=False),
    out_dir: Path = typer.Option(..., "--out", dir_okay=True, file_okay=False),
    config: Path | None = typer.Option(None, "--config", dir_okay=False),
    s3_bucket: str | None = typer.Option(None, "--s3-bucket"),
    s3_prefix: str | None = typer.Option(None, "--s3-prefix"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    workers: int = typer.Option(1, "--workers", min=1),
    allow_partial: bool = typer.Option(False, "--allow-partial"),
    fail_fast: bool = typer.Option(False, "--fail-fast"),
    resume: bool = typer.Option(False, "--resume"),
    keep_local: bool = typer.Option(True, "--keep-local/--no-local"),
    max_files: int | None = typer.Option(None, "--max-files", min=1),
    log_level: str | None = typer.Option(None, "--log-level"),
) -> None:
    config_obj = load_config(
        manifest=manifest,
        out_dir=out_dir,
        config_path=config,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        dry_run=dry_run,
        workers=workers,
        allow_partial=allow_partial,
        fail_fast=fail_fast,
        resume=resume,
        keep_local=keep_local,
        max_files=max_files,
        log_level=log_level,
    )
    outputs = run_pipeline(config_obj)
    status_path = outputs.get("status_csv")
    if status_path and not config_obj.dry_run:
        import pandas as pd

        status_df = pd.read_csv(status_path)
        failures = int((status_df["status"] != "success").sum())
        if failures > 0 and not config_obj.allow_partial:
            console.print("Pipeline completed with failures.")
            raise typer.Exit(code=2)
    console.print("Pipeline completed.")


@app.command("dry-run")
def dry_run_command(
    manifest: Path = typer.Option(..., "--manifest", exists=True, dir_okay=False),
    out_dir: Path = typer.Option(..., "--out", dir_okay=True, file_okay=False),
    config: Path | None = typer.Option(None, "--config", dir_okay=False),
) -> None:
    config_obj = load_config(
        manifest=manifest,
        out_dir=out_dir,
        config_path=config,
        dry_run=True,
    )
    run_pipeline(config_obj)
    console.print("Dry-run completed.")


@app.command("validate-manifest")
def validate_manifest_command(
    manifest: Path = typer.Option(..., "--manifest", exists=True, dir_okay=False),
) -> None:
    df = read_manifest(manifest)
    console.print(f"Manifest valid with {len(df)} rows.")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
