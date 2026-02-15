from __future__ import annotations

import time
from pathlib import Path

import typer
import yaml
from rich.console import Console

app = typer.Typer(no_args_is_help=True)
console = Console()

run_app = typer.Typer(no_args_is_help=True)
app.add_typer(run_app, name="run")


@run_app.command("pipeline")
def pipeline(
    pipeline_file: Path = typer.Argument(..., exists=True),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    data = yaml.safe_load(pipeline_file.read_text(encoding="utf-8"))
    name = data.get("name", "pipeline")
    run_id = str(int(time.time()))
    console.print(f"[bold]Running[/bold] {name} (run_id={run_id})")

    if dry_run:
        console.print("[yellow]DRY RUN[/yellow] - not executing commands")
    else:
        console.print("[green]OK[/green] (stub runner for now)")

    raise typer.Exit(code=0)
