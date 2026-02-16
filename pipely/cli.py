from __future__ import annotations

import shlex
import subprocess
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
    pipeline_name = data.get("name", "pipeline")
    run_id = str(int(time.time()))
    console.print(f"[bold]Running[/bold] {pipeline_name} (run_id={run_id})")

    jobs: dict = data.get("jobs", {})
    if not jobs:
        console.print("[red]No jobs found in pipeline YAML.[/red]")
        raise typer.Exit(code=2)

    # MVP runner: run jobs in the order they appear, steps in order.
    for job_name, job in jobs.items():
        console.print(f"\n[bold blue]Job:[/bold blue] {job_name}")

        steps = job.get("steps", [])
        if not steps:
            console.print("[yellow]  (no steps)[/yellow]")
            continue

        for idx, step in enumerate(steps, start=1):
            step_name = step.get("name", f"step-{idx}")
            command = step.get("run")

            if not command:
                console.print(f"[yellow]  Skipping {step_name}: no 'run' command[/yellow]")
                continue

            console.print(f"[cyan]  → Step {idx}: {step_name}[/cyan]")
            console.print(f"[dim]    $ {command}[/dim]")

            if dry_run:
                console.print("[yellow]    DRY RUN[/yellow]")
                continue

            # Safely parse the command into arguments
            try:
                args = shlex.split(command)
            except ValueError as e:
                console.print(f"[red]    Failed to parse command: {e}[/red]")
                raise typer.Exit(code=2)

            # Execute the command with timeout safety
            try:
                result = subprocess.run(
                    args,
                    text=True,
                    capture_output=True,
                    timeout=300,  # 5 minute safety timeout
                )
            except subprocess.TimeoutExpired:
                console.print("[red]    ✖ Step timed out after 300s[/red]")
                raise typer.Exit(code=124)

            # Print output
            if result.stdout:
                console.print(result.stdout.rstrip())
            if result.stderr:
                console.print(f"[red]{result.stderr.rstrip()}[/red]")

            # Fail fast on error
            if result.returncode != 0:
                console.print(f"[red]    ✖ Step failed (exit code {result.returncode})[/red]")
                raise typer.Exit(code=result.returncode)

            console.print("[green]    ✓ OK[/green]")

    console.print("\n[bold green]Pipeline completed successfully.[/bold green]")
