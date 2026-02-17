from __future__ import annotations

import shlex
import subprocess
import time
from pathlib import Path

import typer
import yaml
from rich.console import Console

from pipely.logging_utils import init_logging

app = typer.Typer(no_args_is_help=True)
console = Console()

run_app = typer.Typer(no_args_is_help=True)
app.add_typer(run_app, name="run")


@run_app.command("pipeline")
def pipeline(
    pipeline_file: Path = typer.Argument(..., exists=True),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    # Generate a readable run id like 20260216-180903
    run_id = time.strftime("%Y%m%d-%H%M%S")

    # Set up file logging and get a logger for this run
    logger, log_path = init_logging(run_id)

    logger.info("Starting pipeline run")
    logger.info("Run id: %s", run_id)
    logger.info("Log file: %s", log_path)
    logger.info("Pipeline file: %s", pipeline_file)
    logger.info("Dry run: %s", dry_run)

    # Load YAML
    try:
        data = yaml.safe_load(pipeline_file.read_text(encoding="utf-8")) or {}
    except Exception as e:
        console.print(f"[red]Failed to read YAML: {e}[/red]")
        raise typer.Exit(code=2)

    pipeline_name = data.get("name", "pipeline")
    logger.info("Pipeline name: %s", pipeline_name)

    # Print a short human-friendly header (logging has the full detail)
    console.print(f"[bold]Running[/bold] {pipeline_name} (run_id={run_id})")

    jobs: dict = data.get("jobs", {})
    if not jobs:
        console.print("[red]No jobs found in pipeline YAML.[/red]")
        logger.error("No jobs found in pipeline YAML")
        raise typer.Exit(code=2)

    # MVP runner: run jobs in YAML order; steps in order
    for job_name, job in jobs.items():
        console.print(f"\n[bold blue]Job:[/bold blue] {job_name}")
        logger.info("=== Job start: %s ===", job_name)

        steps = job.get("steps", [])
        if not steps:
            console.print("[yellow]  (no steps)[/yellow]")
            logger.warning("Job %s has no steps", job_name)
            logger.info("=== Job end: %s ===", job_name)
            continue

        for idx, step in enumerate(steps, start=1):
            step_name = step.get("name", f"step-{idx}")
            command = step.get("run")

            if not command:
                console.print(f"[yellow]  Skipping {step_name}: no 'run' command[/yellow]")
                logger.warning("Step %s skipped: no 'run' command", step_name)
                continue

            logger.info("Step %s start: %s", idx, step_name)
            logger.info("$ %s", command)

            console.print(f"[cyan]  → Step {idx}: {step_name}[/cyan]")
            console.print(f"[dim]    $ {command}[/dim]")

            if dry_run:
                console.print("[yellow]    DRY RUN[/yellow]")
                logger.info("Step %s DRY RUN: %s", idx, step_name)
                continue

            # Safer than shell=True: parse into args and run directly
            try:
                args = shlex.split(command)
            except ValueError as e:
                console.print(f"[red]    Failed to parse command: {e}[/red]")
                logger.error("Failed to parse command: %s", e)
                raise typer.Exit(code=2)

            result = subprocess.run(args, text=True, capture_output=True)

            # Log stdout/stderr to file AND show in terminal
            if result.stdout:
                logger.info("stdout:\n%s", result.stdout.rstrip())
                console.print(result.stdout.rstrip())

            if result.stderr:
                logger.warning("stderr:\n%s", result.stderr.rstrip())
                console.print(f"[red]{result.stderr.rstrip()}[/red]")


            # --- Failure strategy: continue_on_error ---
            continue_on_error = step.get("continue_on_error", False)

            if result.returncode != 0:
                console.print(f"[red]    ✖ Step failed (exit code {result.returncode})[/red]")
                logger.error(
                    "Step %s FAILED: %s (exit code %s)",
                    idx,
                    step_name,
                    result.returncode,
                )

                if continue_on_error:
                    console.print("[yellow]    ⚠ Continuing despite failure[/yellow]")
                    logger.warning("Continuing despite failure (continue_on_error=true)")
                    continue

                raise typer.Exit(code=result.returncode)

            # Only log OK if we did NOT fail
            console.print("[green]    ✓ OK[/green]")
            logger.info("Step %s OK: %s", idx, step_name)

        logger.info("=== Job end: %s ===", job_name)

    console.print("\n[bold green]Pipeline completed successfully.[/bold green]")
    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    app()
