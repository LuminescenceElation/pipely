from __future__ import annotations

import shlex
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from time import perf_counter
from typing import Dict, List, Set

import typer
import yaml
from rich.console import Console

from pipely.logging_utils import init_logging, new_run_id

app = typer.Typer(no_args_is_help=True)
console = Console()

run_app = typer.Typer(no_args_is_help=True)
app.add_typer(run_app, name="run")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_needs(needs) -> List[str]:
    """Ensure `needs` is always a list."""
    if needs is None:
        return []
    if isinstance(needs, str):
        return [needs]
    return list(needs)


# ---------------------------------------------------------------------------
# Main command
# ---------------------------------------------------------------------------

@run_app.command("pipeline")
def pipeline(
    pipeline_file: Path = typer.Argument(..., exists=True),
    dry_run: bool = typer.Option(False, "--dry-run"),
    max_workers: int = typer.Option(1, "--max-workers"),
    fail_fast: bool = typer.Option(False, "--fail-fast"),
) -> None:
    run_id = new_run_id()
    logger, log_path = init_logging(run_id)

    logger.info("Starting pipeline run")
    logger.info("Run id: %s", run_id)
    logger.info("Pipeline file: %s", pipeline_file)
    logger.info("Max workers: %s", max_workers)
    logger.info("Fail fast: %s", fail_fast)

    data = yaml.safe_load(pipeline_file.read_text()) or {}
    pipeline_name = data.get("name", "pipeline")
    jobs: Dict[str, dict] = data.get("jobs", {})

    console.print(f"[bold]Running[/bold] {pipeline_name} (run_id={run_id})")

    if not jobs:
        console.print("[red]No jobs found.[/red]")
        raise typer.Exit(code=2)

    # -----------------------------------------------------------------------
    # Dependency tracking
    # -----------------------------------------------------------------------
    remaining_jobs: Dict[str, dict] = jobs.copy()
    completed: Set[str] = set()
    failed: Set[str] = set()
    job_durations: Dict[str, float] = {}

    pipeline_start = perf_counter()

    # -----------------------------------------------------------------------
    # Job runner
    # -----------------------------------------------------------------------
    def run_job(job_name: str, job: dict) -> tuple[str, bool]:
        job_start = perf_counter()

        console.print(f"\n[bold blue]Job:[/bold blue] {job_name}")
        logger.info("=== Job start: %s ===", job_name)

        steps = job.get("steps", [])
        for idx, step in enumerate(steps, start=1):
            step_name = step.get("name", f"step-{idx}")
            command = step.get("run")

            console.print(f"[cyan]  → ({job_name}) Step {idx}: {step_name}[/cyan]")

            if dry_run:
                console.print("[yellow]    DRY RUN[/yellow]")
                continue

            args = shlex.split(command)
            retries = int(step.get("retries", 0))
            timeout_seconds = step.get("timeout_seconds")
            continue_on_error = step.get("continue_on_error", False)

            attempt = 0
            while True:
                attempt += 1
                try:
                    result = subprocess.run(
                        args,
                        text=True,
                        capture_output=True,
                        timeout=timeout_seconds,
                    )
                except subprocess.TimeoutExpired:
                    console.print(f"[red]    ⏰ Timeout ({timeout_seconds}s)[/red]")
                    if continue_on_error:
                        break
                    return job_name, False

                if result.returncode == 0:
                    break

                if attempt > retries:
                    break

                console.print(f"[yellow]Retrying {step_name} ({attempt}/{retries})[/yellow]")

            if result.stdout:
                console.print(result.stdout.rstrip())

            if result.stderr:
                console.print(f"[red]{result.stderr.rstrip()}[/red]")

            if result.returncode != 0 and not continue_on_error:
                return job_name, False

            console.print("[green]    ✓ OK[/green]")

        duration = perf_counter() - job_start
        job_durations[job_name] = duration
        console.print(f"[dim]Job duration: {duration:.2f}s[/dim]")

        logger.info("=== Job end: %s (%.2fs) ===", job_name, duration)
        return job_name, True

    # -----------------------------------------------------------------------
    # Scheduler loop
    # -----------------------------------------------------------------------
    while remaining_jobs:
        ready = []

        # Find jobs whose dependencies are satisfied
        for job_name, job in remaining_jobs.items():
            needs = normalize_needs(job.get("needs"))
            if all(dep in completed for dep in needs):
                ready.append(job_name)

        if not ready:
            console.print("[red]Dependency cycle detected![/red]")
            raise typer.Exit(code=2)

        # Run ready jobs in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(run_job, name, remaining_jobs[name]): name
                for name in ready
            }

            for future in as_completed(futures):
                job_name, success = future.result()

                if success:
                    completed.add(job_name)
                else:
                    failed.add(job_name)
                    if fail_fast:
                        console.print("[red]Fail-fast triggered[/red]")
                        raise typer.Exit(code=1)

                del remaining_jobs[job_name]

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    total_time = perf_counter() - pipeline_start

    console.print("\n[bold]Job summary:[/bold]")
    for job_name in jobs:
        status = "✓" if job_name in completed else "✖"
        duration = job_durations.get(job_name, 0.0)
        console.print(f"{status} {job_name} ({duration:.2f}s)")

    console.print(f"\n[bold]Total pipeline time:[/bold] {total_time:.2f}s")

    if failed:
        console.print("[bold red]Pipeline completed with failures[/bold red]")
        raise typer.Exit(code=1)
    else:
        console.print("[bold green]Pipeline completed successfully[/bold green]")


if __name__ == "__main__":
    app()
