from __future__ import annotations

import shlex
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import typer
import yaml
from rich.console import Console

from pipely.logging_utils import init_logging, new_run_id

app = typer.Typer(no_args_is_help=True)
console = Console()

run_app = typer.Typer(no_args_is_help=True)
app.add_typer(run_app, name="run")


@run_app.command("pipeline")
def pipeline(
    pipeline_file: Path = typer.Argument(..., exists=True),
    dry_run: bool = typer.Option(False, "--dry-run"),
    max_workers: int = typer.Option(4, "--max-workers", min=1, help="Max parallel jobs."),
    fail_fast: bool = typer.Option(
        False,
        "--fail-fast",
        help="Stop scheduling new jobs after the first failure (still waits for running jobs).",
    ),
) -> None:
    """
    Run a pipeline YAML.

    Jobs run in parallel (up to --max-workers).
    Steps within a job run sequentially.
    """
    run_id = new_run_id()
    logger, log_path = init_logging(run_id)

    logger.info("Starting pipeline run")
    logger.info("Run id: %s", run_id)
    logger.info("Log file: %s", log_path)
    logger.info("Pipeline file: %s", pipeline_file)
    logger.info("Dry run: %s", dry_run)
    logger.info("Max workers: %s", max_workers)
    logger.info("Fail fast: %s", fail_fast)

    # Load YAML
    try:
        data = yaml.safe_load(pipeline_file.read_text(encoding="utf-8")) or {}
    except Exception as e:
        console.print(f"[red]Failed to read YAML: {e}[/red]")
        raise typer.Exit(code=2)

    pipeline_name = data.get("name", "pipeline")
    logger.info("Pipeline name: %s", pipeline_name)
    console.print(f"[bold]Running[/bold] {pipeline_name} (run_id={run_id})")

    jobs: dict[str, Any] = data.get("jobs", {}) or {}
    if not jobs:
        console.print("[red]No jobs found in pipeline YAML.[/red]")
        logger.error("No jobs found in pipeline YAML")
        raise typer.Exit(code=2)

    # Prevent messy interleaving in terminal output when jobs run in parallel.
    console_lock = threading.Lock()

    def safe_print(msg: str) -> None:
        with console_lock:
            console.print(msg)

    def run_job(job_name: str, job: dict[str, Any]) -> int:
        """
        Run a single job. Return 0 if success else non-zero.
        """
        logger.info("=== Job start: %s ===", job_name)
        safe_print(f"\n[bold blue]Job:[/bold blue] {job_name}")

        steps = job.get("steps", []) or []
        if not steps:
            safe_print("[yellow]  (no steps)[/yellow]")
            logger.warning("Job %s has no steps", job_name)
            logger.info("=== Job end: %s ===", job_name)
            return 0

        for idx, step in enumerate(steps, start=1):
            step_name = step.get("name", f"step-{idx}")
            command = step.get("run")

            if not command:
                safe_print(f"[yellow]  Skipping {job_name}.{step_name}: no 'run' command[/yellow]")
                logger.warning("[%s] Step %s skipped: no 'run' command", job_name, step_name)
                continue

            logger.info("[%s] Step %s start: %s", job_name, idx, step_name)
            logger.info("[%s] $ %s", job_name, command)

            safe_print(f"[cyan]  → ({job_name}) Step {idx}: {step_name}[/cyan]")
            safe_print(f"[dim]    $ {command}[/dim]")

            if dry_run:
                safe_print("[yellow]    DRY RUN[/yellow]")
                logger.info("[%s] Step %s DRY RUN: %s", job_name, idx, step_name)
                continue

            # Safer than shell=True: parse into args and run directly
            try:
                args = shlex.split(command)
            except ValueError as e:
                safe_print(f"[red]    Failed to parse command: {e}[/red]")
                logger.error("[%s] Failed to parse command: %s", job_name, e)
                return 2

            retries = int(step.get("retries", 0) or 0)
            retry_delay_seconds = float(step.get("retry_delay_seconds", 0) or 0)
            timeout_seconds = step.get("timeout_seconds")

            attempt = 0
            last_result: subprocess.CompletedProcess[str] | None = None

            while True:
                attempt += 1

                try:
                    last_result = subprocess.run(
                        args,
                        text=True,
                        capture_output=True,
                        timeout=timeout_seconds,
                    )
                except subprocess.TimeoutExpired:
                    safe_print(f"[red]    ⏰ Step timed out after {timeout_seconds}s[/red]")
                    logger.error("[%s] Step %s TIMEOUT after %ss: %s", job_name, idx, timeout_seconds, step_name)

                    continue_on_error = bool(step.get("continue_on_error", False))
                    if continue_on_error:
                        safe_print("[yellow]    ⚠ Continuing despite timeout[/yellow]")
                        logger.warning("[%s] Continuing despite timeout (continue_on_error=true)", job_name)
                        break

                    return 124  # common "timeout" style code

                # Success
                if last_result.returncode == 0:
                    break

                # Failure, no retries left
                if attempt > (retries + 1):
                    break

                # Retry
                if attempt <= (retries + 1):
                    next_attempt = attempt + 1
                    if attempt <= retries:
                        safe_print(
                            f"[yellow]Retrying {job_name}.{step_name} "
                            f"(attempt {attempt + 1}/{retries + 1})[/yellow]"
                        )
                        logger.warning(
                            "[%s] Retrying step %s (attempt %s/%s)",
                            job_name,
                            step_name,
                            attempt + 1,
                            retries + 1,
                        )
                        if retry_delay_seconds > 0:
                            # small sleep without importing time globally
                            import time as _time

                            _time.sleep(retry_delay_seconds)
                        continue
                    break

            # If we never got a result (shouldn't happen), treat as failure
            if last_result is None:
                logger.error("[%s] Step produced no result: %s", job_name, step_name)
                return 2

            # Log stdout/stderr to file AND show in terminal
            if last_result.stdout:
                logger.info("[%s] stdout:\n%s", job_name, last_result.stdout.rstrip())
                safe_print(last_result.stdout.rstrip())

            if last_result.stderr:
                logger.warning("[%s] stderr:\n%s", job_name, last_result.stderr.rstrip())
                safe_print(f"[red]{last_result.stderr.rstrip()}[/red]")

            continue_on_error = bool(step.get("continue_on_error", False))

            if last_result.returncode != 0:
                safe_print(f"[red]    ✖ Step failed (exit code {last_result.returncode})[/red]")
                logger.error(
                    "[%s] Step %s FAILED: %s (exit code %s)",
                    job_name,
                    idx,
                    step_name,
                    last_result.returncode,
                )

                if continue_on_error:
                    safe_print("[yellow]    ⚠ Continuing despite failure[/yellow]")
                    logger.warning("[%s] Continuing despite failure (continue_on_error=true)", job_name)
                    continue

                return int(last_result.returncode)

            safe_print("[green]    ✓ OK[/green]")
            logger.info("[%s] Step %s OK: %s", job_name, idx, step_name)

        logger.info("=== Job end: %s ===", job_name)
        return 0

    # Run jobs in parallel
    job_results: dict[str, int] = {}
    any_failed = False

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for job_name, job in jobs.items():
            if fail_fast and any_failed:
                logger.warning("Fail-fast enabled: skipping job %s (not scheduled)", job_name)
                safe_print(f"[yellow]Skipping job {job_name} due to fail-fast[/yellow]")
                job_results[job_name] = 1
                continue

            futures[pool.submit(run_job, job_name, job)] = job_name

        for future in as_completed(futures):
            job_name = futures[future]
            try:
                code = int(future.result())
            except Exception as e:
                # Hard crash in worker
                logger.exception("Job %s crashed: %s", job_name, e)
                code = 2

            job_results[job_name] = code
            if code != 0:
                any_failed = True

    # Summary
    safe_print("\n[bold]Job summary:[/bold]")
    for name, code in job_results.items():
        if code == 0:
            safe_print(f"[green]  ✓ {name}[/green]")
        else:
            safe_print(f"[red]  ✖ {name} (exit {code})[/red]")

    if any_failed:
        logger.error("Pipeline completed with failures: %s", job_results)
        safe_print("\n[bold red]Pipeline completed with failures.[/bold red]")
        raise typer.Exit(code=1)

    logger.info("Pipeline completed successfully")
    safe_print("\n[bold green]Pipeline completed successfully.[/bold green]")


if __name__ == "__main__":
    app()
