from __future__ import annotations

import shlex
import subprocess
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import dataclass
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


# ----------------------------
# Models / helpers
# ----------------------------

@dataclass
class JobResult:
    name: str
    status: str  # "success" | "failed" | "skipped" | "canceled"
    duration_s: float
    error: str | None = None


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value]
    return [str(value)]


def _run_command_with_cancel(
    args: list[str],
    *,
    timeout_seconds: int | None,
    cancel_flag: dict[str, bool],
) -> subprocess.CompletedProcess:
    """
    Run a command but allow cooperative cancellation.

    - If cancel_flag["cancel"] becomes True while running, we terminate the process.
    - Returns a CompletedProcess-like object.
    """
    start = time.time()
    proc = subprocess.Popen(
        args,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Poll loop so we can notice cancellation.
        while True:
            if cancel_flag.get("cancel", False):
                # Best-effort stop
                proc.terminate()
                try:
                    out, err = proc.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    out, err = proc.communicate()
                return subprocess.CompletedProcess(args, returncode=130, stdout=out, stderr=err)

            if timeout_seconds is not None and (time.time() - start) > timeout_seconds:
                proc.terminate()
                try:
                    out, err = proc.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    out, err = proc.communicate()
                raise subprocess.TimeoutExpired(cmd=args, timeout=timeout_seconds, output=out, stderr=err)

            rc = proc.poll()
            if rc is not None:
                out, err = proc.communicate()
                return subprocess.CompletedProcess(args, returncode=rc, stdout=out, stderr=err)

            time.sleep(0.05)
    finally:
        # Safety: if anything weird happens, ensure the process isn't left hanging.
        if proc.poll() is None:
            proc.kill()


def _job_can_start(job_name: str, needs_map: dict[str, list[str]], statuses: dict[str, str]) -> bool:
    return all(statuses.get(dep) == "success" for dep in needs_map.get(job_name, []))


def _job_is_blocked(job_name: str, needs_map: dict[str, list[str]], statuses: dict[str, str]) -> bool:
    """
    Blocked if any dependency is failed/skipped/canceled.
    """
    for dep in needs_map.get(job_name, []):
        if statuses.get(dep) in {"failed", "skipped", "canceled"}:
            return True
    return False


# ----------------------------
# Job execution
# ----------------------------

def _run_job(
    job_name: str,
    job: dict[str, Any],
    *,
    dry_run: bool,
    logger,
    cancel_flag: dict[str, bool],
) -> JobResult:
    """
    Runs a single job (steps in order).
    Cooperatively stops if cancel_flag["cancel"] is set.
    """
    job_start = time.time()
    logger.info("=== Job start: %s ===", job_name)

    steps = job.get("steps", []) or []
    if not steps:
        logger.warning("Job %s has no steps", job_name)
        logger.info("=== Job end: %s ===", job_name)
        return JobResult(name=job_name, status="success", duration_s=time.time() - job_start)

    for idx, step in enumerate(steps, start=1):
        if cancel_flag.get("cancel", False):
            logger.warning("Job %s canceled before step %s", job_name, idx)
            return JobResult(name=job_name, status="canceled", duration_s=time.time() - job_start)

        step_name = step.get("name", f"step-{idx}")
        command = step.get("run")

        if not command:
            console.print(f"[yellow]Skipping {step_name}: no 'run' command[/yellow]")
            logger.warning("Step %s skipped: no 'run' command", step_name)
            continue

        retries = int(step.get("retries", 0) or 0)
        retry_delay_seconds = float(step.get("retry_delay_seconds", 0) or 0)
        timeout_seconds = step.get("timeout_seconds")
        timeout_seconds = int(timeout_seconds) if timeout_seconds is not None else None
        continue_on_error = bool(step.get("continue_on_error", False))

        console.print(f"[cyan]  → ({job_name}) Step {idx}: {step_name}[/cyan]")
        console.print(f"[dim]    $ {command}[/dim]")

        logger.info("[%s] Step %s start: %s", job_name, idx, step_name)
        logger.info("[%s] $ %s", job_name, command)

        if dry_run:
            console.print("[yellow]    DRY RUN[/yellow]")
            logger.info("[%s] Step %s DRY RUN: %s", job_name, idx, step_name)
            continue

        try:
            args = shlex.split(command)
        except ValueError as e:
            console.print(f"[red]Failed to parse command: {e}[/red]")
            logger.error("[%s] Failed to parse command: %s", job_name, e)
            return JobResult(name=job_name, status="failed", duration_s=time.time() - job_start, error=str(e))

        attempt = 0
        last_result: subprocess.CompletedProcess | None = None

        while True:
            attempt += 1
            try:
                last_result = _run_command_with_cancel(
                    args,
                    timeout_seconds=timeout_seconds,
                    cancel_flag=cancel_flag,
                )
            except subprocess.TimeoutExpired:
                console.print(f"[red]    ⏰ Step timed out after {timeout_seconds}s[/red]")
                logger.error("[%s] Step %s TIMEOUT after %ss: %s", job_name, idx, timeout_seconds, step_name)

                if continue_on_error:
                    console.print("[yellow]    ⚠ Continuing despite timeout[/yellow]")
                    logger.warning("[%s] Continuing despite timeout (continue_on_error=true)", job_name)
                    break  # treat as handled and move to next step

                return JobResult(
                    name=job_name,
                    status="failed",
                    duration_s=time.time() - job_start,
                    error=f"timeout after {timeout_seconds}s",
                )

            # Print & log output
            if last_result.stdout:
                logger.info("[%s] stdout:\n%s", job_name, last_result.stdout.rstrip())
                console.print(last_result.stdout.rstrip())
            if last_result.stderr:
                logger.warning("[%s] stderr:\n%s", job_name, last_result.stderr.rstrip())
                console.print(f"[red]{last_result.stderr.rstrip()}[/red]")

            if last_result.returncode == 0:
                console.print("[green]    ✓ OK[/green]")
                logger.info("[%s] Step %s OK: %s", job_name, idx, step_name)
                break

            # Cancel code (130) is treated as canceled
            if last_result.returncode == 130 and cancel_flag.get("cancel", False):
                logger.warning("[%s] Step %s canceled: %s", job_name, idx, step_name)
                return JobResult(name=job_name, status="canceled", duration_s=time.time() - job_start)

            # Retry if allowed
            if attempt <= retries:
                next_attempt = attempt + 1
                console.print(
                    f"[yellow]    Retrying {step_name} (attempt {next_attempt}/{retries + 1})[/yellow]"
                )
                logger.warning(
                    "[%s] Retrying step %s (attempt %s/%s)",
                    job_name,
                    step_name,
                    next_attempt,
                    retries + 1,
                )
                if retry_delay_seconds > 0:
                    time.sleep(retry_delay_seconds)
                continue

            # No more retries — fail or continue_on_error
            console.print(f"[red]    ✖ Step failed (exit code {last_result.returncode})[/red]")
            logger.error(
                "[%s] Step %s FAILED: %s (exit code %s)",
                job_name,
                idx,
                step_name,
                last_result.returncode,
            )

            if continue_on_error:
                console.print("[yellow]    ⚠ Continuing despite failure[/yellow]")
                logger.warning("[%s] Continuing despite failure (continue_on_error=true)", job_name)
                break

            return JobResult(
                name=job_name,
                status="failed",
                duration_s=time.time() - job_start,
                error=f"exit code {last_result.returncode}",
            )

    logger.info("=== Job end: %s ===", job_name)
    return JobResult(name=job_name, status="success", duration_s=time.time() - job_start)


# ----------------------------
# CLI command
# ----------------------------

@run_app.command("pipeline")
def pipeline(
    pipeline_file: Path = typer.Argument(..., exists=True),
    dry_run: bool = typer.Option(False, "--dry-run"),
    max_workers: int = typer.Option(1, "--max-workers", min=1),
    fail_fast: bool = typer.Option(False, "--fail-fast", help="Cancel remaining jobs after first failure."),
) -> None:
    run_id = new_run_id()
    logger, log_path = init_logging(run_id)

    logger.info("Starting pipeline run")
    logger.info("Run id: %s", run_id)
    logger.info("Log file: %s", log_path)
    logger.info("Pipeline file: %s", pipeline_file)
    logger.info("Dry run: %s", dry_run)
    logger.info("Max workers: %s", max_workers)
    logger.info("Fail fast: %s", fail_fast)

    try:
        data = yaml.safe_load(pipeline_file.read_text(encoding="utf-8")) or {}
    except Exception as e:
        console.print(f"[red]Failed to read YAML: {e}[/red]")
        raise typer.Exit(code=2)

    pipeline_name = data.get("name", "pipeline")
    console.print(f"[bold]Running[/bold] {pipeline_name} (run_id={run_id})")

    jobs: dict[str, Any] = data.get("jobs", {}) or {}
    if not jobs:
        console.print("[red]No jobs found in pipeline YAML.[/red]")
        logger.error("No jobs found in pipeline YAML")
        raise typer.Exit(code=2)

    # Build needs map
    needs_map: dict[str, list[str]] = {}
    for job_name, job in jobs.items():
        needs_map[job_name] = _as_list(job.get("needs"))

    # State
    statuses: dict[str, str] = {name: "pending" for name in jobs.keys()}
    results: dict[str, JobResult] = {}
    cancel_flag: dict[str, bool] = {"cancel": False}

    pending = set(jobs.keys())
    running: dict[Future[JobResult], str] = {}

    pipeline_start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        while pending or running:
            # If fail-fast triggered, stop scheduling new work.
            if fail_fast and cancel_flag["cancel"]:
                # Mark all still-pending jobs as canceled.
                for j in list(pending):
                    statuses[j] = "canceled"
                    results[j] = JobResult(name=j, status="canceled", duration_s=0.0, error="fail-fast cancel")
                    pending.remove(j)
                break

            # Mark blocked jobs as skipped (deps failed/skipped/canceled)
            for j in list(pending):
                if _job_is_blocked(j, needs_map, statuses):
                    statuses[j] = "skipped"
                    results[j] = JobResult(name=j, status="skipped", duration_s=0.0, error="dependency failed/blocked")
                    pending.remove(j)

            # Schedule ready jobs up to worker capacity
            capacity = max_workers - len(running)
            if capacity > 0 and not (fail_fast and cancel_flag["cancel"]):
                ready = [j for j in pending if _job_can_start(j, needs_map, statuses)]
                # Deterministic order
                ready.sort()

                for j in ready[:capacity]:
                    statuses[j] = "running"
                    fut = pool.submit(
                        _run_job,
                        j,
                        jobs[j],
                        dry_run=dry_run,
                        logger=logger,
                        cancel_flag=cancel_flag,
                    )
                    running[fut] = j
                    pending.remove(j)

            if not running:
                # No running jobs and none schedulable: we're done
                break

            done, _ = wait(running.keys(), return_when=FIRST_COMPLETED)
            for fut in done:
                j = running.pop(fut)
                try:
                    res = fut.result()
                except Exception as e:
                    res = JobResult(name=j, status="failed", duration_s=0.0, error=str(e))

                results[j] = res
                statuses[j] = res.status

                # If fail-fast enabled, a failure triggers cancellation
                if fail_fast and res.status == "failed":
                    cancel_flag["cancel"] = True
                    logger.error("Fail-fast triggered by job failure: %s", j)

    total_time = time.time() - pipeline_start

    # Summary output
    console.print("\n[bold]Job summary:[/bold]")
    any_failed = False
    for name in sorted(results.keys()):
        r = results[name]
        if r.status == "success":
            console.print(f"[green]✓ {name} ({r.duration_s:.2f}s)[/green]")
        elif r.status == "skipped":
            console.print(f"[yellow]– {name} (skipped)[/yellow]")
        elif r.status == "canceled":
            console.print(f"[yellow]⨯ {name} (canceled)[/yellow]")
        else:
            any_failed = True
            msg = f" ({r.error})" if r.error else ""
            console.print(f"[red]✗ {name} (failed){msg}[/red]")

    console.print(f"\n[bold]Total pipeline time:[/bold] {total_time:.2f}s")

    if any_failed:
        console.print("[bold red]Pipeline failed.[/bold red]")
        logger.error("Pipeline failed")
        raise typer.Exit(code=1)

    # If fail-fast canceled jobs but no “failed” recorded (rare), treat as failure
    if fail_fast and any(r.status == "canceled" for r in results.values()):
        console.print("[bold yellow]Pipeline canceled (fail-fast).[/bold yellow]")
        logger.warning("Pipeline canceled (fail-fast)")
        raise typer.Exit(code=1)

    console.print("[bold green]Pipeline completed successfully.[/bold green]")
    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    app()
