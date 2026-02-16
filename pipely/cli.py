from __future__ import annotations

import shlex
import subprocess
from pathlib import Path

import typer
import yaml

from pipely.logging_utils import new_run_id, setup_logging

app = typer.Typer(no_args_is_help=True)

run_app = typer.Typer(no_args_is_help=True)
app.add_typer(run_app, name="run")


@run_app.command("pipeline")
def pipeline(
    pipeline_file: Path = typer.Argument(..., exists=True, readable=True),
    dry_run: bool = typer.Option(False, "--dry-run", help="Do not execute commands; only print what would run."),
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level: DEBUG, INFO, WARNING, ERROR"),
    log_dir: Path = typer.Option(Path("logs"), "--log-dir", help="Directory to store run logs."),
) -> None:
    # Create a unique run id and start logging immediately.
    run_id = new_run_id()
    logger, logfile = setup_logging(run_id, log_level=log_level, log_dir=log_dir)

    logger.info("Starting pipeline run")
    logger.info("Pipeline file: %s", pipeline_file)
    logger.info("Dry run: %s", dry_run)

    # Load YAML
    try:
        data = yaml.safe_load(pipeline_file.read_text(encoding="utf-8")) or {}
    except Exception as e:
        logger.exception("Failed to read/parse YAML: %s", e)
        raise typer.Exit(code=2)

    pipeline_name = data.get("name", "pipeline")
    logger.info("Pipeline name: %s", pipeline_name)

    jobs: dict = data.get("jobs", {})
    if not jobs:
        logger.error("No jobs found in pipeline YAML.")
        raise typer.Exit(code=2)

    # MVP runner: run jobs in the order they appear, steps in order.
    for job_name, job in jobs.items():
        logger.info("=== Job start: %s ===", job_name)

        steps = (job or {}).get("steps", [])
        if not steps:
            logger.warning("Job '%s' has no steps", job_name)
            continue

        for idx, step in enumerate(steps, start=1):
            step_name = (step or {}).get("name", f"step-{idx}")
            command = (step or {}).get("run")

            if not command:
                logger.warning("Skipping step '%s' (no 'run' command)", step_name)
                continue

            logger.info("Step %s start: %s", idx, step_name)
            logger.info("$ %s", command)

            if dry_run:
                logger.info("DRY RUN - not executing")
                continue

            # Safer default than shell=True: split into args and run directly.
            try:
                args = shlex.split(command)
            except ValueError as e:
                logger.error("Failed to parse command: %s", e)
                raise typer.Exit(code=2)

            result = subprocess.run(args, text=True, capture_output=True)

            if result.stdout:
                logger.info("stdout:\n%s", result.stdout.rstrip())
            if result.stderr:
                logger.warning("stderr:\n%s", result.stderr.rstrip())

            if result.returncode != 0:
                logger.error("Step failed (exit code %s)", result.returncode)
                logger.info("Log file saved at: %s", logfile)
                raise typer.Exit(code=result.returncode)

            logger.info("Step %s OK: %s", idx, step_name)

        logger.info("=== Job end: %s ===", job_name)

    logger.info("Pipeline completed successfully")
    logger.info("Log file saved at: %s", logfile)
