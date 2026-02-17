from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from rich.logging import RichHandler


def new_run_id() -> str:
    """Create a readable unique-ish run id (UTC timestamp)."""
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def init_logging(
    run_id: str,
    *,
    log_level: str = "INFO",
    log_dir: Path = Path("logs"),
) -> tuple[logging.Logger, Path]:
    """
    Configure logging to:
      1) Pretty console output (Rich)
      2) A log file per run: logs/pipely-<run_id>.log
    Returns (logger, logfile_path).
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    log_dir.mkdir(parents=True, exist_ok=True)
    logfile = log_dir / f"pipely-{run_id}.log"

    logger = logging.getLogger("pipely")
    logger.setLevel(level)

    # Avoid duplicate handlers if you run multiple times in same Python process.
    logger.handlers.clear()
    logger.propagate = False

    # Console handler (pretty)
    console_handler = RichHandler(
        rich_tracebacks=True,
        show_time=True,
        show_level=True,
        show_path=False,
    )
    console_handler.setLevel(level)

    # File handler (detailed)
    file_handler = logging.FileHandler(logfile, encoding="utf-8")
    file_handler.setLevel(level)

    file_formatter = logging.Formatter(
        fmt="%(asctime)sZ | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info("Logging initialized")
    logger.info("Run id: %s", run_id)
    logger.info("Log file: %s", logfile)

    return logger, logfile
