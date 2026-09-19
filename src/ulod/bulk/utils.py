import logging
import os
import queue
import shutil
from datetime import datetime, timedelta
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path


def init_logger(log_directory: Path) -> tuple[logging.Logger, QueueListener]:
    root = logging.getLogger(f"crawlerLogger_{os.getpid()}")
    root.setLevel(logging.INFO)
    q = queue.Queue(-1)
    queue_handler = QueueHandler(q)
    if root.handlers:
        root.handlers.clear()

    old_dirs = sorted([d for d in os.listdir(log_directory.parent)], reverse=True)
    dirs_to_delete = old_dirs[3:] if len(old_dirs) > 3 else []

    for dir_to_delete in dirs_to_delete:
        dir_path = log_directory.parent.joinpath(dir_to_delete)
        shutil.rmtree(dir_path)

    if not root.handlers:
        logfile = log_directory.joinpath(f"{os.getpid()}.log")
        handler = RotatingFileHandler(logfile, mode="a", maxBytes=1024**3)
        log_formatter = logging.Formatter(
            "[%(asctime)s][%(process)d][%(threadName)s][%(levelname)s],%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(log_formatter)
        root.addHandler(queue_handler)

    listener = QueueListener(q, handler)
    return root, listener


def _format_duration(seconds: float) -> str:
    seconds = max(0.0, seconds)
    if seconds < 60:
        return f"{seconds:.2f} seconds"

    minutes, remaining_seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {remaining_seconds:.2f}s"

    hours, remaining_minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(remaining_minutes)}m {remaining_seconds:.2f}s"


def write_download_report(
    destination: Path,
    *,
    source: str,
    started_at: datetime,
    elapsed_seconds: float,
    total_documents: int,
    successful_downloads: int,
    retrieved_documents: int,
    skipped_documents: int = 0,
    output_format: str | None = None,
    metadata_source: str | None = None,
) -> Path:
    """Write a short, human-readable summary of a completed bulk download."""
    attempted_downloads = max(0, total_documents - skipped_documents)
    failed_downloads = max(0, attempted_downloads - successful_downloads)
    finished_at = started_at + timedelta(seconds=max(0.0, elapsed_seconds))
    success_rate = (
        successful_downloads / attempted_downloads * 100
        if attempted_downloads
        else 0.0
    )

    metrics = [
        ("Source", source),
        ("Started at", started_at.isoformat(timespec="seconds")),
        ("Finished at", finished_at.isoformat(timespec="seconds")),
        ("Total time", _format_duration(elapsed_seconds)),
        ("Total documents", str(total_documents)),
        ("Download attempts", str(attempted_downloads)),
        ("Successful downloads", str(successful_downloads)),
        ("Failed downloads", str(failed_downloads)),
        ("Success rate", f"{success_rate:.1f}%"),
        ("Skipped existing documents", str(skipped_documents)),
        ("Documents available after run", str(retrieved_documents)),
    ]
    if output_format is not None:
        metrics.append(("Output format", output_format))
    if metadata_source is not None:
        metrics.append(("Metadata", metadata_source))

    rows = "\n".join(f"| {name} | {value} |" for name, value in metrics)
    report = (
        "# Bulk download report\n\n"
        "| Metric | Value |\n"
        "| --- | --- |\n"
        f"{rows}\n"
    )

    report_path = destination / "download_report.md"
    report_path.write_text(report, encoding="utf-8")
    return report_path
