import logging
import os
import queue
import shutil
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path


def init_logger(log_directory: Path) -> tuple[logging.Logger, QueueListener]:
    root = logging.getLogger(f"crawlerLogger_{os.getpid()}")
    root.setLevel(logging.INFO)
    q = queue.Queue(-1)
    queue_handler = QueueHandler(q)
    if root.hasHandlers():
        root.handlers.clear()

    old_dirs = sorted([d for d in os.listdir(log_directory.parent)], reverse=True)
    dirs_to_delete = old_dirs[3:] if len(old_dirs) > 3 else []

    for dir_to_delete in dirs_to_delete:
        dir_path = log_directory.parent.joinpath(dir_to_delete)
        shutil.rmtree(dir_path)

    if not root.hasHandlers():
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
