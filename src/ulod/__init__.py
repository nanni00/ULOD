"""Download open-data catalogs, metadata and native files locally."""

# from importlib import import_module
from . import bulk
from . import un
from . import wbo
from .bulk.catalog import download_catalog, DownloadReport, DownloadConfig

__all__ = ["DownloadConfig", "DownloadReport", "download_catalog", "bulk", "un", "wbo"]


# def __getattr__(name):
#     if name in {"DownloadConfig", "DownloadReport", "download_catalog"}:
#         return getattr(import_module("ulod.bulk"), name)
#     if name in {"base", "bulk", "un", "wbo", "us"}:
#         return import_module(f"ulod.{name}")
#     raise AttributeError(f"module 'ulod' has no attribute {name!r}")
