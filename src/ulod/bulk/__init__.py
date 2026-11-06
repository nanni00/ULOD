"""Catalog downloads and the independent Data.gov bulk workflow."""

from importlib import import_module

from .configurations import DownloadConfig, USDownloadConfig

__all__ = [
    "DownloadConfig",
    "DownloadReport",
    "download_catalog",
    "USDownloadConfig",
    "datagov_download_datasets",
]


def __getattr__(name):
    if name in {"DownloadReport", "download_catalog"}:
        return getattr(import_module("ulod.bulk.catalog"), name)
    if name == "datagov_download_datasets":
        return import_module("ulod.bulk.datagov").datagov_download_datasets
    raise AttributeError(f"module 'ulod.bulk' has no attribute {name!r}")
