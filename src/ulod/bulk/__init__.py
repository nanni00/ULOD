from importlib import import_module

from .configurations import CKANDownloadConfig, ODSDownloadConfig, SocrataDownloadConfig

__all__ = [
    "CKANDownloadConfig",
    "SocrataDownloadConfig",
    "ODSDownloadConfig",
    "ckan_download_datasets",
    "socrata_download_datasets",
    "ods_download_datasets",
]


def __getattr__(name: str):
    if name == "ckan_download_datasets":
        return import_module("ulod.bulk.ckan").ckan_download_datasets
    if name == "ods_download_datasets":
        return import_module("ulod.bulk.ods").ods_download_datasets
    if name == "socrata_download_datasets":
        return import_module("ulod.bulk.socrata").socrata_download_datasets
    raise AttributeError(f"module 'ulod.bulk' has no attribute {name!r}")
