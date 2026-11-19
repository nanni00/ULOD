import os

from ulod import DownloadConfig, download_catalog
from ulod.bulk.filters import tabular_files

MAX_DATASETS = 5000  # Packages to inspect; a package can contain several files.
MAX_DOWNLOADS = MAX_DATASETS  # Maximum matching files selected, including existing files.
MAX_WORKERS = 8  # Concurrent download threads, not separate processes.
FORMATS = {"csv", "tsv", "xls", "xlsx", "parquet"}


def canada():
    report = download_catalog(
        "ckan",
        "https://open.canada.ca/data",
        DownloadConfig(
            "downloads/canada-tabular",
            formats=FORMATS,
            resource_filter=tabular_files(FORMATS, MAX_DOWNLOADS),
            max_datasets=MAX_DATASETS,
            max_workers=MAX_WORKERS,
            api_key=os.getenv("CKAN_API_KEY"),
        ),
    )
    print(report)


def modena():
    report = download_catalog(
        "ckan",
        "https://opendata.comune.modena.it",
        DownloadConfig(
            "downloads/modena-tabular",
            formats=FORMATS,
            resource_filter=tabular_files(FORMATS, MAX_DOWNLOADS),
            max_datasets=MAX_DATASETS,
            max_workers=MAX_WORKERS,
            api_key=os.getenv("CKAN_API_KEY"),
            verbose=True
        ),
    )
    print(report)



if __name__ == "__main__":
    modena()
