"""Run: python examples/example_ckan_tabular.py"""

import os

from tabular_filter import tabular_files
from ulod import DownloadConfig, download_catalog

MAX_DATASETS = 50  # Packages to inspect; a package can contain several files.
MAX_DOWNLOADS = 10  # Maximum matching files selected, including existing files.
MAX_WORKERS = 2  # Concurrent download threads, not separate processes.
FORMATS = {"csv", "tsv", "xls", "xlsx", "parquet"}


def main():
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


if __name__ == "__main__":
    main()
