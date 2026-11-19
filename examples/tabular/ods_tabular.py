import os

from ulod import DownloadConfig, download_catalog
from ulod.bulk.filters import tabular_files

MAX_DATASETS = None  # Catalog entries to inspect, before format filtering.
MAX_DOWNLOADS = None  # Maximum CSV exports/files selected, including existing files.
MAX_RESOURCE_SIZE = None
MAX_WORKERS = 4  # Concurrent download threads within each dataset.
FORMATS = {"csv"}  # One CSV export per dataset, plus matching CSV attachments.


def paris():
    report = download_catalog(
        "ods",
        "https://opendata.paris.fr",
        DownloadConfig(
            "downloads/paris-tabular",
            formats=FORMATS,
            export_format="parquet",
            resource_filter=tabular_files(FORMATS, MAX_DOWNLOADS),
            max_datasets=MAX_DATASETS,
            max_resource_size=MAX_RESOURCE_SIZE,
            max_workers=MAX_WORKERS,
            api_key=os.getenv("ODS_API_KEY"),
            verbose=True
        ),
    )
    print(report)


def bologna():
    report = download_catalog(
        "ods",
        "https://opendata.comune.bologna.it",
        DownloadConfig(
            "downloads/bologna-tabular",
            formats=FORMATS,
            export_format="parquet",
            resource_filter=tabular_files(FORMATS, MAX_DOWNLOADS),
            max_datasets=MAX_DATASETS,
            max_resource_size=MAX_RESOURCE_SIZE,
            max_workers=MAX_WORKERS,
            api_key=os.getenv("ODS_API_KEY"),
            verbose=True
        ),
    )
    print(report)


if __name__ == "__main__":
    # paris()
    bologna()
