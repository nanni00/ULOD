import os

from ulod import DownloadConfig, download_catalog
from ulod.bulk.filters import tabular_files

MAX_DATASETS = 50  # Catalog entries to inspect, before format filtering.
MAX_DOWNLOADS = 10  # Maximum CSV exports/files selected, including existing files.
MAX_WORKERS = 2  # Concurrent download threads within each dataset.
FORMATS = {"csv"}  # One CSV export per table, plus matching CSV attachments.


def nyc():
    report = download_catalog(
        "socrata",
        "https://data.cityofnewyork.us",
        DownloadConfig(
            "downloads/nyc-tabular",
            formats=FORMATS,
            export_format="csv",
            resource_filter=tabular_files(FORMATS, MAX_DOWNLOADS),
            max_datasets=MAX_DATASETS,
            max_workers=MAX_WORKERS,
            app_token=os.getenv("SOCRATA_APP_TOKEN"),
            access_token=os.getenv("SOCRATA_ACCESS_TOKEN"),
        ),
    )
    print(report)


if __name__ == "__main__":
    nyc()
