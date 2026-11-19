import os

from tabular_filter import tabular_files
from ulod import DownloadConfig, download_catalog

MAX_DATASETS = 50  # Catalog entries to inspect, before format filtering.
MAX_DOWNLOADS = 10  # Maximum CSV exports/files selected, including existing files.
MAX_WORKERS = 2  # Concurrent download threads within each dataset.
FORMATS = {"csv"}  # One CSV export per dataset, plus matching CSV attachments.


def main():
    report = download_catalog(
        "ods",
        "https://opendata.paris.fr",
        DownloadConfig(
            "downloads/paris-tabular",
            formats=FORMATS,
            export_format="csv",
            resource_filter=tabular_files(FORMATS, MAX_DOWNLOADS),
            max_datasets=MAX_DATASETS,
            max_workers=MAX_WORKERS,
            api_key=os.getenv("ODS_API_KEY"),
            verbose=True
        ),
    )
    print(report)


if __name__ == "__main__":
    main()
