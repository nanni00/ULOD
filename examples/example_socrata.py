import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

from ulod.bulk.socrata import (
    SocrataDownloadConfig,
    socrata_download_datasets,
)
from ulod.socrata import NYC

from config import SOCRATA_DATA_PATH

_dotenv_path = Path.cwd().parents[2] / ".env"
load_dotenv(_dotenv_path.resolve(), verbose=True)


def nyc():
    assert "SOCRATA_NYC_APP_TOKEN" in os.environ
    app_token = os.environ["SOCRATA_NYC_APP_TOKEN"]

    download_dst = SOCRATA_DATA_PATH / "nyc"
    download_dst.mkdir(parents=True, exist_ok=True)

    nyc = NYC(app_token)

    cfg = SocrataDownloadConfig(
        download_dst,
        max_datasets=5000,
        from_dataset_index=0,
        download_format="parquet",
        engine="polars",
        cast_datatypes=False,
        save_metadata=True,
        max_rows_per_dataset=-1,
        parquet_compression_level=15,
        max_workers=30,
        skip_existing_datasets=True,
        verbose=True,
    )

    socrata_download_datasets(cfg, nyc)


def main():
    parser = argparse.ArgumentParser(description="Socrata bulk downloads examples CLI")

    # Define positional arguments
    parser.add_argument(
        "location",
        choices=["nyc"],
        help="Target location",
    )
    parser.add_argument("mode", choices=["all", "sample"], help="Download mode")

    args = parser.parse_args()

    match args.location:
        case "nyc":
            func = nyc

    func()


if __name__ == "__main__":
    main()
