import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.append(str(Path(__file__, "..", "..", "..", "src").resolve()))

from ulod.bulk.socrata import (
    SocrataDownloadConfig,
    socrata_download_datasets,
)
from ulod.socrata import NYCSocrata

p = Path(__file__) / ".." / ".." / ".." / ".env"
load_dotenv(p.resolve(), verbose=True)


def nyc_all():
    assert "SOCRATA_NYC_APP_TOKEN" in os.environ
    app_token = os.environ["SOCRATA_NYC_APP_TOKEN"]

    download_dst = Path(os.environ["DATADIR"], "ulod", "socrata", "nyc")
    download_dst.mkdir(parents=True, exist_ok=True)

    nyc = NYCSocrata(app_token)

    cfg = SocrataDownloadConfig(
        download_dst,
        max_datasets=5000,
        from_dataset_index=0,
        download_format="csv",
        engine="polars",
        cast_datatypes=False,
        save_metadata=True,
        max_rows_per_dataset=1_000_000,
        max_workers=20,
        verbose=True,
    )

    socrata_download_datasets(cfg, nyc)


def nyc_sample():
    assert "SOCRATA_NYC_APP_TOKEN" in os.environ
    app_token = os.environ["SOCRATA_NYC_APP_TOKEN"]

    nyc = NYCSocrata(app_token)

    download_dst = Path(os.environ["DATADIR"], "ulod", "socrata", "nyc")
    download_dst.mkdir(parents=True, exist_ok=True)

    cfg = SocrataDownloadConfig(
        download_dst,
        max_datasets=10,
        from_dataset_index=0,
        download_format="csv",
        engine="pandas",
        cast_datatypes=True,
        max_rows_per_dataset=20,
        max_workers=1,
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

    # Dispatch logic
    commands = {
        ("nyc", "all"): nyc_all,
        ("nyc", "sample"): nyc_sample,
    }

    func = commands.get((args.location, args.mode))

    if func:
        func()
    else:
        print(f"Error: The combination {args.location} {args.mode} is not supported.")


if __name__ == "__main__":
    main()
