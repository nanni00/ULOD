import os
from pathlib import Path

from dotenv import load_dotenv
from ulod.socrata import NYCSocrata
from ulod.bulk.socrata import (
    SocrataDownloadConfig,
    socrata_download_datasets,
)


def nyc_all(dotenv_path: Path):
    load_dotenv(dotenv_path)
    assert "SOCRATA_NYC_APP_TOKEN" in os.environ
    app_token = os.environ["SOCRATA_NYC_APP_TOKEN"]

    download_dst = Path(os.environ["DATADIR"], "open_data", "socrata", "nyc")
    download_dst.mkdir(parents=True, exist_ok=True)

    nyc = NYCSocrata(app_token)

    cfg = SocrataDownloadConfig(
        download_dst,
        max_datasets=5000,
        from_dataset_index=0,
        download_format="csv",
        engine="pandas",
        cast_datatypes=True,
        save_metadata=True,
        max_rows_per_dataset=100_000,
        max_process_workers=6,
        max_thread_workers=2,
        verbose=True,
    )

    socrata_download_datasets(cfg, nyc)


def nyc_sample(dotenv_path: Path):
    load_dotenv(dotenv_path)
    assert "SOCRATA_NYC_APP_TOKEN" in os.environ
    app_token = os.environ["SOCRATA_NYC_APP_TOKEN"]

    nyc = NYCSocrata(app_token)

    download_dst = Path(os.environ["DATADIR"], "open_data", "socrata", "nyc")
    download_dst.mkdir(parents=True, exist_ok=True)

    cfg = SocrataDownloadConfig(
        download_dst,
        max_datasets=10,
        from_dataset_index=0,
        download_format="csv",
        engine="pandas",
        cast_datatypes=True,
        max_rows_per_dataset=20,
        max_process_workers=1,
        max_thread_workers=1,
        verbose=True,
    )

    socrata_download_datasets(cfg, nyc)


if __name__ == "__main__":
    dotenv_path = Path(os.path.dirname(__file__), "..", "..", ".env")

    import sys

    args = sys.argv

    if args[1:] == ["NYC", "all"]:
        nyc_all(dotenv_path)
    elif args[1:] == ["NYC", "sample"]:
        nyc_sample(dotenv_path)
