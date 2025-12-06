from typing import Any
import re
import os
from pathlib import Path

from fake_useragent import UserAgent

from ulod.ckan import CanadaCKAN, ModenaCKAN
from ulod.bulk.ckan import CKANDownloadConfig, ckan_download_datasets

ua = UserAgent()

headers = {"User-Agent": ua.firefox}


def canada_filter_resource_metadata(metadata: dict[str, Any]) -> bool:
    if metadata["format"].lower() not in ["csv"]:
        return False

    if "language" in metadata and "en" not in metadata["language"]:
        return False

    if re.search(r"\(CSV.+\)", metadata["name"], re.DOTALL) is not None:
        return False

    return True


def modena_filter_resource_metadata(metadata: dict[str, Any]) -> bool:
    if metadata["format"].lower() not in ["csv"]:
        return False
    return True


# IMPORTAN: CKAN configuration is pandas-oriented now :IMPORTANT
# Because pandas supports CSV files sep auto-detection, while
# polars not at this time.

# these options should be configured wrt the selected engine
read_dataset_kwargs = {
    "sep": None,
    "encoding": "latin-1",
    "encoding_errors": "ignore",
    "on_bad_lines": "skip",
    "engine": "python",
}

save_csv_kwargs = {"index": False}
# save_parquet_kwargs = {"index": False, "compression": "gzip"}

# END: ---------------------------------------------------- :END


def canada_sample():
    download_destination = Path(
        os.environ["DATADIR"], "open_data", "ckan", "canada_sample"
    )
    download_destination.mkdir(parents=True, exist_ok=True)

    canada = CanadaCKAN(headers=headers)

    cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=30,
        from_dataset_index=0,
        batch_fetch_metadata=10,
        filter_resource_metadata=canada_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        read_dataset_kwargs=read_dataset_kwargs,
        save_dataset_kwargs=save_csv_kwargs,
        accept_zip=True,
        engine="pandas",
        max_resource_size=2**25,
        max_process_workers=2,
        max_thread_workers=1,
        verbose=True,
    )

    ckan_download_datasets(cfg, canada)


def canada_all():
    download_destination = Path(os.environ["DATADIR"], "open_data", "ckan", "canada")
    download_destination.mkdir(parents=True, exist_ok=True)

    canada = CanadaCKAN()
    cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=20_000,
        from_dataset_index=0,
        batch_fetch_metadata=100,
        filter_resource_metadata=canada_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        read_dataset_kwargs=read_dataset_kwargs,
        save_dataset_kwargs=save_csv_kwargs,
        accept_zip=True,
        engine="pandas",
        max_resource_size=2**26,
        max_process_workers=4,
        max_thread_workers=4,
        verbose=True,
    )

    ckan_download_datasets(cfg, canada)


def modena_all():
    download_destination = Path(os.environ["DATADIR"], "open_data", "ckan", "modena")
    download_destination.mkdir(parents=True, exist_ok=True)

    modena = ModenaCKAN()

    cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=1000,
        from_dataset_index=0,
        batch_fetch_metadata=200,
        filter_resource_metadata=modena_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        read_dataset_kwargs=read_dataset_kwargs,
        save_dataset_kwargs=save_csv_kwargs,
        save_with_resource_name=True,
        engine="pandas",
        max_resource_size=2**26,
        max_process_workers=3,
        max_thread_workers=3,
        verbose=True,
    )

    ckan_download_datasets(cfg, modena)


def main():
    import sys

    args = sys.argv

    if args[1:] == ["canada", "all"]:
        canada_all()
    elif args[1:] == ["canada", "sample"]:
        canada_sample()
    elif args[1:] == ["modena", "all"]:
        modena_all()


if __name__ == "__main__":
    main()
