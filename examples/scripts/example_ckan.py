import argparse
import os
import re
import sys
from pathlib import Path
from typing import Any

from fake_useragent import UserAgent

sys.path.append(str(Path(__file__, "..", "..", "..", "src").resolve()))

ua = UserAgent()

headers = {"User-Agent": ua.firefox}
connection_pool_kw = {"redirect": True, "timeout": 5}


def canada_filter_resource_metadata(metadata: dict[str, Any]) -> bool:
    if metadata["format"].lower() not in ["csv"]:
        return False

    if "language" in metadata and "en" not in metadata["language"]:
        return False

    if re.search(r"\(CSV.+\)", metadata["name"], re.DOTALL) is not None:
        return False

    return True


def _uk_filter_resource_metadata(metadata: dict[str, Any]) -> bool:
    if metadata["format"].lower() not in ["csv"]:
        return False
    # TODO: UK tarif datasets have many many many different
    # versions for the same data, thus is not easy to work
    # on them for OrQA aim. For now, we skip them. In future,
    # we might be interested into more fine-grained tasks
    # about selecting some specific version of a dataset.
    if metadata["name"] and re.match(r"v\d+", metadata["name"]):
        return False

    # NOTE: UK Contracts Finder datasets have a very bad formatting,
    # something that have maybe taken from XML files to CSV without a
    # proper handling. We can't work on them, since their informative
    # content is not easy to catch.
    if metadata["name"] and re.match(r"Contracts Finder", metadata["name"]):
        return False

    # related to the tarif datasets
    # if "ODS" in metadata["name"]:
    #     return False
    return True


def csv_only_filter_resource_metadata(metadata: dict[str, Any]) -> bool:
    if metadata["format"].lower() not in ["csv"]:
        return False
    return True


def csv_json_only_filter_resource_metadata(metadata: dict[str, Any]) -> bool:
    if metadata["format"].lower() not in ["csv", "json"]:
        return False
    return True


def canada_sample():
    from ulod.bulk.ckan import CKANDownloadConfig, ckan_download_datasets
    from ulod.ckan import Canada

    download_destination = Path(os.environ["DATADIR"], "ulod", "ckan", "canada_sample")
    download_destination.mkdir(parents=True, exist_ok=True)

    client = Canada(headers=headers, connection_kw=connection_pool_kw)

    cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=20,
        from_dataset_index=0,
        batch_fetch_metadata=10,
        filter_resource_metadata=canada_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        accept_zip_files=True,
        max_resource_size=2**25,
        max_workers=2,
        verbose=True,
    )

    ckan_download_datasets(cfg, client)


def canada_all():
    from ulod.bulk.ckan import CKANDownloadConfig, ckan_download_datasets
    from ulod.ckan import Canada

    download_destination = Path(os.environ["DATADIR"], "ulod", "ckan", "canada")
    download_destination.mkdir(parents=True, exist_ok=True)

    client = Canada(headers=headers, connection_kw=connection_pool_kw)
    cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=20_000,
        from_dataset_index=0,
        batch_fetch_metadata=100,
        filter_resource_metadata=canada_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        accept_zip_files=True,
        max_resource_size=2**26,
        max_workers=4,
        verbose=True,
    )

    ckan_download_datasets(cfg, client)


def uk_all():
    from ulod.bulk.ckan import CKANDownloadConfig, ckan_download_datasets
    from ulod.ckan import UK

    download_destination = Path(os.environ["DATADIR"], "ulod", "ckan", "uk")
    download_destination.mkdir(parents=True, exist_ok=True)

    client = UK(headers=headers, connection_kw=connection_pool_kw)

    download_cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=100_000,
        from_dataset_index=0,
        batch_fetch_metadata=1000,
        filter_resource_metadata=_uk_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        save_with_resource_name=True,
        accept_zip_files=False,
        connection_pool_kw=connection_pool_kw,
        max_resource_size=2**27,
        max_workers=4,
        verbose=True,
    )

    ckan_download_datasets(download_cfg, client)


def uk_sample():
    from ulod.bulk.ckan import CKANDownloadConfig, ckan_download_datasets
    from ulod.ckan import UK

    download_destination = Path(os.environ["DATADIR"], "ulod", "ckan", "uk-sample")
    download_destination.mkdir(parents=True, exist_ok=True)

    client = UK(headers=headers, connection_kw=connection_pool_kw)

    download_cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=2000,
        from_dataset_index=5000,
        batch_fetch_metadata=1000,
        filter_resource_metadata=_uk_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        save_with_resource_name=True,
        accept_zip_files=False,
        connection_pool_kw=connection_pool_kw,
        max_resource_size=2**27,
        max_workers=20,
        verbose=True,
    )

    ckan_download_datasets(download_cfg, client)


def nhs_uk_sample():
    from ulod.bulk.ckan import CKANDownloadConfig, ckan_download_datasets
    from ulod.ckan import NHSUK

    download_destination = Path(os.environ["DATADIR"], "ulod", "ckan", "nhs_uk")
    download_destination.mkdir(parents=True, exist_ok=True)

    connection_pool_kw.update({"timeout": 20})
    client = NHSUK(headers=headers, connection_kw=connection_pool_kw)

    download_cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=500,
        from_dataset_index=0,
        batch_fetch_metadata=100,
        filter_resource_metadata=_uk_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        save_with_resource_name=True,
        accept_zip_files=False,
        connection_pool_kw=connection_pool_kw,
        max_resource_size=2**26,
        max_workers=8,
        verbose=True,
    )

    ckan_download_datasets(download_cfg, client)


def modena_all():
    from ulod.bulk.ckan import CKANDownloadConfig, ckan_download_datasets
    from ulod.ckan import Modena

    download_destination = Path(os.environ["DATADIR"], "ulod", "ckan", "modena")
    download_destination.mkdir(parents=True, exist_ok=True)

    client = Modena(headers=headers, connection_kw=connection_pool_kw)

    cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=1000,
        from_dataset_index=0,
        batch_fetch_metadata=200,
        filter_resource_metadata=csv_only_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        save_with_resource_name=True,
        accept_zip_files=False,
        connection_pool_kw=connection_pool_kw,
        max_resource_size=2**27,
        max_workers=1,
        verbose=True,
    )

    ckan_download_datasets(cfg, client)


def ferrara_all():
    from ulod.bulk.ckan import CKANDownloadConfig, ckan_download_datasets
    from ulod.ckan import Ferrara

    download_destination = Path(os.environ["DATADIR"], "ulod", "ckan", "ferrara_v2")
    download_destination.mkdir(parents=True, exist_ok=True)

    client = Ferrara(headers=headers, connection_kw=connection_pool_kw)

    cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=1000,
        from_dataset_index=0,
        batch_fetch_metadata=200,
        filter_resource_metadata=csv_only_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        save_with_resource_name=True,
        accept_zip_files=False,
        connection_pool_kw=connection_pool_kw,
        max_resource_size=2**27,
        max_workers=3,
        verbose=True,
    )

    ckan_download_datasets(cfg, client)


def milano_all():
    from ulod.bulk.ckan import CKANDownloadConfig, ckan_download_datasets
    from ulod.ckan import Milano

    download_destination = Path(os.environ["DATADIR"], "ulod", "ckan", "milano")
    download_destination.mkdir(parents=True, exist_ok=True)

    client = Milano(headers=headers, connection_kw=connection_pool_kw)

    cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=100,
        from_dataset_index=0,
        batch_fetch_metadata=200,
        filter_resource_metadata=csv_only_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        save_with_resource_name=True,
        accept_zip_files=False,
        connection_pool_kw=connection_pool_kw,
        max_resource_size=2**28,
        max_workers=2,
        verbose=True,
    )

    ckan_download_datasets(cfg, client)


def madrid_all():
    from ulod.bulk.ckan import CKANDownloadConfig, ckan_download_datasets
    from ulod.ckan import Madrid

    download_destination = Path(os.environ["DATADIR"]) / "ulod" / "ckan" / "madrid"
    download_destination.mkdir(parents=True, exist_ok=True)

    client = Madrid(headers=headers, connection_kw=connection_pool_kw)

    cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=3000,
        from_dataset_index=0,
        batch_fetch_metadata=200,
        filter_resource_metadata=csv_only_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        save_with_resource_name=True,
        accept_zip_files=False,
        connection_pool_kw=connection_pool_kw,
        max_resource_size=2**28,
        max_workers=2,
        verbose=True,
    )

    ckan_download_datasets(cfg, client)


def valencia_all():
    from ulod.bulk.ckan import CKANDownloadConfig, ckan_download_datasets
    from ulod.ckan import Valencia

    download_destination = Path(os.environ["DATADIR"]) / "ulod" / "ckan" / "valencia"
    download_destination.mkdir(parents=True, exist_ok=True)

    client = Valencia(headers=headers, connection_kw=connection_pool_kw)

    cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=3000,
        from_dataset_index=0,
        batch_fetch_metadata=200,
        filter_resource_metadata=csv_only_filter_resource_metadata,
        download_format="csv",
        http_headers=headers,
        save_with_resource_name=True,
        accept_zip_files=False,
        connection_pool_kw=connection_pool_kw,
        max_resource_size=2**28,
        max_workers=2,
        verbose=True,
    )

    ckan_download_datasets(cfg, client)

def main():
    parser = argparse.ArgumentParser(description="CKAN bulk downloads examples CLI")

    # Define positional arguments
    parser.add_argument(
        "location",
        choices=["canada", "uk", "nhs-uk", "modena", "ferrara", "milano", "madrid", "valencia"],
        help="Target location",
    )
    parser.add_argument("mode", choices=["all", "sample"], help="Download mode")

    args = parser.parse_args()

    # Dispatch logic
    commands = {
        ("canada", "all"): canada_all,
        ("canada", "sample"): canada_sample,
        ("uk", "all"): uk_all,
        ("uk", "sample"): uk_sample,
        ("nhs-uk", "sample"): nhs_uk_sample,
        ("modena", "all"): modena_all,
        ("ferrara", "all"): ferrara_all,
        ("milano", "all"): milano_all,
        ("madrid", "all"): madrid_all,
        ("valencia", "all"): valencia_all,
    }

    func = commands.get((args.location, args.mode))

    if func:
        func()
    else:
        print(f"Error: The combination {args.location} {args.mode} is not supported.")


if __name__ == "__main__":
    main()
