import argparse
import re
from typing import Any
from ulod.bulk.ckan import CKANDownloadConfig, ckan_download_datasets

from config import CKAN_DATA_PATH, headers, connection_pool_kw


def canada_filter_resource_metadata(metadata: dict[str, Any]) -> bool:
    if metadata["format"].lower() not in ["csv"]:
        return False

    if "language" in metadata and "en" not in metadata["language"]:
        return False

    return re.search(r"\(CSV.+\)", metadata["name"], re.DOTALL) is None


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
    return metadata["name"] and re.match(r"Contracts Finder", metadata["name"]) is None


def csv_only_filter_resource_metadata(metadata: dict[str, Any]) -> bool:
    return metadata["format"].lower() in ["csv"]


def csv_json_only_filter_resource_metadata(metadata: dict[str, Any]) -> bool:
    return metadata["format"].lower() in ["csv", "json"]


def canada():
    from ulod.ckan import Canada

    download_destination = CKAN_DATA_PATH / "canada"
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
        accept_zip_files=False,
        max_resource_size="64MB",
        max_workers=4,
        verbose=True,
    )

    ckan_download_datasets(cfg, client)


def uk():
    from ulod.ckan import UK

    download_destination = CKAN_DATA_PATH / "uk"
    download_destination.mkdir(parents=True, exist_ok=True)

    client = UK(headers=headers, connection_kw=connection_pool_kw)

    download_cfg = CKANDownloadConfig(
        download_destination,
        max_datasets=-1,
        from_dataset_index=0,
        batch_fetch_metadata=1000,
        filter_resource_metadata=_uk_filter_resource_metadata,
        download_format="parquet",
        http_headers=headers,
        save_with_resource_name=True,
        accept_zip_files=False,
        connection_pool_kw=connection_pool_kw,
        max_resource_size="1GB",
        skip_resource_statuses=(403,),
        max_workers=30,
        verbose=True,
    )

    ckan_download_datasets(download_cfg, client)


def nhs_uk():
    from ulod.ckan import NHSUK

    download_destination = CKAN_DATA_PATH / "nhs_uk"
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
        max_resource_size="64MB",
        max_workers=8,
        verbose=True,
    )

    ckan_download_datasets(download_cfg, client)


def modena():
    from ulod.ckan import Modena

    download_destination = CKAN_DATA_PATH / "modena"
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
        max_resource_size="128MB",
        max_workers=1,
        verbose=True,
    )

    ckan_download_datasets(cfg, client)


def ferrara():
    from ulod.ckan import Ferrara

    download_destination = CKAN_DATA_PATH / "ferrara"
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
        max_resource_size="128MB",
        max_workers=3,
        verbose=True,
    )

    ckan_download_datasets(cfg, client)


def milano():
    from ulod.ckan import Milano

    download_destination = CKAN_DATA_PATH / "milano"
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
        max_resource_size="256MB",
        max_workers=2,
        verbose=True,
    )

    ckan_download_datasets(cfg, client)


def madrid():
    from ulod.ckan import Madrid

    download_destination = CKAN_DATA_PATH / "madrid"
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
        max_resource_size="256MB",
        max_workers=2,
        verbose=True,
    )

    ckan_download_datasets(cfg, client)


def valencia():
    from ulod.ckan import Valencia

    download_destination = CKAN_DATA_PATH / "valencia"
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
        use_existing_metadata=False,
        accept_zip_files=False,
        connection_pool_kw=connection_pool_kw,
        max_resource_size="256MB",
        max_workers=4,
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

    args = parser.parse_args()

    match args.location:
        case "canada":
            func = canada
        case "uk":
            func = uk
        case "nhs-uk":
            func = nhs_uk
        case "modena":
            func = modena
        case "ferrara":
            func = ferrara
        case "milano":
            func = milano
        case "madrid":
            func = madrid
        case "valencia":
            func = valencia
    func()


if __name__ == "__main__":
    main()
