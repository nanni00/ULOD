import argparse

from ulod.bulk.ods import ODSDownloadConfig, ods_download_datasets
from config import ODS_DATA_PATH, headers, connection_pool_kw


def bologna():
    from ulod.ods import Bologna

    download_destination = ODS_DATA_PATH / "bologna"
    download_destination.mkdir(parents=True, exist_ok=True)

    client = Bologna(headers=headers, connection_kw=connection_pool_kw)

    cfg = ODSDownloadConfig(
        download_destination,
        max_datasets=5000,
        from_dataset_index=0,
        batch_fetch_metadata=100,
        download_format="csv",
        http_headers=headers,
        save_with_resource_name=True,
        use_existing_metadata=False,
        connection_pool_kw=connection_pool_kw,
        # max_resource_size=2**27,
        max_workers=8,
        verbose=True,
    )

    ods_download_datasets(cfg, client)


def paris():
    from ulod.ods import Paris

    download_destination = ODS_DATA_PATH / "paris"
    download_destination.mkdir(parents=True, exist_ok=True)

    client = Paris(headers=headers, connection_kw=connection_pool_kw)

    cfg = ODSDownloadConfig(
        download_destination,
        max_datasets=1000,
        from_dataset_index=0,
        batch_fetch_metadata=100,
        download_format="csv",
        http_headers=headers,
        save_with_resource_name=True,
        connection_pool_kw=connection_pool_kw,
        use_existing_metadata=False,
        # max_resource_size=2**27,
        max_workers=10,
        verbose=True,
    )

    ods_download_datasets(cfg, client)


def main():
    parser = argparse.ArgumentParser(description="ODS bulk downloads examples CLI")

    # Define positional arguments
    parser.add_argument(
        "location",
        choices=["bologna", "paris"],
        help="Target location",
    )

    args = parser.parse_args()

    match args.location:
        case "bologna":
            func = bologna
        case "paris":
            func = paris
    func ()

if __name__ == "__main__":
    main()
