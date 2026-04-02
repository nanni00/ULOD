import argparse
import os
import sys
from pathlib import Path

from fake_useragent import UserAgent

sys.path.append(str(Path(__file__, "..", "..", "..", "src").resolve()))


ua = UserAgent()
headers = {"User-Agent": ua.firefox}
connection_pool_kw = {"redirect": True, "timeout": 5}


def bologna_all():
    from ulod.bulk.ods import ODSDownloadConfig, ods_download_datasets
    from ulod.ods.italy import BolognaODS

    download_destination = Path(os.environ["DATADIR"], "ulod", "ods", "bologna")
    download_destination.mkdir(parents=True, exist_ok=True)

    client = BolognaODS(headers=headers, connection_kw=connection_pool_kw)

    cfg = ODSDownloadConfig(
        download_destination,
        max_datasets=3,
        from_dataset_index=0,
        batch_fetch_metadata=200,
        download_format="csv",
        http_headers=headers,
        save_with_resource_name=True,
        connection_pool_kw=connection_pool_kw,
        # max_resource_size=2**27,
        max_workers=1,
        verbose=True,
    )

    ods_download_datasets(cfg, client)


def paris_all():
    from ulod.bulk.ods import ODSDownloadConfig, ods_download_datasets
    from ulod.ods.france import ParisODS

    download_destination = Path(os.environ["DATADIR"], "ulod", "ods", "paris")
    download_destination.mkdir(parents=True, exist_ok=True)

    client = ParisODS(headers=headers, connection_kw=connection_pool_kw)

    cfg = ODSDownloadConfig(
        download_destination,
        max_datasets=10,
        from_dataset_index=0,
        batch_fetch_metadata=100,
        download_format="csv",
        http_headers=headers,
        save_with_resource_name=True,
        connection_pool_kw=connection_pool_kw,
        use_existing_metadata=False,
        # max_resource_size=2**27,
        max_workers=1,
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
    parser.add_argument("mode", choices=["all", "sample"], help="Download mode")

    args = parser.parse_args()

    # Dispatch logic
    commands = {
        ("bologna", "all"): bologna_all,
        ("paris", "all"): paris_all,
    }

    func = commands.get((args.location, args.mode))

    if func:
        func()
    else:
        print(f"Error: The combination {args.location} {args.mode} is not supported.")


if __name__ == "__main__":
    main()
