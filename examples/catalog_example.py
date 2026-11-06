"""Shared command-line example for the three catalog sources."""

import argparse
import os

from ulod import DownloadConfig, download_catalog


def main(source, default_url):
    parser = argparse.ArgumentParser(description=f"Download a {source} catalog")
    parser.add_argument("destination")
    parser.add_argument("--url", default=default_url)
    parser.add_argument("--formats", nargs="*", default=None)
    parser.add_argument("--max-datasets", type=int)
    parser.add_argument("--max-resource-size", type=int, help="Maximum bytes per file")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--refresh-metadata", action="store_true")
    args = parser.parse_args()
    config = DownloadConfig(
        args.destination,
        formats=args.formats,
        max_datasets=args.max_datasets,
        max_resource_size=args.max_resource_size,
        max_workers=args.workers,
        refresh_metadata=args.refresh_metadata,
        api_key=os.getenv(f"{source.upper()}_API_KEY"),
        app_token=os.getenv("SOCRATA_APP_TOKEN") if source == "socrata" else None,
        access_token=os.getenv("SOCRATA_ACCESS_TOKEN") if source == "socrata" else None,
    )
    report = download_catalog(source, args.url, config)
    print(report)
    raise SystemExit(0 if report.success else 1)
