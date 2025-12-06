import time
import json
import os
import zipfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from typing import Any, Literal, Optional, Callable

import pandas as pd
from tqdm import tqdm
from urllib3 import PoolManager

from .utils import init_logger
from ..ckan import CKAN


class CKANDownloadConfig:
    def __init__(
        self,
        download_dst: Path,
        max_datasets: int = int(1e9),
        from_dataset_index: int = 0,
        batch_fetch_metadata: int = 1000,
        search_filters: dict = {},
        filter_resource_metadata: Optional[Callable] = None,
        download_format: Literal["csv", "parquet", "json"] = "csv",
        read_dataset_kwargs: dict = {},
        save_dataset_kwargs: dict = {},
        save_with_resource_name: bool = True,
        save_metadata: bool = True,
        accept_zip: bool = True,
        engine: Literal["pandas", "polars"] = "pandas",
        http_headers: dict[str, Any] = {},
        max_resource_size: Optional[int] = None,
        max_process_workers: Optional[int] = None,
        max_thread_workers: Optional[int] = None,
        verbose: bool = False,
    ) -> None:
        if not os.path.exists(download_dst):
            raise FileNotFoundError(f"Directory doesn't exist: {download_dst}")

        self.engine = engine
        if engine != "pandas":
            raise NotImplementedError()

        assert download_format in ["csv", "parquet", "json"], (
            f"Invalid download format: {download_format}"
        )
        self.download_format = download_format

        self.start = from_dataset_index
        self.rows = max_datasets

        self.batch = batch_fetch_metadata
        self.search_filters = search_filters

        self.filter_resource_metadata = filter_resource_metadata
        self.download_dst = download_dst
        self.datasets_folder_path: Path
        self.log_folder_path: Path
        self.metadata_path: Path
        self.save_metadata = save_metadata

        self.save_with_resource_name = save_with_resource_name

        self.accept_zip = accept_zip

        self.max_process_workers = max_process_workers if max_process_workers else 1
        self.max_thread_workers = max_thread_workers if max_thread_workers else 1
        self.max_resource_size = max_resource_size if max_resource_size else 2**20

        self.headers = http_headers

        self.read_dataset_kwargs = read_dataset_kwargs
        self.save_dataset_kwargs = save_dataset_kwargs

        self._pbars = {}
        self.verbose = verbose


def csv(
    url: str,
    data: bytes,
    resource_id: str,
    download_dst: Path,
    download_format: str,
    read_dataset_kwargs: dict = {},
    save_dataset_kwargs: dict = {},
):
    """
    Read a CSV file from the given input data. If the file name terminates
    with ".zip" of it is identified the string "DOCTYPE" in its first characters,
    then the data is recognized as not valid and terminate.

    The data will be read and stored with the specified options, if the download
    format is valid
    """
    # sometimes the data are encoded, sometimes not
    # and we do not want to start reading zip files here
    assert not url.endswith(".zip")
    assert "DOCTYPE" not in data[:100].decode("latin-1")

    download_dst = download_dst.joinpath(f"{resource_id}.{download_format}")

    if download_dst.parent.name != download_format:
        # this should mean that the file is contained into another folder
        download_dst.parent.mkdir(parents=True, exist_ok=True)

    try:
        df = pd.read_csv(data, **read_dataset_kwargs)
    except Exception:
        # in some cases data are encoded, thus we try again with a bytes stream
        df = pd.read_csv(BytesIO(data), **read_dataset_kwargs)

    match download_format:
        case "csv":
            df.to_csv(download_dst, **save_dataset_kwargs)
        case "parquet":
            df.to_parquet(download_dst, **save_dataset_kwargs)
        case "json":
            df.to_json(download_dst, **save_dataset_kwargs)


def zip(url: str, data: bytes, resource_id: str, download_dst: Path, *args):
    """
    Extract all the content from the given input data as it is a ZIP archive.
    A new directory is created on top of the download destination, and all the
    files and sub-folders from the archive are extracted there.
    """
    dst_folder = download_dst.joinpath(resource_id)
    dst_folder.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(BytesIO(data), "r") as zip:
        # we don't use zip.extractall() as suggested by the documentation
        # https://docs.python.org/3/library/zipfile.html#zipfile.ZipFile.extractall
        for filename in zip.namelist():
            zip.extract(filename, dst_folder)


def _thread_task(
    http: PoolManager, id: str, url: str, cfg: CKANDownloadConfig, client: CKAN
):
    # try to get the size of the file
    response = http.request("HEAD", url)

    content_length = response.headers.get("Content-Length")

    # Accept files with limited size
    if content_length and int(content_length) > cfg.max_resource_size:
        raise Exception(f"[URL:{url}][error:large content-length]")

    # download all the resource data at once
    data = http.request("GET", url).data

    # try each method to get the data
    download_methods = [csv]
    if cfg.accept_zip:
        download_methods += [zip]

    errors = []

    success = False
    for method in download_methods:
        try:
            method(
                url,
                data,
                id,
                cfg.datasets_folder_path,
                cfg.download_format,
                cfg.read_dataset_kwargs,
                cfg.save_dataset_kwargs,
            )
            success = True
            break
        except AssertionError:
            continue
        except Exception as e:
            errors.append(f"[method:{method.__name__}][error:{str(e)}]")

    if cfg.verbose:
        cfg._pbars[os.getpid()].update()

    if success:
        errors.clear()

    return success, errors


def _process_task(
    metadata: list[tuple[str, str]], cfg: CKANDownloadConfig, client: CKAN
):
    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()
    logger.info("[PROCESS STARTED]")

    if cfg.verbose:
        cfg._pbars[os.getpid()] = tqdm(
            metadata,
            desc=f"Process {os.getpid()}: ",
            leave=False,
            position=os.getpid() % cfg.max_process_workers + 1,
        )

    http = PoolManager(cfg.max_thread_workers, cfg.headers)

    success_count = 0
    start_t = time.time()
    with ThreadPoolExecutor(cfg.max_thread_workers) as executor:
        futures = {
            (
                executor.submit(_thread_task, http, resource_id, url, cfg, client),
                resource_id,
                url,
            )
            for resource_id, url in metadata
        }

        for future, resource_id, url in futures:
            try:
                success, errors = future.result(timeout=60)
                success_count += success

                for e in errors:
                    logger.error(f"[URL:{url}][TYPE:{type(e)}][MSG:{e}]")

            except Exception as e:
                logger.error(f"[URL:{url}][TYPE:{type(e)}][MSG:{str(e)}]")
    download_t = round(time.time() - start_t)

    if cfg.verbose:
        cfg._pbars[os.getpid()].close()

    logger.info(f"[TOTAL DOWNLOADS:{success_count}/{len(metadata)}]")
    logger.info(f"[TOTAL TIME: {download_t}s]")
    logger.info("[PROCESS COMPLETED]")
    listener.stop()
    return success_count


def download_tabular_resources(
    metadata: list[tuple[str, str]], cfg: CKANDownloadConfig, client: CKAN
):
    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()
    logger.info(" BULK DOWNLOAD STARTED ".center(100, "="))
    logger.info(f"Total resources identified: {len(metadata)}")

    packages_per_process = len(metadata) // cfg.max_process_workers

    work = [
        metadata[i : i + packages_per_process]
        for i in range(0, len(metadata), packages_per_process)
    ]

    with ProcessPoolExecutor(cfg.max_process_workers) as executor:
        futures = {executor.submit(_process_task, task, cfg, client) for task in work}

        success_count = 0

        for future in tqdm(
            futures, desc="Fetching resources: ", disable=not cfg.verbose
        ):
            try:
                success_count += future.result()
            except Exception as e:
                logger.error(str(e))

    logger.info(f"[TOTAL DOWNLOADS:{success_count}/{len(metadata)}]")
    logger.info(" BULK DOWNLOAD COMPLETED ".center(100, "="))
    listener.stop()
    return work, success_count


def fetch_metadata(
    cfg: CKANDownloadConfig, client: CKAN
) -> tuple[list[tuple[str, str]], list[dict]]:
    """
    Return a list of tuples <resource_id, URL> given by the specified
    CKAN client. If passed, resources are filterd through a user function,
    otherwise all identified resources are returned.
    """
    resource_ids_urls = []
    full_metadata = []

    # parameter names for CKAN package_search method
    # other functions may require offset and limit
    metadata = client.package_search(start=0, rows=0, **cfg.search_filters)

    # the total number of packages in the current CKAN domain
    # we will fetch all the available resources metadata from there
    packages_count = min(metadata["result"]["count"], cfg.rows)

    start = cfg.start

    for interval in tqdm(
        range(0, packages_count, cfg.batch),
        total=packages_count // cfg.batch,
        desc="Fetching resources metadata: ",
        disable=not cfg.verbose,
    ):
        metadata = client.package_search(start=start, rows=cfg.batch)
        start += cfg.batch

        packages = metadata["result"]["results"]

        for package in packages:
            resources: list[dict] = package["resources"]
            package_resource_ids = []

            for resource in resources:
                if cfg.filter_resource_metadata and not cfg.filter_resource_metadata(
                    resource
                ):
                    continue

                resource_id = resource["id"].replace("/", "_")
                package_resource_ids.append(resource_id)

                resource_name = (
                    resource["name"].strip().replace(" ", "-").replace(":", "-")
                )
                url = resource["url"]

                if cfg.save_with_resource_name:
                    resource_id = "{}::{}".format(resource_name, resource_id)

                if not url.startswith("http"):
                    url = f"{client.base_url}/{url}"
                resource_ids_urls.append((resource_id, url))

            if package_resource_ids:
                # add the package and its valid resources (if any)
                # to the list of full metadata
                package["resources"] = [
                    rsc
                    for rsc in resources
                    if rsc["id"].replace("/", "_") in package_resource_ids
                ]
                package["num_resources"] = len(package_resource_ids)

                full_metadata.append(package)

    return resource_ids_urls, full_metadata


def ckan_download_datasets(cfg: CKANDownloadConfig, client: CKAN):
    cfg.log_folder_path = cfg.download_dst.joinpath(
        "log", "download", time.strftime("%y%m%d_%H_%M_%S")
    )
    cfg.log_folder_path.mkdir(parents=True, exist_ok=True)

    cfg.datasets_folder_path = cfg.download_dst.joinpath(
        "datasets", cfg.download_format
    )
    cfg.datasets_folder_path.mkdir(parents=True, exist_ok=True)

    rsc_url_path = cfg.download_dst.joinpath("metadata", "rsc_url.json")
    cfg.metadata_path = cfg.download_dst.joinpath("metadata", "metadata.json")
    cfg.metadata_path.parent.mkdir(parents=True, exist_ok=True)

    rsc_url, metadata = fetch_metadata(cfg, client)
    if cfg.save_metadata:
        with open(cfg.metadata_path, "w") as file:
            json.dump(metadata, file, indent=4)
        with open(rsc_url_path, "w") as file:
            json.dump(rsc_url, file, indent=4)

    download_tabular_resources(rsc_url, cfg, client)
