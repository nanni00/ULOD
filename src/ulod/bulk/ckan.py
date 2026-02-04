import wrapt_timeout_decorator
import os
import zipfile
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm
from urllib3 import HTTPResponse, PoolManager

from ulod.bulk.configurations import CKANDownloadConfig
from ulod.bulk.utils import init_logger
from ulod.ckan import CKAN
from ulod.utils.exceptions import (
    HTTPResourceError,
    TooLargeResourceError,
)

SEP = "__"
ZIPFILE_MAGIC_BYTES = b"\x50\x4b\x03\x04"
XLS_2003_MAGIC_BYTES = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"


def stream_zip_to_disk(
    response: HTTPResponse, initial_bytes: bytes, download_destination: Path
):
    raise NotImplementedError()


def unzip(zippath: Path):
    folder = zippath.parent / zippath.stem
    folder.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zippath, "r") as zip:
        zip.extractall(folder, [f for f in zip.namelist() if f.endswith(".csv")])


@wrapt_timeout_decorator.timeout(60)
def stream_data_to_disk(
    response: HTTPResponse,
    resource_id: str,
    destination: Path,
    format: str,
    chunk_size: int = 65536,
):
    destination = destination / f"{resource_id}.{format}"

    # We download each resource into a "download_format/" folder
    # In some cases, resources have names like city-council/2021/resource.csv
    # and if we try to write to that file, we have a error since the path
    # "city-council/2021" is not found. Thus, we first create it if needed.
    destination.parent.mkdir(parents=True, exist_ok=True)
    is_zip = False
    is_xls_2003 = False

    with open(destination, "wb") as file:
        # Read and write in chunks (e.g., 64KB at a time)
        for i, chunk in enumerate(response.stream(chunk_size)):
            if i == 0 and chunk[: len(ZIPFILE_MAGIC_BYTES)] == ZIPFILE_MAGIC_BYTES:
                is_zip = True
            elif i == 0 and chunk[: len(XLS_2003_MAGIC_BYTES)] == XLS_2003_MAGIC_BYTES:
                is_xls_2003 = True

            file.write(chunk)

    if is_zip:
        destination = destination.rename(destination.parent / f"{destination.stem}.zip")
        unzip(destination)
        os.remove(destination)

        try:
            os.rmdir(destination.parent / destination.stem)
        except OSError:
            pass
    elif is_xls_2003:
        destination = destination.rename(destination.parent / f"{destination.stem}.xls")


def _executor_task(
    worker_id: int,
    http: PoolManager,
    metadata: list[tuple[str, str]],
    cfg: CKANDownloadConfig,
    client: CKAN,
):
    errors = []
    success_count = 0
    if cfg.verbose:
        _pbar = tqdm(
            metadata,
            desc=f"Worker {worker_id}",
            leave=False,
            position=worker_id % cfg.max_workers + 1,
        )

    for resource_id, url in metadata:
        response = None
        try:
            response = http.request(
                "GET",
                url,
                preload_content=False,
                decode_content=False,
                **cfg.connection_pool_kw,
            )

            if response.status >= 400:
                raise HTTPResourceError(url, response.status)

            # Accept files with limited size
            content_length = response.headers.get("Content-Length")
            if content_length and int(content_length) > cfg.max_resource_size:
                raise TooLargeResourceError(
                    url, int(content_length), cfg.max_resource_size
                )

            stream_data_to_disk(
                response,  # ty: ignore
                resource_id,
                cfg.datasets_folder_path,
                cfg.download_format,
            )
            success_count += 1
        except Exception as e:
            errors.append(f"[TYPE:{type(e)}][error:{str(e)}][URL:{url}]")
        finally:
            if response is not None:
                response.release_conn()
            if cfg.verbose:
                _pbar.update()

    return success_count, errors


def download_tabular_resources(
    metadata: list[tuple[str, str]], cfg: CKANDownloadConfig, client: CKAN
):
    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()
    logger.info(" BULK DOWNLOAD STARTED ".center(100, "="))
    logger.info(f"Total resources identified: {len(metadata)}")

    max_workers = cfg.max_workers
    packages_per_task = len(metadata) // max_workers

    work = [
        metadata[i : i + packages_per_task]
        for i in range(0, len(metadata), packages_per_task)
    ]

    http = PoolManager(headers=cfg.http_headers)

    try:
        with ThreadPoolExecutor(max_workers) as executor:
            futures = {
                executor.submit(_executor_task, worker_id, http, task, cfg, client)
                for worker_id, task in enumerate(work)
            }

            success_count = 0

            for future in tqdm(
                as_completed(futures),
                desc="Resources",
                total=len(work),
                disable=not cfg.verbose,
            ):
                try:
                    n_success, errors = future.result()
                    success_count += n_success
                    for err in errors:
                        logger.error(err)
                except KeyboardInterrupt as e:
                    raise e
                except Exception as e:
                    logger.error(str(e))
    finally:
        http.clear()

        logger.info(f"[TOTAL DOWNLOADS:{success_count}/{len(metadata)}]")
        logger.info(" BULK DOWNLOAD COMPLETED ".center(100, "="))
        listener.stop()
    return work, success_count


def fetch_metadata(
    cfg: CKANDownloadConfig, client: CKAN
) -> tuple[list[tuple[str, str]], list[dict]]:
    """
    Return a list of tuples <resource_id, URL> given by the specified
    CKAN client. If passed, r
    esources are filterd through a user function,
    otherwise all identified resources are returned.
    """
    resource_ids_urls = []
    full_metadata = []

    # parameter names for CKAN package_search method
    # other functions may require offset and limit
    metadata = client.package_search(start=0, rows=0, **cfg.package_search_filters)
    if not metadata:
        raise ValueError("Failed to fetch metadata")

    # the total number of packages in the current CKAN domain
    # we will fetch all the available resources metadata from there
    packages_count = min(metadata["result"]["count"], cfg.max_datasets)

    start = cfg.from_dataset_index

    for interval in tqdm(
        range(0, packages_count, cfg.batch_fetch_metadata),
        total=packages_count // cfg.batch_fetch_metadata,
        desc="Metadata",
        disable=not cfg.verbose,
    ):
        try:
            metadata = client.package_search(
                start=start, rows=cfg.batch_fetch_metadata, **cfg.package_search_filters
            )
            start += cfg.batch_fetch_metadata
        except Exception as e:
            print(f"Failed to fetch metadata at {start=}: {e}")
            continue

        packages = metadata["result"]["results"]

        for package in packages:
            resources: list[dict] = package["resources"]
            package_resource_ids = []

            for resource in resources:
                if cfg.filter_resource_metadata and not cfg.filter_resource_metadata(
                    resource
                ):
                    continue

                url = resource["url"]
                resource_id = resource["id"]
                resource_name = resource["name"]

                # FIX: if any of these is None or an empty string,
                # continue since this can lead to problems
                if not url or not resource_id:
                    continue

                # clean these two values
                resource_id = resource_id.replace("/", "-").replace("__", "--")
                resource_name = (
                    resource_name.strip()
                    .replace(" ", "-")
                    .replace(":", "-")
                    .replace("__", "--")
                    if resource_name
                    else ""
                )

                package_resource_ids.append(resource_id)

                if cfg.save_with_resource_name and resource_name:
                    resource_id = f"{resource_name}{SEP}{resource_id}"

                # NOTE: in some canada records, the URL is partially
                # correct but needs to be extended with the base url
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
    cfg.log_folder_path = cfg.download_destination.joinpath(
        "log", "download", time.strftime("%y%m%d_%H_%M_%S")
    )
    cfg.log_folder_path.mkdir(parents=True, exist_ok=True)

    cfg.datasets_folder_path = cfg.download_destination.joinpath(
        "datasets", cfg.download_format
    )
    cfg.datasets_folder_path.mkdir(parents=True, exist_ok=True)

    rsc_url_path = cfg.download_destination.joinpath("metadata", "rsc_url.json")
    cfg.metadata_path = cfg.download_destination.joinpath("metadata", "metadata.json")
    cfg.metadata_path.parent.mkdir(parents=True, exist_ok=True)

    if rsc_url_path.exists():
        with open(rsc_url_path, "r") as file:
            rsc_url = json.load(file)
        with open(cfg.metadata_path, "r") as file:
            metadata = json.load(file)
    else:
        rsc_url, metadata = fetch_metadata(cfg, client)

    if cfg.save_metadata:
        with open(cfg.metadata_path, "w") as file:
            json.dump(metadata, file, indent=4)
        with open(rsc_url_path, "w") as file:
            json.dump(rsc_url, file, indent=4)

    download_tabular_resources(rsc_url, cfg, client)
