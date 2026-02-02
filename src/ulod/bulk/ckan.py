import json
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
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

# def zip(
#     url: str,
#     data: bytes,
#     resource_id: str,
#     download_dst: Path,
#     download_format: str,
#     read_dataset_kwargs: dict = {},
#     save_dataset_kwargs: dict = {},
# ):
#     """
#     Extract all the content from the given input data as it is a ZIP archive.
#     A new directory is created on top of the download destination, and all the
#     files and sub-folders from the archive are extracted there.
#     """
#     dst_folder = download_dst.joinpath(resource_id)
#     dst_folder.mkdir(parents=True, exist_ok=True)
#     with zipfile.ZipFile(BytesIO(data), "r") as zip:
#         # we don't use zip.extractall() as suggested by the documentation
#         # https://docs.python.org/3/library/zipfile.html#zipfile.ZipFile.extractall
#         for filename in zip.namelist():
#             if not filename.endswith(".csv"):
#                 continue
#             elif "metadata" in filename.lower():
#                 zip.extract(filename, dst_folder)
#             else:
#                 with zip.open(filename) as csv_file:
#                     df = pd.read_csv(csv_file, **read_dataset_kwargs)
#                     _save_dataset(
#                         df,
#                         download_format,
#                         dst_folder.joinpath(filename),
#                         save_dataset_kwargs,
#                     )
#     try:
#         dst_folder.rmdir()
#     except OSError:
#         pass


def stream_zip_to_disk(
    response: HTTPResponse, initial_bytes: bytes, download_destination: Path
):
    raise NotImplementedError()


def stream_data_to_disk(
    response: HTTPResponse,
    resource_id: str,
    download_destination: Path,
    download_format: str,
    chunk_size: int = 65536,
):
    # initial_bytes = response.read(chunk_size)
    # zip_signature_bytes = b"\x50\x4b\x03\0x04"

    # if initial_bytes[:4] == zip_signature_bytes:
    #    stream_zip_to_disk(response, initial_bytes, download_destination)

    download_destination = download_destination.joinpath(
        f"{resource_id}.{download_format}"
    )

    # We download each resource into a "download_format/" folder
    # In some cases, resources have names like city-council/2021/resource.csv
    # and if we try to write to that file, we have a error since the path
    # "city-council/2021" is not found. Thus, we first create it if needed.
    download_destination.parent.mkdir(parents=True, exist_ok=True)

    with open(download_destination, "wb") as file:
        # Write also the initial bytes used for ZIP check
        # file.write(initial_bytes)

        # Read and write in chunks (e.g., 64KB at a time)
        for chunk in response.stream(chunk_size):
            file.write(chunk)
    response.release_conn()


def _thread_task(
    metadata: list[tuple[str, str]], cfg: CKANDownloadConfig, client: CKAN
):
    http = PoolManager(headers=cfg.http_headers)
    errors = []
    success_count = 0

    for resource_id, url in metadata:
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
            if cfg.verbose:
                cfg._pbars[os.getpid()].update()
    return success_count, errors


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
            # lock_args=(False,),
        )

    packages_per_thread = max(len(metadata) // cfg.max_thread_workers, 1)

    work = [
        metadata[i : i + packages_per_thread]
        for i in range(0, len(metadata), packages_per_thread)
    ]

    success_count = 0
    start_t = time.time()
    with ThreadPoolExecutor(cfg.max_thread_workers) as executor:
        futures = {executor.submit(_thread_task, task, cfg, client) for task in work}

        for future in as_completed(futures):
            try:
                success, errors = future.result(timeout=60)
                success_count += success

                for e in errors:
                    logger.error(e)
            except Exception as e:
                logger.error(f"[TYPE:{type(e)}][MSG:{str(e)}]")
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
            as_completed(futures), desc="Fetching resources: ", total=len(work),  disable=not cfg.verbose
        ):
            try:
                success_count += future.result()
            except KeyboardInterrupt as e:
                raise e
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
    metadata = client.package_search(start=0, rows=0, **cfg.package_search_filters)

    # the total number of packages in the current CKAN domain
    # we will fetch all the available resources metadata from there
    packages_count = min(metadata["result"]["count"], cfg.max_datasets)

    start = cfg.from_dataset_index

    for interval in tqdm(
        range(0, packages_count, cfg.batch_fetch_metadata),
        total=packages_count // cfg.batch_fetch_metadata,
        desc="Fetching resources metadata: ",
        disable=not cfg.verbose,
    ):
        try:
            metadata = client.package_search(start=start, rows=cfg.batch_fetch_metadata)
            start += cfg.batch_fetch_metadata
        except Exception as e:
            print(f"Failed fetch metadata at {start=}: {e}")
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
                resource_id = resource_id.replace("/", "_")
                resource_name = (
                    resource_name.strip().replace(" ", "-").replace(":", "-")
                    if resource_name
                    else ""
                )

                package_resource_ids.append(resource_id)

                if cfg.save_with_resource_name and resource_name:
                    resource_id = "{}::{}".format(resource_name, resource_id)

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
