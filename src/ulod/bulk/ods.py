import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import wrapt_timeout_decorator
from tqdm import tqdm

from ulod.bulk.configurations import ODSDownloadConfig
from ulod.bulk.utils import init_logger
from ulod.ods.client import ODS
from ulod.utils.exceptions import TooLargeResourceError

TIMEOUT_STREAM_TO_DISK = 60


# TODO: stream mode like for CKAN downloads?
# TODO: avoid huge downloads in memory?
@wrapt_timeout_decorator.timeout(TIMEOUT_STREAM_TO_DISK)
def write_data_to_disk(
    client: ODS,
    dataset_id: str,
    destination: Path,
    format: str,
):
    """Fetch dataset records from ODS and stream them to disk."""
    destination = destination / f"{dataset_id}.{format}"
    destination.parent.mkdir(parents=True, exist_ok=True)

    response = client.export_dataset_in_format(
        dataset_id=dataset_id,
        format=format,
    )

    with open(destination, "w") as file:
        file.write(response)


def _executor_task(
    worker_id: int,
    dataset_ids: list[str],
    cfg: ODSDownloadConfig,
    client: ODS,
):
    errors = []
    success_count = 0

    if cfg.verbose:
        _pbar = tqdm(
            dataset_ids,
            desc=f"Worker {worker_id}",
            leave=False,
            position=worker_id % cfg.max_workers + 1,
            disable=False,
        )

    for dataset_id in dataset_ids:
        try:
            write_data_to_disk(
                client,
                dataset_id,
                cfg.datasets_folder_path,
                cfg.download_format,
            )
            success_count += 1
        except Exception as e:
            errors.append(f"[TYPE:{type(e)}][error:{str(e)}][DATASET:{dataset_id}]")
        finally:
            if cfg.verbose:
                _pbar.update()

    return success_count, errors


def download_tabular_resources(
    dataset_ids: list[str], cfg: ODSDownloadConfig, client: ODS
):
    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()
    logger.info(" BULK DOWNLOAD STARTED ".center(100, "="))
    logger.info(f"Total datasets identified: {len(dataset_ids)}")

    max_workers = cfg.max_workers
    datasets_per_task = len(dataset_ids) // max_workers

    work = [
        dataset_ids[i : i + datasets_per_task]
        for i in range(0, len(dataset_ids), datasets_per_task)
    ]

    try:
        with ThreadPoolExecutor(max_workers) as executor:
            futures = {
                executor.submit(_executor_task, worker_id, task, cfg, client)
                for worker_id, task in enumerate(work)
            }

            success_count = 0

            for future in tqdm(
                as_completed(futures),
                desc="Datasets",
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
        logger.info(f"[TOTAL DOWNLOADS:{success_count}/{len(dataset_ids)}]")
        logger.info(" BULK DOWNLOAD COMPLETED ".center(100, "="))
        listener.stop()

    return work, success_count


def fetch_metadata(cfg: ODSDownloadConfig, client: ODS) -> tuple[list[str], list[dict]]:
    """
    Return a list of dataset IDs and their full metadata from the ODS catalog.

    ODS catalog/datasets returns a paginated response with the shape:
        {
            "total_count": <int>,
            "results": [
                {
                    "dataset_id": <str>,
                    "metas": { "default": { "title": <str>, ... } },
                    ...
                },
                ...
            ]
        }
    """
    dataset_ids = []
    full_metadata = []

    # First call with limit=0 to retrieve the total count
    initial = client.catalog_datasets(limit=0, offset=0)
    if not initial:
        raise ValueError("Failed to fetch metadata")

    total_count = min(initial.get("total_count", 0), cfg.max_datasets)
    offset = cfg.from_dataset_index

    for _ in tqdm(
        range(0, total_count, cfg.batch_fetch_metadata),
        total=total_count // cfg.batch_fetch_metadata,
        desc="Metadata",
        disable=not cfg.verbose,
    ):
        try:
            page = client.catalog_datasets(
                limit=cfg.batch_fetch_metadata,
                offset=offset,
            )
            offset += cfg.batch_fetch_metadata
        except Exception as e:
            print(f"Failed to fetch metadata at {offset=}: {e}")
            continue

        for dataset in page.get("results", []):
            dataset_id = dataset.get("dataset_id")

            if not dataset_id:
                print("Missing dataset_id")
                continue
                # Sanitise the dataset_id so it is safe to use as a filename
            safe_id = Path(
                dataset_id.replace("/", "-").replace("__", "--").replace(":", "-")
            ).stem

            dataset_ids.append(safe_id)
            full_metadata.append(dataset)

    # Honour the overall cap
    dataset_ids = dataset_ids[:total_count]

    return dataset_ids, full_metadata


def ods_download_datasets(cfg: ODSDownloadConfig, client: ODS):
    cfg.log_folder_path = cfg.download_destination.joinpath(
        "log", "download", time.strftime("%y%m%d_%H_%M_%S")
    )
    cfg.log_folder_path.mkdir(parents=True, exist_ok=True)

    cfg.datasets_folder_path = cfg.download_destination.joinpath(
        "datasets", cfg.download_format
    )
    cfg.datasets_folder_path.mkdir(parents=True, exist_ok=True)

    dataset_ids_path = cfg.download_destination.joinpath("metadata", "dataset_ids.json")
    cfg.metadata_path = cfg.download_destination.joinpath("metadata", "metadata.json")
    cfg.metadata_path.parent.mkdir(parents=True, exist_ok=True)

    if dataset_ids_path.exists() and cfg.use_existing_metadata:
        with open(dataset_ids_path, "r") as file:
            dataset_ids = json.load(file)
        with open(cfg.metadata_path, "r") as file:
            metadata = json.load(file)
    else:
        dataset_ids, metadata = fetch_metadata(cfg, client)

        if cfg.save_metadata:
            with open(cfg.metadata_path, "w") as file:
                json.dump(metadata, file, indent=4)
            with open(dataset_ids_path, "w") as file:
                json.dump(dataset_ids, file, indent=4)

    download_tabular_resources(dataset_ids, cfg, client)
