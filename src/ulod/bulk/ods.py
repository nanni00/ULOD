import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import wrapt_timeout_decorator
from tqdm import tqdm

from ulod.bulk.configurations import ODSDownloadConfig
from ulod.bulk.utils import init_logger
from ulod.ods.client import ODS

TIMEOUT_STREAM_TO_DISK = 60


def _safe_dataset_id(dataset_id: Any) -> str:
    return Path(
        str(dataset_id).replace("/", "-").replace("__", "--").replace(":", "-")
    ).stem


def _dataset_output_path(dataset_id: str, cfg: ODSDownloadConfig) -> Path:
    return (
        cfg.datasets_folder_path
        / f"{_safe_dataset_id(dataset_id)}.{cfg.download_format}"
    )


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
    dataset_id: str,
    cfg: ODSDownloadConfig,
    client: ODS,
):
    try:
        write_data_to_disk(
            client,
            dataset_id,
            cfg.datasets_folder_path,
            cfg.download_format,
        )
        return 1, []
    except Exception as e:
        return 0, [f"[DATASET:{dataset_id}][ERROR:{e}][TYPE:{type(e)}]"]


def download_tabular_resources(
    dataset_ids: list[str], cfg: ODSDownloadConfig, client: ODS
):
    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()
    logger.info(" BULK DOWNLOAD STARTED ".center(100, "="))
    logger.info(f"Total datasets identified: {len(dataset_ids)}")

    work = [[dataset_id] for dataset_id in dataset_ids]
    success_count = 0
    skipped_count = 0

    try:
        if not dataset_ids:
            return work, success_count

        datasets_to_download = dataset_ids

        if cfg.skip_existing_datasets:
            datasets_to_download = []
            for dataset_id in dataset_ids:
                output_path = _dataset_output_path(dataset_id, cfg)
                if output_path.exists():
                    skipped_count += 1
                    logger.info(f"[DATASET:{dataset_id}][SKIPPED EXISTING]")
                else:
                    datasets_to_download.append(dataset_id)

        if not datasets_to_download:
            return work, success_count

        max_workers = min(max(cfg.max_workers, 1), len(datasets_to_download))

        with ThreadPoolExecutor(max_workers) as executor:
            futures = {
                executor.submit(_executor_task, dataset_id, cfg, client)
                for dataset_id in datasets_to_download
            }

            for future in tqdm(
                as_completed(futures),
                desc="Datasets",
                total=len(datasets_to_download),
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
        logger.info(f"[TOTAL SKIPPED:{skipped_count}]")
        logger.info(" BULK DOWNLOAD COMPLETED ".center(100, "="))
        listener.stop()

    return work, success_count


def fetch_metadata(
    cfg: ODSDownloadConfig,
    client: ODS,
) -> tuple[list[str], list[dict]]:
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

    offset = cfg.from_dataset_index
    remote_total_count = initial.get("total_count", 0)
    available_count = max(0, remote_total_count - offset)
    total_count = (
        available_count
        if cfg.max_datasets == -1
        else min(available_count, cfg.max_datasets)
    )

    for _ in tqdm(
        range(0, total_count, cfg.batch_fetch_metadata),
        total=math.ceil(total_count / cfg.batch_fetch_metadata),
        desc="Metadata",
        disable=not cfg.verbose,
    ):
        current_limit = min(
            cfg.batch_fetch_metadata,
            total_count - len(dataset_ids),
        )
        if current_limit <= 0:
            break

        try:
            page = client.catalog_datasets(
                limit=current_limit,
                offset=offset,
            )
            offset += current_limit
        except Exception as e:
            print(f"Failed to fetch metadata at {offset=}: {e}")
            continue

        for dataset in page.get("results", []):
            dataset_id = dataset.get("dataset_id")

            if not dataset_id:
                print("Missing dataset_id")
                continue
                # Sanitise the dataset_id so it is safe to use as a filename
            safe_id = _safe_dataset_id(dataset_id)

            dataset_ids.append(safe_id)
            full_metadata.append(dataset)

    # Honour the overall cap
    dataset_ids = dataset_ids[:total_count]

    return dataset_ids, full_metadata


def filter_retrieved_metadata(metadata: list[dict], cfg: ODSDownloadConfig):
    retrieved_metadata = [
        dataset
        for dataset in metadata
        if dataset.get("dataset_id")
        and _dataset_output_path(dataset["dataset_id"], cfg).exists()
    ]

    output_path = cfg.metadata_path.parent / "metadata_retrieved_only.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as file:
        json.dump(retrieved_metadata, file, indent=4)
    return retrieved_metadata


def ods_download_datasets(cfg: ODSDownloadConfig, client: ODS):
    cfg.log_folder_path = cfg.download_destination.joinpath(
        "log", "download", time.strftime("%y%m%d_%H_%M_%S")
    )
    cfg.log_folder_path.mkdir(parents=True, exist_ok=True)

    cfg.datasets_folder_path = cfg.download_destination.joinpath(
        "datasets", cfg.download_format
    )
    cfg.datasets_folder_path.mkdir(parents=True, exist_ok=True)

    dataset_ids_path = cfg.download_destination.joinpath(
        "metadata",
        "dataset_ids.json",
    )
    cfg.metadata_path = cfg.download_destination.joinpath("metadata", "metadata.json")
    cfg.metadata_path.parent.mkdir(parents=True, exist_ok=True)

    if (
        dataset_ids_path.exists()
        and cfg.metadata_path.exists()
        and cfg.use_existing_metadata
    ):
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
    filter_retrieved_metadata(metadata, cfg)
