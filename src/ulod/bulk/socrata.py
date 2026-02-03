import json
import os
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

from tqdm import tqdm

from ulod.bulk.configurations import SocrataDownloadConfig
from ulod.bulk.utils import init_logger
from ulod.socrata.socrata import SocrataClient

warnings.filterwarnings("ignore")


def _executor_task(
    worker_id: int,
    metadata: list[dict],
    cfg: SocrataDownloadConfig,
    client: SocrataClient,
):
    if cfg.verbose:
        _pbar = tqdm(
            metadata,
            desc=f"Worker {worker_id}",
            leave=False,
            position=worker_id + 1,
        )

    success_count = 0
    errors = []

    for resource_metadata in metadata:
        try:
            dataset_id = resource_metadata["resource"]["id"]
            client.get_and_store_dataset(
                dataset_id,
                cfg.datasets_folder_path,
                cfg.download_format,
                cfg.engine,
                cfg.cast_datatypes,
                resource_metadata,
                limit=cfg.max_rows_per_dataset,
                batch_size=cfg.batch_rows_per_dataset,
            )
            success_count += 1
        except Exception as e:
            errors.append(f"[DATASET:{dataset_id}][ERROR:{e}][TYPE:{type(e)}]")
        finally:
            _pbar.update()
    return success_count


def download_tabular_resources(
    metadata: list[dict], cfg: SocrataDownloadConfig, client: SocrataClient
):
    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()
    logger.info(" BULK DOWNLOAD STARTED ".center(100, "="))

    max_workers = cfg.max_workers
    packages_per_worker = min(len(metadata) // max_workers, cfg.max_datasets_per_worker)

    work = [
        metadata[i : i + packages_per_worker]
        for i in range(0, len(metadata), packages_per_worker)
    ]

    try:
        with ThreadPoolExecutor(max_workers) as executor:
            futures = {
                executor.submit(
                    _executor_task,
                    worker_id % max_workers,
                    task,
                    cfg,
                    client,
                )
                for worker_id, task in enumerate(work)
            }

            success_count = 0

            for future in tqdm(
                as_completed(futures),
                desc="Datasets",
                total=len(futures),
                disable=not cfg.verbose,
            ):
                try:
                    n_success, errors = future.result()
                    success_count += n_success
                    for err in errors:
                        logger.error(err)
                except KeyError:
                    raise KeyError()
                except Exception as e:
                    logger.error(e)
    finally:
        logger.info(f"[TOTAL DOWNLOADS:{success_count}/{len(metadata)}]")
        logger.info(" BULK DOWNLOAD COMPLETED ".center(100, "="))
        listener.stop()
    return work, success_count


def fetch_metadata(cfg: SocrataDownloadConfig, client: SocrataClient):
    # usually Socrata domains contain less datasets than CKAN
    # and a single step can retrieve all the metadata at once
    metadata = client.get_datasets_information(cfg.max_datasets, cfg.from_dataset_index)

    return metadata


def socrata_download_datasets(cfg: SocrataDownloadConfig, client: SocrataClient):
    cfg.log_folder_path = (
        cfg.download_destination / "log" / "download" / time.strftime("%y%m%d_%H_%M_%S")
    )
    cfg.log_folder_path.mkdir(parents=True, exist_ok=True)

    cfg.datasets_folder_path = (
        cfg.download_destination / "datasets" / cfg.download_format
    )
    cfg.datasets_folder_path.mkdir(parents=True, exist_ok=True)

    cfg.metadata_path = cfg.download_destination / "metadata" / "metadata.json"
    cfg.metadata_path.parent.mkdir(parents=True, exist_ok=True)

    metadata = fetch_metadata(cfg, client)
    if cfg.save_metadata:
        with open(cfg.metadata_path, "w") as file:
            json.dump(metadata, file, indent=4)

    download_tabular_resources(metadata, cfg, client)
