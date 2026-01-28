import json
import os
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from tqdm import tqdm

from ulod.socrata.socrata import SocrataClient
from ulod.bulk.configurations import SocrataDownloadConfig
from ulod.bulk.utils import init_logger

warnings.filterwarnings("ignore")


def _thread_task(metadata: dict, cfg: SocrataDownloadConfig, client: SocrataClient):
    dataset_id = metadata["resource"]["id"]

    client.get_and_store_dataset(
        dataset_id,
        cfg.datasets_folder_path,
        cfg.download_format,
        cfg.engine,
        cfg.cast_datatypes,
        metadata,
        limit=cfg.max_rows_per_dataset,
        batch_size=cfg.batch_rows_per_dataset,
    )

    if cfg.verbose:
        cfg._pbars[os.getpid()].update()

    return True


def _process_task(
    metadata: list[dict], cfg: SocrataDownloadConfig, client: SocrataClient
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

    success_count = 0
    start_t = time.time()
    with ThreadPoolExecutor(cfg.max_thread_workers) as executor:
        futures = {
            (executor.submit(_thread_task, task, cfg, client), task["resource"]["id"])
            for task in metadata
        }

        for future, dataset_id in futures:
            try:
                future.result(timeout=60)
                success_count += 1
            except Exception as e:
                e_str = str(e).replace("\n", " ")
                logger.error(
                    f"[DATASET_ID:{dataset_id}][MSG:{e_str}][TYPE(exc):{type(e)}]"
                )
    download_t = round(time.time() - start_t)

    if cfg.verbose:
        cfg._pbars[os.getpid()].close()

    logger.info(f"[TOTAL DOWNLOADS:{success_count}/{len(metadata)}]")
    logger.info(f"[TOTAL TIME: {download_t}s")
    logger.info("[PROCESS COMPLETED]")
    listener.stop()
    return success_count


def download_tabular_resources(
    metadata: list[dict], cfg: SocrataDownloadConfig, client: SocrataClient
):
    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()
    logger.info(" BULK DOWNLOAD STARTED ".center(100, "="))

    packages_per_process = len(metadata) // cfg.max_process_workers

    work = [
        metadata[i : i + packages_per_process]
        for i in range(0, len(metadata), packages_per_process)
    ]

    with ProcessPoolExecutor(cfg.max_process_workers) as executor:
        futures = {
            executor.submit(
                _process_task,
                task,
                cfg,
                client,
            )
            for task in work
        }

        success_count = 0

        for future in tqdm(
            futures, desc="Fetching resources: ", disable=not cfg.verbose
        ):
            try:
                success_count += future.result()
            except Exception as e:
                logger.error(e)

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
