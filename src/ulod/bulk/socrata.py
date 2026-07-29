import json
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

from ulod.bulk.configurations import SocrataDownloadConfig
from ulod.bulk.utils import init_logger
from ulod.socrata.client import SocrataClient

warnings.filterwarnings("ignore")


def _executor_task(
    resource_metadata: dict,
    cfg: SocrataDownloadConfig,
    client: SocrataClient,
) -> tuple[int, list[str]]:
    try:
        dataset_id = resource_metadata["resource"]["id"]

        if cfg.download_strategy == "export" and cfg.download_format in {
            "csv",
            "parquet",
        }:
            client.download_dataset_export(
                dataset_id,
                cfg.datasets_folder_path,
                cfg.download_format,
                cfg.engine,
                chunk_size=cfg.export_chunk_size,
                limit=cfg.max_rows_per_dataset,
                parquet_compression_level=cfg.parquet_compression_level,
                keep_intermediate_files=cfg.keep_intermediate_files,
            )
        else:
            client.get_and_store_dataset(
                dataset_id,
                cfg.datasets_folder_path,
                cfg.download_format,
                cfg.engine,
                cfg.cast_datatypes,
                resource_metadata,
                limit=cfg.max_rows_per_dataset,
                batch_size=cfg.batch_rows_per_dataset,
                parquet_compression_level=cfg.parquet_compression_level,
            )
        return 1, []
    except Exception as e:
        dataset_id = resource_metadata.get("resource", {}).get("id", "<unknown>")
        return 0, [f"[DATASET:{dataset_id}][ERROR:{e}][TYPE:{type(e)}]"]


def download_tabular_resources(
    metadata: list[dict], cfg: SocrataDownloadConfig, client: SocrataClient
):
    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()
    logger.info(" BULK DOWNLOAD STARTED ".center(100, "="))

    work = [[resource_metadata] for resource_metadata in metadata]
    success_count = 0

    if not metadata:
        logger.info("[TOTAL DOWNLOADS:0/0]")
        logger.info(" BULK DOWNLOAD COMPLETED ".center(100, "="))
        listener.stop()
        return work, success_count

    max_workers = min(max(cfg.max_workers, 1), len(metadata))

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _executor_task,
                    resource_metadata,
                    cfg,
                    client,
                )
                for resource_metadata in metadata
            }

            for future in tqdm(
                as_completed(futures),
                desc="Datasets",
                total=len(metadata),
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

    if cfg.metadata_path.exists() and cfg.use_existing_metadata:
        with open(cfg.metadata_path, "r") as file:
            metadata = json.load(file)
    else:
        metadata = fetch_metadata(cfg, client)

        if cfg.save_metadata:
            with open(cfg.metadata_path, "w") as file:
                json.dump(metadata, file, indent=4)

    download_tabular_resources(metadata, cfg, client)
