from __future__ import annotations

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from tqdm import tqdm

from ulod.bulk.configurations import USDownloadConfig
from ulod.bulk.utils import init_logger
from ulod.countries.us import US
from ulod.sources.ckan import StreamResponse
from ulod.utils.exceptions import HTTPResourceError, TooLargeResourceError

SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")
RUN_DATE_FORMAT = "%d_%m_%y"
RUN_TIME_FORMAT = "%H_%M_%S"


@dataclass(frozen=True)
class DataGovResource:
    dataset_id: str
    dataset_modified: str | None
    distribution_id: str
    url: str
    format: str
    filename_extension: str

    @property
    def resource_id(self) -> str:
        return f"{self.dataset_id}__{self.distribution_id}"


def datagov_download_datasets(cfg: USDownloadConfig, client: US):
    run_date = _run_date()
    run_time = _run_time()

    cfg.run_root_path = cfg.download_destination / run_date
    cfg.datasets_folder_path = cfg.run_root_path / "datasets"
    cfg.log_folder_path = cfg.run_root_path / "log" / "download" / run_time
    cfg.metadata_path = cfg.run_root_path / "metadata" / f"metadata_{run_time}.json"
    cfg.manifest_path = cfg.run_root_path / "metadata" / f"manifest_{run_time}.json"

    cfg.datasets_folder_path.mkdir(parents=True, exist_ok=True)
    cfg.log_folder_path.mkdir(parents=True, exist_ok=True)
    cfg.metadata_path.parent.mkdir(parents=True, exist_ok=True)

    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()

    try:
        logger.info(" DATAGOV METADATA FETCH STARTED ".center(100, "="))
        metadata = fetch_metadata(cfg, client)
        previous_manifest = load_latest_manifest(
            cfg.download_destination, exclude=cfg.manifest_path
        )
        resources = collect_downloadable_resources(metadata, cfg, previous_manifest)

        if cfg.save_metadata:
            with open(cfg.metadata_path, "w") as file:
                json.dump(metadata, file, indent=4)

        save_manifest(cfg.manifest_path, metadata, resources)
        logger.info(f"Datasets fetched: {len(metadata)}")
        logger.info(f"Resources selected: {len(resources)}")
    finally:
        logger.info(" DATAGOV METADATA FETCH COMPLETED ".center(100, "="))
        _stop_logger(listener)

    return download_resources(resources, cfg, client)


def fetch_metadata(cfg: USDownloadConfig, client: US) -> list[dict[str, Any]]:
    metadata: list[dict[str, Any]] = []

    for dataset in tqdm(
        client.iter_datasets(
            q=cfg.q,
            sort=cfg.sort,
            per_page=cfg.per_page,
            after=cfg.after,
            org_slug=cfg.org_slug,
            org_type=cfg.org_type,
            keyword=cfg.keyword,
            spatial_filter=cfg.spatial_filter,
            spatial_geometry=cfg.spatial_geometry,
            spatial_within=cfg.spatial_within,
        ),
        total=cfg.max_datasets if cfg.max_datasets < int(1e9) else None,
        desc="Metadata",
        disable=not cfg.verbose,
    ):
        metadata.append(dataset)
        if len(metadata) >= cfg.max_datasets:
            break

    return metadata


def collect_downloadable_resources(
    metadata: list[dict[str, Any]],
    cfg: USDownloadConfig,
    previous_manifest: dict[str, Any] | None,
) -> list[DataGovResource]:
    previous_updates = _manifest_updates(previous_manifest)
    resources: list[DataGovResource] = []

    for dataset in metadata:
        dataset_id = _dataset_id(dataset)
        modified = _dataset_modified(dataset)

        if cfg.mode == "updated-only" and not _dataset_has_changed(
            dataset_id, modified, previous_updates
        ):
            continue

        for index, distribution in enumerate(_dataset_distributions(dataset)):
            resource = _resource_from_distribution(
                dataset_id,
                modified,
                distribution,
                index,
                cfg.formats,
            )
            if resource is not None:
                resources.append(resource)

    return resources


def download_resources(
    resources: list[DataGovResource], cfg: USDownloadConfig, client: US
):
    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()
    logger.info(" DATAGOV BULK DOWNLOAD STARTED ".center(100, "="))
    logger.info(f"Total resources identified: {len(resources)}")

    max_workers = max(1, cfg.max_workers)
    resources_per_task = max(1, (len(resources) + max_workers - 1) // max_workers)
    work = [
        resources[i : i + resources_per_task]
        for i in range(0, len(resources), resources_per_task)
    ]
    success_count = 0

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_executor_task, worker_id, task, cfg, client)
                for worker_id, task in enumerate(work)
            }

            for future in tqdm(
                as_completed(futures),
                desc="Resources",
                total=len(futures),
                disable=not cfg.verbose,
            ):
                try:
                    n_success, errors = future.result()
                    success_count += n_success
                    for err in errors:
                        logger.error(err)
                except KeyboardInterrupt as error:
                    raise error
                except Exception as error:
                    logger.error(str(error))
    finally:
        logger.info(f"[TOTAL DOWNLOADS:{success_count}/{len(resources)}]")
        logger.info(" DATAGOV BULK DOWNLOAD COMPLETED ".center(100, "="))
        _stop_logger(listener)

    return work, success_count


def _executor_task(
    worker_id: int,
    resources: list[DataGovResource],
    cfg: USDownloadConfig,
    client: US,
) -> tuple[int, list[str]]:
    success_count = 0
    errors = []

    if cfg.verbose:
        pbar = tqdm(
            resources,
            desc=f"Worker {worker_id}",
            leave=False,
            position=worker_id % max(cfg.max_workers, 1) + 1,
        )

    for resource in resources:
        response = None
        try:
            response = client.stream_request(resource.url)
            if response.status >= 400:
                raise HTTPResourceError(resource.url, response.status)

            content_length = response.headers.get("Content-Length")
            if (
                cfg.max_resource_size is not None
                and content_length
                and int(content_length) > cfg.max_resource_size
            ):
                raise TooLargeResourceError(
                    resource.url, int(content_length), cfg.max_resource_size
                )

            stream_data_to_disk(response, resource, cfg.datasets_folder_path, cfg.chunk_size)
            success_count += 1
        except Exception as error:
            errors.append(
                f"[RESOURCE:{resource.resource_id}][URL:{resource.url}]"
                f"[TYPE:{type(error)}][ERROR:{error}]"
            )
        finally:
            if response is not None:
                response.close()
            if cfg.verbose:
                pbar.update()

    return success_count, errors


def stream_data_to_disk(
    response: StreamResponse,
    resource: DataGovResource,
    destination: Path,
    chunk_size: int,
) -> None:
    destination = (
        destination
        / resource.format
        / f"{resource.resource_id}.{resource.filename_extension}"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)

    with open(destination, "wb") as file:
        for chunk in response.iter_content(chunk_size):
            if chunk:
                file.write(chunk)


def save_manifest(
    manifest_path: Path,
    metadata: list[dict[str, Any]],
    resources: list[DataGovResource],
) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "datasets": {
            _dataset_id(dataset): {"modified": _dataset_modified(dataset)}
            for dataset in metadata
        },
        "resources": [
            {
                "dataset_id": resource.dataset_id,
                "dataset_modified": resource.dataset_modified,
                "distribution_id": resource.distribution_id,
                "url": resource.url,
                "format": resource.format,
            }
            for resource in resources
        ],
    }
    with open(manifest_path, "w") as file:
        json.dump(payload, file, indent=4)


def load_latest_manifest(root: Path, exclude: Path | None = None) -> dict[str, Any] | None:
    manifests = [
        manifest
        for manifest in root.glob("*/metadata/manifest_*.json")
        if exclude is None or manifest.resolve() != exclude.resolve()
    ]
    if not manifests:
        return None

    latest = max(manifests, key=_manifest_sort_key)
    with open(latest, "r") as file:
        return json.load(file)


def _manifest_sort_key(path: Path) -> tuple[datetime, float]:
    try:
        timestamp = f"{path.parent.parent.name}_{path.stem.removeprefix('manifest_')}"
        return datetime.strptime(
            timestamp, f"{RUN_DATE_FORMAT}_{RUN_TIME_FORMAT}"
        ), path.stat().st_mtime
    except ValueError:
        return datetime.min, path.stat().st_mtime


def _stop_logger(listener) -> None:
    listener.stop()
    for handler in getattr(listener, "handlers", ()):
        handler.close()


def _run_date() -> str:
    return time.strftime(RUN_DATE_FORMAT)


def _run_time() -> str:
    return time.strftime(RUN_TIME_FORMAT)


def _manifest_updates(manifest: dict[str, Any] | None) -> dict[str, str | None]:
    if not manifest:
        return {}
    datasets = manifest.get("datasets", {})
    if not isinstance(datasets, dict):
        return {}

    updates: dict[str, str | None] = {}
    for dataset_id, dataset in datasets.items():
        if isinstance(dataset, dict):
            updates[dataset_id] = dataset.get("modified")
    return updates


def _dataset_has_changed(
    dataset_id: str,
    modified: str | None,
    previous_updates: dict[str, str | None],
) -> bool:
    if modified is None:
        return True
    return previous_updates.get(dataset_id) != modified


def _dataset_id(dataset: dict[str, Any]) -> str:
    dcat = dataset.get("dcat")
    dcat_id = dcat.get("identifier") if isinstance(dcat, dict) else None
    identifier = dataset.get("identifier") or dataset.get("slug") or dcat_id
    if not identifier:
        identifier = dataset.get("title") or dataset.get("name") or "dataset"
    return _sanitize(str(identifier))


def _dataset_modified(dataset: dict[str, Any]) -> str | None:
    dcat = dataset.get("dcat")
    if isinstance(dcat, dict):
        modified = dcat.get("modified")
        if modified is not None:
            return str(modified)
    return None


def _dataset_distributions(dataset: dict[str, Any]) -> list[dict[str, Any]]:
    dcat = dataset.get("dcat")
    if not isinstance(dcat, dict):
        return []
    distributions = dcat.get("distribution", [])
    if isinstance(distributions, dict):
        return [distributions]
    if not isinstance(distributions, list):
        return []
    return [item for item in distributions if isinstance(item, dict)]


def _resource_from_distribution(
    dataset_id: str,
    modified: str | None,
    distribution: dict[str, Any],
    index: int,
    formats: tuple[str, ...] | None,
) -> DataGovResource | None:
    download_url = _first_string(distribution.get("downloadURL"))
    access_url = _first_string(distribution.get("accessURL"))
    url = download_url or access_url
    if not url:
        return None

    requested_formats = _normalize_requested_formats(formats)
    detected_formats = _distribution_formats(distribution, url)

    matched_format = _match_format(detected_formats, requested_formats)
    if requested_formats and matched_format is None:
        return None
    if download_url is None and requested_formats and matched_format is None:
        return None

    normalized_format = matched_format or detected_formats[0] if detected_formats else "data"
    extension = _url_extension(url) or normalized_format
    distribution_id = _distribution_id(distribution, index)

    return DataGovResource(
        dataset_id=dataset_id,
        dataset_modified=modified,
        distribution_id=distribution_id,
        url=url,
        format=normalized_format,
        filename_extension=extension,
    )


def _distribution_formats(distribution: dict[str, Any], url: str) -> list[str]:
    formats = []
    for key in ("format", "mediaType"):
        normalized = _normalize_format_value(distribution.get(key))
        if normalized:
            formats.append(normalized)

    extension = _url_extension(url)
    if extension:
        formats.append(_normalize_format_value(extension) or extension)

    return list(dict.fromkeys(formats))


def _match_format(
    detected_formats: list[str],
    requested_formats: tuple[str, ...] | None,
) -> str | None:
    if not requested_formats:
        return None
    for detected in detected_formats:
        if detected in requested_formats:
            return detected
    return None


def _normalize_requested_formats(formats: tuple[str, ...] | None) -> tuple[str, ...] | None:
    if formats is None:
        return None
    return tuple(
        normalized
        for fmt in formats
        if (normalized := _normalize_format_value(fmt)) is not None
    )


def _normalize_format_value(value: Any) -> str | None:
    raw = _first_string(value)
    if not raw:
        return None
    normalized = raw.lower().strip().lstrip(".")
    if "csv" in normalized or "comma-separated" in normalized:
        return "csv"
    if "json" in normalized:
        return "json"
    return _sanitize(normalized)


def _url_extension(url: str) -> str | None:
    path = unquote(urlparse(url).path)
    suffix = Path(path).suffix.lower().lstrip(".")
    if suffix and suffix.isalnum() and len(suffix) <= 8:
        return suffix
    return None


def _distribution_id(distribution: dict[str, Any], index: int) -> str:
    identifier = (
        distribution.get("identifier")
        or distribution.get("@id")
        or distribution.get("title")
        or f"distribution-{index}"
    )
    return _sanitize(str(identifier))


def _first_string(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, list):
        for item in value:
            found = _first_string(item)
            if found:
                return found
    return None


def _sanitize(value: str) -> str:
    sanitized = SAFE_NAME.sub("_", value.strip()).strip("._")
    return sanitized or "unknown"
