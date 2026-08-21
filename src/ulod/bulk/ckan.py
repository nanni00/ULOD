from __future__ import annotations

import json
import queue
import random
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import wrapt_timeout_decorator
from tqdm import tqdm

from ulod.bulk.configurations import CKANDownloadConfig
from ulod.bulk.utils import init_logger
from ulod.ckan.client import CKAN, StreamResponse
from ulod.utils.exceptions import HTTPResourceError, TooLargeResourceError

SEP = "__"
PACKAGE_RESOURCE_SEP = "___"
ZIPFILE_MAGIC_BYTES = b"\x50\x4b\x03\x04"
XLS_2003_MAGIC_BYTES = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
TIMEOUT_STREAM_TO_DISK = 60
MAX_REQUEST_ATTEMPTS = 3
TRANSIENT_HTTP_STATUSES = {403, 429, 500, 502, 503, 504}
SUPPORTED_DOWNLOAD_FORMATS = {"csv", "parquet"}
SUPPORTED_SOURCE_FORMATS = {
    "csv",
    "tsv",
    "json",
    "jsonl",
    "ndjson",
    "xls",
    "xlsx",
    "parquet",
}

ResourceDownload = tuple[str, str, str]


@dataclass(frozen=True)
class CKANRequestPolicy:
    request_delay_s: float = 0.0
    request_jitter_s: float = 0.0
    retry_backoff_base_s: float = 0.0
    cooldown_on_403_s: float = 0.0
    max_consecutive_403: int = 0
    session_warmup_url: str | None = None


@dataclass(frozen=True)
class RetryDecision:
    retry: bool
    delay_s: float = 0.0
    abort: bool = False
    reason: str = ""


class EdgeProtectionBlockedError(RuntimeError):
    pass


class RequestCoordinator:
    def __init__(
        self,
        policy: CKANRequestPolicy,
        sleep_fn: Callable[[float], None] = time.sleep,
        jitter_fn: Callable[[float, float], float] = random.uniform,
    ) -> None:
        self.policy = policy
        self._sleep = sleep_fn
        self._jitter = jitter_fn
        self._lock = threading.Lock()
        self._next_request_at = 0.0
        self._consecutive_403 = 0
        self._transient_failures = 0
        self._blocked_reason: str | None = None

    def wait_for_turn(self) -> None:
        with self._lock:
            if self._blocked_reason:
                raise EdgeProtectionBlockedError(self._blocked_reason)

            now = time.monotonic()
            wait_s = max(0.0, self._next_request_at - now)
            spacing = self.policy.request_delay_s
            if self.policy.request_jitter_s > 0:
                spacing += self._jitter(0.0, self.policy.request_jitter_s)
            self._next_request_at = max(now, self._next_request_at) + spacing

        if wait_s > 0:
            self._sleep(wait_s)

        with self._lock:
            if self._blocked_reason:
                raise EdgeProtectionBlockedError(self._blocked_reason)

    def register_status(self, status: int, attempt: int) -> RetryDecision:
        with self._lock:
            now = time.monotonic()

            if 200 <= status < 400:
                self._consecutive_403 = 0
                self._transient_failures = 0
                return RetryDecision(retry=False)

            if status == 403:
                self._consecutive_403 += 1
                self._transient_failures += 1

                if (
                    self.policy.max_consecutive_403 > 0
                    and self._consecutive_403 >= self.policy.max_consecutive_403
                ):
                    self._blocked_reason = (
                        "Stopping bulk download after "
                        f"{self._consecutive_403} consecutive HTTP 403 responses."
                    )
                    return RetryDecision(
                        retry=False,
                        abort=True,
                        reason=self._blocked_reason,
                    )

                if attempt < MAX_REQUEST_ATTEMPTS:
                    delay_s = self.policy.cooldown_on_403_s + self._backoff_delay()
                    self._next_request_at = max(self._next_request_at, now + delay_s)
                    return RetryDecision(retry=True, delay_s=delay_s)

                return RetryDecision(retry=False)

            if status == 429 or status >= 500:
                self._consecutive_403 = 0
                self._transient_failures += 1

                if attempt < MAX_REQUEST_ATTEMPTS:
                    delay_s = self._backoff_delay()
                    self._next_request_at = max(self._next_request_at, now + delay_s)
                    return RetryDecision(retry=True, delay_s=delay_s)

                return RetryDecision(retry=False)

            self._consecutive_403 = 0
            self._transient_failures = 0
            return RetryDecision(retry=False)

    def _backoff_delay(self) -> float:
        if self.policy.retry_backoff_base_s <= 0:
            return 0.0
        return self.policy.retry_backoff_base_s * (2 ** (self._transient_failures - 1))


def stream_zip_to_disk(
    response: StreamResponse, initial_bytes: bytes, download_destination: Path
):
    raise NotImplementedError("ZIP downloads are not implemented for CKAN resources")


def unzip(zippath: Path):
    raise NotImplementedError("ZIP extraction is not implemented for CKAN resources")


@wrapt_timeout_decorator.timeout(TIMEOUT_STREAM_TO_DISK)
def stream_response_to_file(
    response: StreamResponse,
    destination: Path,
    chunk_size: int = 65536,
) -> str | None:
    """Stream response bytes to an exact path without changing its suffix."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    detected_format = None

    try:
        with open(destination, "wb") as file:
            for i, chunk in enumerate(response.iter_content(chunk_size)):
                if not chunk:
                    continue
                if i == 0 and chunk[: len(ZIPFILE_MAGIC_BYTES)] == ZIPFILE_MAGIC_BYTES:
                    raise NotImplementedError(
                        "ZIP downloads are not implemented for CKAN resources"
                    )
                if (
                    i == 0
                    and chunk[: len(XLS_2003_MAGIC_BYTES)] == XLS_2003_MAGIC_BYTES
                ):
                    detected_format = "xls"
                file.write(chunk)
    except Exception:
        destination.unlink(missing_ok=True)
        raise

    return detected_format


def stream_data_to_disk(
    response: StreamResponse,
    resource_id: str,
    destination: Path,
    format: str,
    chunk_size: int = 65536,
) -> Path:
    """Backward-compatible wrapper that stores the raw response bytes."""
    file_name = destination / f"{resource_id}.{format}"
    detected_format = stream_response_to_file(response, file_name, chunk_size)
    if detected_format and _normalize_format(format) != detected_format:
        corrected_file_name = destination / f"{resource_id}.{detected_format}"
        file_name = file_name.replace(corrected_file_name)
    return file_name


def _normalize_format(value: Any) -> str:
    text = str(value or "").strip().lower().lstrip(".")
    aliases = {
        "application/json": "json",
        "application/ld+json": "json",
        "application/vnd.ms-excel": "xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
        "application/parquet": "parquet",
        "application/x-parquet": "parquet",
        "comma-separated-values": "csv",
        "text/csv": "csv",
        "text/tab-separated-values": "tsv",
    }
    if text in aliases:
        return aliases[text]
    if text in SUPPORTED_SOURCE_FORMATS:
        return text

    # CKAN format labels are often values such as "CSV", "CSV / ZIP", or
    # "GeoJSON". Prefer an exact supported token when one is present.
    tokens = text.replace("/", " ").replace("-", " ").split()
    candidates = (
        "parquet",
        "xlsx",
        "xls",
        "ndjson",
        "jsonl",
        "json",
        "tsv",
        "csv",
    )
    for candidate in candidates:
        if candidate in tokens:
            return candidate
    return ""


def _infer_source_format(url: str, declared_format: Any = None) -> str:
    source_format = _normalize_format(declared_format)
    if source_format:
        return source_format

    suffix = Path(urlparse(url).path).suffix
    source_format = _normalize_format(suffix)
    # Most CKAN tabular-resource filters select CSV resources. Keeping CSV as
    # the fallback also makes old two-item rsc_url.json checkpoints compatible.
    return source_format or "csv"


def _normalize_resource_download(resource: list | tuple) -> ResourceDownload:
    if len(resource) < 2:
        raise ValueError(f"Invalid CKAN resource download entry: {resource!r}")

    resource_id = str(resource[0])
    url = str(resource[1])
    declared_format = resource[2] if len(resource) > 2 else None
    return resource_id, url, _infer_source_format(url, declared_format)


def _read_pandas(source_path: Path, source_format: str):
    import pandas as pd

    match source_format:
        case "csv":
            return pd.read_csv(source_path)
        case "tsv":
            return pd.read_csv(source_path, sep="\t")
        case "xls" | "xlsx":
            return pd.read_excel(source_path)
        case "jsonl" | "ndjson":
            return pd.read_json(source_path, lines=True)
        case "json":
            try:
                return pd.read_json(source_path)
            except ValueError:
                return pd.read_json(source_path, lines=True)
        case "parquet":
            return pd.read_parquet(source_path)
        case _:
            raise ValueError(f"Unsupported CKAN source format: {source_format}")


def _convert_with_pandas(
    source_path: Path,
    source_format: str,
    output_path: Path,
    output_format: str,
    parquet_compression_level: int | None,
) -> None:
    dataframe = _read_pandas(source_path, source_format)

    match output_format:
        case "csv":
            dataframe.to_csv(output_path, index=False)
        case "parquet":
            parquet_kwargs: dict[str, Any] = {
                "index": False,
                "compression": "zstd",
            }
            if parquet_compression_level is not None:
                parquet_kwargs["compression_level"] = parquet_compression_level
            dataframe.to_parquet(output_path, **parquet_kwargs)
        case _:
            raise ValueError(f"Unsupported CKAN download format: {output_format}")


def _convert_with_polars(
    source_path: Path,
    source_format: str,
    output_path: Path,
    output_format: str,
    parquet_compression_level: int | None,
) -> None:
    import polars as pl

    match source_format:
        case "csv":
            dataframe = pl.scan_csv(
                source_path,
                infer_schema_length=10_000,
                ignore_errors=True,
            )
        case "tsv":
            dataframe = pl.scan_csv(
                source_path,
                separator="\t",
                infer_schema_length=10_000,
                ignore_errors=True,
            )
        case "jsonl" | "ndjson":
            dataframe = pl.scan_ndjson(source_path)
        case "json":
            dataframe = pl.read_json(source_path).lazy()
        case "parquet":
            dataframe = pl.scan_parquet(source_path)
        case "xls" | "xlsx":
            # Polars does not provide a streaming Excel reader. Use pandas for
            # these formats while retaining the configured output behavior.
            _convert_with_pandas(
                source_path,
                source_format,
                output_path,
                output_format,
                parquet_compression_level,
            )
            return
        case _:
            raise ValueError(f"Unsupported CKAN source format: {source_format}")

    match output_format:
        case "csv":
            dataframe.sink_csv(output_path)
        case "parquet":
            dataframe.sink_parquet(
                output_path,
                compression_level=parquet_compression_level,
            )
        case _:
            raise ValueError(f"Unsupported CKAN download format: {output_format}")


def _convert_downloaded_resource(
    source_path: Path,
    source_format: str,
    output_path: Path,
    output_format: str,
    engine: str,
    parquet_compression_level: int | None,
) -> None:
    temporary_output_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    temporary_output_path.unlink(missing_ok=True)

    try:
        match engine:
            case "pandas":
                _convert_with_pandas(
                    source_path,
                    source_format,
                    temporary_output_path,
                    output_format,
                    parquet_compression_level,
                )
            case "polars":
                _convert_with_polars(
                    source_path,
                    source_format,
                    temporary_output_path,
                    output_format,
                    parquet_compression_level,
                )
            case _:
                raise ValueError(
                    f"Unsupported dataframe engine {engine!r}; use 'pandas' or 'polars'"
                )

        temporary_output_path.replace(output_path)
    finally:
        temporary_output_path.unlink(missing_ok=True)


def _resolve_request_policy(cfg: CKANDownloadConfig, client: CKAN) -> CKANRequestPolicy:
    defaults = client.default_bulk_download_policy()
    return CKANRequestPolicy(
        request_delay_s=_resolve_float(
            cfg.request_delay_s,
            defaults.get("request_delay_s"),
        ),
        request_jitter_s=_resolve_float(
            cfg.request_jitter_s,
            defaults.get("request_jitter_s"),
        ),
        retry_backoff_base_s=_resolve_float(
            cfg.retry_backoff_base_s, defaults.get("retry_backoff_base_s")
        ),
        cooldown_on_403_s=_resolve_float(
            cfg.cooldown_on_403_s, defaults.get("cooldown_on_403_s")
        ),
        max_consecutive_403=_resolve_int(
            cfg.max_consecutive_403, defaults.get("max_consecutive_403")
        ),
        session_warmup_url=cfg.session_warmup_url or defaults.get("session_warmup_url"),
    )


def _resolve_float(value: float | None, default: float | None) -> float:
    if value is not None:
        return max(0.0, float(value))
    if default is not None:
        return max(0.0, float(default))
    return 0.0


def _resolve_int(value: int | None, default: int | None) -> int:
    if value is not None:
        return max(0, int(value))
    if default is not None:
        return max(0, int(default))
    return 0


def _log_retry(
    logger,
    label: str,
    status: int,
    attempt: int,
    delay_s: float,
) -> None:
    logger.info(
        f"{label}: received HTTP {status}. "
        f"Retrying attempt {attempt + 1}/{MAX_REQUEST_ATTEMPTS} in {delay_s:.2f}s."
    )


def _request_json_with_retries(
    operation: Callable[[], dict[str, Any]],
    coordinator: RequestCoordinator,
    logger,
    label: str,
) -> dict[str, Any]:
    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        coordinator.wait_for_turn()
        try:
            response = operation()
            coordinator.register_status(200, attempt)
            return response
        except HTTPResourceError as error:
            decision = coordinator.register_status(error.status, attempt)
            if decision.abort:
                raise EdgeProtectionBlockedError(decision.reason) from error
            if decision.retry:
                _log_retry(logger, label, error.status, attempt, decision.delay_s)
                time.sleep(decision.delay_s)
                continue
            raise

    raise RuntimeError(f"{label}: exhausted metadata retries without a result.")


def _open_stream_with_retries(
    client: CKAN,
    url: str,
    coordinator: RequestCoordinator,
    logger,
    skip_statuses: tuple[int, ...] = (),
) -> StreamResponse:
    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        coordinator.wait_for_turn()
        response = client.stream_request(url)
        if response.status < 400:
            coordinator.register_status(response.status, attempt)
            return response

        response.close()

        if response.status in skip_statuses:
            logger.info(f"{url}: skipping HTTP {response.status} without retry.")
            raise HTTPResourceError(url, response.status)

        decision = coordinator.register_status(response.status, attempt)
        if decision.abort:
            raise EdgeProtectionBlockedError(decision.reason)
        if decision.retry:
            _log_retry(logger, url, response.status, attempt, decision.delay_s)
            time.sleep(decision.delay_s)
            continue
        raise HTTPResourceError(url, response.status)

    raise RuntimeError(f"{url}: exhausted download retries without a response.")


def _content_length_bytes(headers) -> int | None:
    content_length = headers.get("Content-Length")
    if not content_length:
        return None

    try:
        return int(content_length)
    except (TypeError, ValueError):
        return None


def _resource_output_path(resource_id: str, cfg: CKANDownloadConfig) -> Path:
    return (
        cfg.datasets_folder_path
        / f"{resource_id}.{_normalize_format(cfg.download_format)}"
    )


def _download_resource(
    resource_id: str,
    url: str,
    source_format: str,
    cfg: CKANDownloadConfig,
    client: CKAN,
    coordinator: RequestCoordinator,
    logger,
) -> None:
    output_format = _normalize_format(cfg.download_format)
    if output_format not in SUPPORTED_DOWNLOAD_FORMATS:
        raise ValueError(
            "CKAN bulk downloads support only csv and parquet output formats"
        )

    source_format = _infer_source_format(url, source_format)
    output_path = _resource_output_path(resource_id, cfg)
    keep_intermediate_files = bool(
        getattr(cfg, "keep_intermediate_files", False)
    )
    engine = str(getattr(cfg, "engine", "polars"))
    parquet_compression_level = getattr(
        cfg,
        "parquet_compression_level",
        None,
    )

    if source_format == output_format:
        intermediate_path = output_path
    else:
        if keep_intermediate_files:
            intermediate_name = f"{resource_id}.{source_format}"
        else:
            intermediate_name = f"{resource_id}.tmp.{source_format}"
        intermediate_path = cfg.datasets_folder_path / intermediate_name

    response = None
    try:
        response = _open_stream_with_retries(
            client,
            url,
            coordinator,
            logger,
            cfg.skip_resource_statuses,
        )

        content_length = _content_length_bytes(response.headers)
        if (
            cfg.max_resource_size is not None
            and content_length is not None
            and content_length > cfg.max_resource_size
        ):
            raise TooLargeResourceError(url, content_length, cfg.max_resource_size)

        detected_format = stream_response_to_file(response, intermediate_path)
    finally:
        if response is not None:
            response.close()

    if detected_format and detected_format != source_format:
        source_format = detected_format
        if keep_intermediate_files:
            corrected_name = f"{resource_id}.{source_format}"
        else:
            corrected_name = f"{resource_id}.tmp.{source_format}"
        corrected_intermediate_path = cfg.datasets_folder_path / corrected_name
        intermediate_path = intermediate_path.replace(corrected_intermediate_path)

    if source_format == output_format and intermediate_path == output_path:
        return

    if source_format == output_format:
        intermediate_path.replace(output_path)
        return

    try:
        _convert_downloaded_resource(
            intermediate_path,
            source_format,
            output_path,
            output_format,
            engine,
            parquet_compression_level,
        )
    finally:
        if not keep_intermediate_files:
            intermediate_path.unlink(missing_ok=True)


def _executor_task(
    worker_id: int,
    resource_queue: queue.Queue[ResourceDownload],
    cfg: CKANDownloadConfig,
    client: CKAN,
    coordinator: RequestCoordinator,
    logger,
    stop_event: threading.Event,
    progress_bar=None,
):
    errors = []
    success_count = 0

    while not stop_event.is_set():
        try:
            resource_id, url, source_format = resource_queue.get_nowait()
        except queue.Empty:
            break

        try:
            _download_resource(
                resource_id,
                url,
                source_format,
                cfg,
                client,
                coordinator,
                logger,
            )
            success_count += 1
        except EdgeProtectionBlockedError as error:
            stop_event.set()
            errors.append(
                f"[RESOURCE:{resource_id}][URL:{url}]"
                f"[TYPE:{type(error)}][ERROR:{error}]"
            )
            break
        except Exception as error:
            errors.append(
                f"[RESOURCE:{resource_id}][URL:{url}]"
                f"[TYPE:{type(error)}][ERROR:{error}]"
            )
        finally:
            resource_queue.task_done()
            if progress_bar is not None and hasattr(progress_bar, "update"):
                progress_bar.update()

    return success_count, errors


def download_tabular_resources(
    metadata: list[ResourceDownload] | list[list[str]],
    cfg: CKANDownloadConfig,
    client: CKAN,
    coordinator: RequestCoordinator,
):
    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()
    logger.info(" BULK DOWNLOAD STARTED ".center(100, "="))
    logger.info(f"Total resources identified: {len(metadata)}")

    max_workers = min(max(1, cfg.max_workers), max(1, len(metadata)))
    resource_queue: queue.Queue[ResourceDownload] = queue.Queue()
    for resource in metadata:
        resource_queue.put(_normalize_resource_download(resource))

    work = [metadata]
    success_count = 0
    stop_event = threading.Event()
    progress_bar = None

    try:
        if cfg.verbose:
            progress_bar = tqdm(total=len(metadata), desc="Resources")

        with ThreadPoolExecutor(max_workers) as executor:
            futures = {
                executor.submit(
                    _executor_task,
                    worker_id,
                    resource_queue,
                    cfg,
                    client,
                    coordinator,
                    logger,
                    stop_event,
                    progress_bar,
                )
                for worker_id in range(max_workers)
            }

            for future in as_completed(futures):
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
        if progress_bar is not None and hasattr(progress_bar, "close"):
            progress_bar.close()
        logger.info(f"[TOTAL DOWNLOADS:{success_count}/{len(metadata)}]")
        logger.info(" BULK DOWNLOAD COMPLETED ".center(100, "="))
        listener.stop()
    return work, success_count


def _metadata_checkpoint_path(cfg: CKANDownloadConfig) -> Path:
    return cfg.metadata_path.parent / "metadata_checkpoint.json"


def _load_metadata_checkpoint(
    cfg: CKANDownloadConfig,
) -> tuple[list[ResourceDownload], list[dict], int]:
    checkpoint_path = _metadata_checkpoint_path(cfg)
    if not checkpoint_path.exists():
        return [], [], cfg.from_dataset_index

    with open(checkpoint_path, "r") as file:
        checkpoint = json.load(file)

    resource_ids_urls = [
        _normalize_resource_download(item)
        for item in checkpoint.get("resource_ids_urls", [])
    ]
    full_metadata = checkpoint.get("full_metadata", [])
    start = checkpoint.get("next_start", cfg.from_dataset_index)
    return resource_ids_urls, full_metadata, start


def _save_metadata_checkpoint(
    cfg: CKANDownloadConfig,
    resource_ids_urls: list[ResourceDownload],
    full_metadata: list[dict],
    next_start: int,
) -> None:
    checkpoint_path = _metadata_checkpoint_path(cfg)
    payload = {
        "resource_ids_urls": resource_ids_urls,
        "full_metadata": full_metadata,
        "next_start": next_start,
    }
    with open(checkpoint_path, "w") as file:
        json.dump(payload, file, indent=4)


def _delete_metadata_checkpoint(cfg: CKANDownloadConfig) -> None:
    checkpoint_path = _metadata_checkpoint_path(cfg)
    if checkpoint_path.exists():
        checkpoint_path.unlink()


def _resource_limit(cfg: CKANDownloadConfig) -> int | None:
    return None if cfg.max_datasets == -1 else max(0, cfg.max_datasets)


def _trim_to_resource_limit(cfg: CKANDownloadConfig, resources: list) -> list:
    limit = _resource_limit(cfg)
    if limit is None:
        return resources
    return resources[:limit]


def _has_resource_capacity(cfg: CKANDownloadConfig, resource_count: int) -> bool:
    limit = _resource_limit(cfg)
    return limit is None or resource_count < limit


def _sanitize_resource_part(value: Any, *, replace_spaces: bool = False) -> str:
    text = str(value or "").strip()
    if replace_spaces:
        text = text.replace(" ", "-")
    return Path(text.replace("/", "-").replace("__", "--").replace(":", "-")).stem


def _resource_url(client: CKAN, url: str) -> str:
    if url.startswith("http"):
        return url
    return f"{client.base_url}/{url.lstrip('/')}"


def fetch_metadata(
    cfg: CKANDownloadConfig,
    client: CKAN,
    coordinator: RequestCoordinator,
    logger=None,
) -> tuple[list[ResourceDownload], list[dict]]:
    resource_ids_urls, full_metadata, start = _load_metadata_checkpoint(cfg)

    metadata = _request_json_with_retries(
        lambda: client.package_search(start=0, rows=0, **cfg.package_search_filters),
        coordinator,
        logger or _NullLogger(),
        "package_search(count)",
    )
    if not metadata:
        raise ValueError("Failed to fetch metadata")

    packages_count = metadata["result"]["count"]
    batches = range(start, packages_count, cfg.batch_fetch_metadata)

    for current_start in tqdm(
        batches,
        total=max(
            0,
            (packages_count - start + cfg.batch_fetch_metadata - 1)
            // cfg.batch_fetch_metadata,
        ),
        desc="Metadata",
        disable=not cfg.verbose,
    ):
        if not _has_resource_capacity(cfg, len(resource_ids_urls)):
            break

        metadata = _request_json_with_retries(
            lambda current_start=current_start: client.package_search(
                start=current_start,
                rows=cfg.batch_fetch_metadata,
                **cfg.package_search_filters,
            ),
            coordinator,
            logger or _NullLogger(),
            f"package_search(start={current_start})",
        )

        packages = metadata["result"]["results"]

        for package in packages:
            if not _has_resource_capacity(cfg, len(resource_ids_urls)):
                break

            resources: list[dict] = package.get("resources", [])
            selected_resources = []

            for resource in resources:
                if not _has_resource_capacity(cfg, len(resource_ids_urls)):
                    break

                if cfg.filter_resource_metadata and not cfg.filter_resource_metadata(
                    resource
                ):
                    continue

                url = resource.get("url")
                resource_id = resource.get("id")
                resource_name = resource.get("name")
                resource_format = resource.get("format")

                if not url or not resource_id:
                    continue

                resource_id = _sanitize_resource_part(resource_id)
                resource_name = _sanitize_resource_part(
                    resource_name,
                    replace_spaces=True,
                )

                if cfg.save_with_resource_name and resource_name:
                    resource_id = f"{resource_name}{SEP}{resource_id}"

                resource_url = _resource_url(client, url)
                resource_ids_urls.append(
                    (
                        resource_id,
                        resource_url,
                        _infer_source_format(resource_url, resource_format),
                    )
                )
                selected_resources.append(resource)

            if selected_resources:
                selected_package = dict(package)
                selected_package["resources"] = selected_resources
                selected_package["num_resources"] = len(selected_resources)
                full_metadata.append(selected_package)

        if cfg.use_existing_metadata or cfg.save_metadata:
            _save_metadata_checkpoint(
                cfg,
                resource_ids_urls,
                full_metadata,
                current_start + cfg.batch_fetch_metadata,
            )

    return resource_ids_urls, full_metadata


def _ckan_datasets_folder_path(cfg: CKANDownloadConfig) -> Path:
    datasets_folder_path = cfg.download_destination / "datasets" / cfg.download_format
    if datasets_folder_path.exists():
        return datasets_folder_path
    return cfg.datasets_folder_path


def _metadata_resource_file_names(
    resource: dict,
    package: dict,
    cfg: CKANDownloadConfig,
) -> tuple[str, ...]:
    extension = _normalize_format(cfg.download_format)
    resource_id = _sanitize_resource_part(resource.get("id"))
    if not resource_id:
        return ()

    if not cfg.save_with_resource_name:
        return (f"{resource_id}.{extension}",)

    names = []
    resource_name = _sanitize_resource_part(
        resource.get("name"),
        replace_spaces=True,
    )
    if resource_name:
        names.append(f"{resource_name}{SEP}{resource_id}.{extension}")

    package_id = _sanitize_resource_part(
        package.get("id") or package.get("name"),
        replace_spaces=True,
    )
    if package_id:
        names.append(f"{package_id}{PACKAGE_RESOURCE_SEP}{resource_id}.{extension}")

    if not names:
        names.append(f"{resource_id}.{extension}")
    return tuple(dict.fromkeys(names))


def filter_retrieved_metadata(metadata: list[dict], cfg: CKANDownloadConfig):
    retrieved_metadata = []
    files_by_name = {
        path.name
        for path in _ckan_datasets_folder_path(cfg).iterdir()
    }

    for package in metadata:
        retrieved_resources = [
            resource
            for resource in package.get("resources", [])
            if any(
                name in files_by_name
                for name in _metadata_resource_file_names(resource, package, cfg)
            )
        ]

        if retrieved_resources:
            retrieved_package = dict(package)
            retrieved_package["resources"] = retrieved_resources
            retrieved_package["num_resources"] = len(retrieved_resources)
            retrieved_metadata.append(retrieved_package)

    output_path = cfg.metadata_path.parent / "metadata_retrieved_only.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as file:
        json.dump(retrieved_metadata, file, indent=4)
    return retrieved_metadata


def rename_resource_name_files_with_package_id(
    cfg: CKANDownloadConfig,
    metadata: list[dict] | None = None,
) -> list[tuple[Path, Path]]:
    if metadata is None:
        metadata_path = cfg.metadata_path
        if not metadata_path.exists():
            metadata_path = cfg.download_destination / "metadata" / "metadata.json"
        with open(metadata_path, "r") as file:
            metadata = json.load(file)

    datasets_folder_path = _ckan_datasets_folder_path(cfg)

    extension = _normalize_format(cfg.download_format)
    files_by_name = {
        path.name: path
        for path in datasets_folder_path.iterdir()
    }
    renamed_files = []

    for package in metadata:
        package_id = _sanitize_resource_part(
            package.get("id") or package.get("name"),
            replace_spaces=True,
        )
        if not package_id:
            continue

        for resource in package.get("resources", []):
            resource_id = _sanitize_resource_part(resource.get("id"))
            resource_name = _sanitize_resource_part(
                resource.get("name"),
                replace_spaces=True,
            )
            if not resource_id or not resource_name:
                continue

            old_name = f"{resource_name}{SEP}{resource_id}.{extension}"
            old_path = files_by_name.get(old_name)
            if old_path is None:
                continue

            new_name = f"{package_id}{PACKAGE_RESOURCE_SEP}{resource_id}.{extension}"
            if new_name in files_by_name:
                continue
            new_path = datasets_folder_path / new_name

            old_path.rename(new_path)
            del files_by_name[old_name]
            files_by_name[new_name] = new_path
            renamed_files.append((old_path, new_path))

    return renamed_files


def ckan_download_datasets(cfg: CKANDownloadConfig, client: CKAN):
    # policy = _resolve_request_policy(cfg, client)

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

    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()
    # coordinator = RequestCoordinator(policy)

    try:
        try:
            # if policy.session_warmup_url:
            #     logger.info(f"Warming up CKAN session via {policy.session_warmup_url}")
            #     client.warmup_session(policy.session_warmup_url)

            if (
                rsc_url_path.exists()
                and cfg.metadata_path.exists()
                and cfg.use_existing_metadata
            ):
                with open(rsc_url_path, "r") as file:
                    rsc_url = json.load(file)
                with open(cfg.metadata_path, "r") as file:
                    metadata = json.load(file)
                rsc_url = _trim_to_resource_limit(cfg, rsc_url)
            else:
                rsc_url, metadata = fetch_metadata(cfg, client, coordinator, logger)

                if cfg.save_metadata:
                    with open(cfg.metadata_path, "w") as file:
                        json.dump(metadata, file, indent=4)
                    with open(rsc_url_path, "w") as file:
                        json.dump(rsc_url, file, indent=4)

            #     _delete_metadata_checkpoint(cfg)
        finally:
            listener.stop()

        # download_tabular_resources(rsc_url, cfg, client, coordinator)
        rename_resource_name_files_with_package_id(cfg, metadata)
        filter_retrieved_metadata(metadata, cfg)
    finally:
        client.close()


class _NullLogger:
    def info(self, *_args, **_kwargs) -> None:
        return None
