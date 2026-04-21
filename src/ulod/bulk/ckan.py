from __future__ import annotations

import json
import os
import random
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import wrapt_timeout_decorator
from tqdm import tqdm

from ulod.bulk.configurations import CKANDownloadConfig
from ulod.bulk.utils import init_logger
from ulod.sources import CKAN
from ulod.sources.ckan import StreamResponse
from ulod.utils.exceptions import HTTPResourceError, TooLargeResourceError

SEP = "__"
ZIPFILE_MAGIC_BYTES = b"\x50\x4b\x03\x04"
XLS_2003_MAGIC_BYTES = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
TIMEOUT_STREAM_TO_DISK = 60
MAX_REQUEST_ATTEMPTS = 3
TRANSIENT_HTTP_STATUSES = {403, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class CKANRequestPolicy:
    request_delay_s: float = 0.0
    request_jitter_s: float = 0.0
    retry_backoff_base_s: float = 0.0
    cooldown_on_403_s: float = 0.0
    max_consecutive_403: int = 0
    session_warmup_url: Optional[str] = None


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
        self._blocked_reason: Optional[str] = None

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
    raise NotImplementedError()


def unzip(zippath: Path):
    folder = zippath.parent / zippath.stem
    folder.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zippath, "r") as zip_file:
        zip_file.extractall(folder, [f for f in zip_file.namelist() if f.endswith(".csv")])


@wrapt_timeout_decorator.timeout(TIMEOUT_STREAM_TO_DISK)
def stream_data_to_disk(
    response: StreamResponse,
    resource_id: str,
    destination: Path,
    format: str,
    chunk_size: int = 65536,
):
    destination = destination / f"{resource_id}.{format}"

    destination.parent.mkdir(parents=True, exist_ok=True)
    is_zip = False
    is_xls_2003 = False

    with open(destination, "wb") as file:
        for i, chunk in enumerate(response.iter_content(chunk_size)):
            if not chunk:
                continue
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


def _resolve_request_policy(cfg: CKANDownloadConfig, client: CKAN) -> CKANRequestPolicy:
    defaults = client.default_bulk_download_policy()
    return CKANRequestPolicy(
        request_delay_s=_resolve_float(cfg.request_delay_s, defaults.get("request_delay_s")),
        request_jitter_s=_resolve_float(cfg.request_jitter_s, defaults.get("request_jitter_s")),
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


def _resolve_float(value: Optional[float], default: Optional[float]) -> float:
    if value is not None:
        return max(0.0, float(value))
    if default is not None:
        return max(0.0, float(default))
    return 0.0


def _resolve_int(value: Optional[int], default: Optional[int]) -> int:
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
) -> StreamResponse:
    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        coordinator.wait_for_turn()
        response = client.stream_request(url)
        decision = coordinator.register_status(response.status, attempt)
        if response.status < 400:
            return response

        response.close()

        if decision.abort:
            raise EdgeProtectionBlockedError(decision.reason)
        if decision.retry:
            _log_retry(logger, url, response.status, attempt, decision.delay_s)
            time.sleep(decision.delay_s)
            continue
        raise HTTPResourceError(url, response.status)

    raise RuntimeError(f"{url}: exhausted download retries without a response.")


def _executor_task(
    worker_id: int,
    metadata: list[tuple[str, str]],
    cfg: CKANDownloadConfig,
    client: CKAN,
    coordinator: RequestCoordinator,
    logger,
):
    errors = []
    success_count = 0
    if cfg.verbose:
        _pbar = tqdm(
            metadata,
            desc=f"Worker {worker_id}",
            leave=False,
            position=worker_id % max(cfg.max_workers, 1) + 1,
        )

    for resource_id, url in metadata:
        response = None
        try:
            response = _open_stream_with_retries(client, url, coordinator, logger)

            content_length = response.headers.get("Content-Length")
            if content_length and int(content_length) > cfg.max_resource_size:
                raise TooLargeResourceError(
                    url, int(content_length), cfg.max_resource_size
                )

            stream_data_to_disk(
                response,
                resource_id,
                cfg.datasets_folder_path,
                cfg.download_format,
            )
            success_count += 1
        except EdgeProtectionBlockedError as error:
            errors.append(str(error))
            break
        except Exception as error:
            errors.append(f"[TYPE:{type(error)}][error:{str(error)}][URL:{url}]")
        finally:
            if response is not None:
                response.close()
            if cfg.verbose:
                _pbar.update()

    return success_count, errors


def download_tabular_resources(
    metadata: list[tuple[str, str]],
    cfg: CKANDownloadConfig,
    client: CKAN,
    coordinator: RequestCoordinator,
):
    logger, listener = init_logger(cfg.log_folder_path)
    listener.start()
    logger.info(" BULK DOWNLOAD STARTED ".center(100, "="))
    logger.info(f"Total resources identified: {len(metadata)}")

    max_workers = max(1, cfg.max_workers)
    packages_per_task = max(1, (len(metadata) + max_workers - 1) // max_workers)

    work = [
        metadata[i : i + packages_per_task]
        for i in range(0, len(metadata), packages_per_task)
    ]
    success_count = 0

    try:
        with ThreadPoolExecutor(max_workers) as executor:
            futures = {
                executor.submit(
                    _executor_task,
                    worker_id,
                    task,
                    cfg,
                    client,
                    coordinator,
                    logger,
                )
                for worker_id, task in enumerate(work)
            }

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
                except KeyboardInterrupt as error:
                    raise error
                except Exception as error:
                    logger.error(str(error))
    finally:
        logger.info(f"[TOTAL DOWNLOADS:{success_count}/{len(metadata)}]")
        logger.info(" BULK DOWNLOAD COMPLETED ".center(100, "="))
        listener.stop()
    return work, success_count


def _metadata_checkpoint_path(cfg: CKANDownloadConfig) -> Path:
    return cfg.metadata_path.parent / "metadata_checkpoint.json"


def _load_metadata_checkpoint(
    cfg: CKANDownloadConfig,
) -> tuple[list[tuple[str, str]], list[dict], int]:
    checkpoint_path = _metadata_checkpoint_path(cfg)
    if not checkpoint_path.exists():
        return [], [], cfg.from_dataset_index

    with open(checkpoint_path, "r") as file:
        checkpoint = json.load(file)

    resource_ids_urls = [tuple(item) for item in checkpoint.get("resource_ids_urls", [])]
    full_metadata = checkpoint.get("full_metadata", [])
    start = checkpoint.get("next_start", cfg.from_dataset_index)
    return resource_ids_urls, full_metadata, start


def _save_metadata_checkpoint(
    cfg: CKANDownloadConfig,
    resource_ids_urls: list[tuple[str, str]],
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


def fetch_metadata(
    cfg: CKANDownloadConfig,
    client: CKAN,
    coordinator: RequestCoordinator,
    logger=None,
) -> tuple[list[tuple[str, str]], list[dict]]:
    resource_ids_urls, full_metadata, start = _load_metadata_checkpoint(cfg)

    metadata = _request_json_with_retries(
        lambda: client.package_search(start=0, rows=0, **cfg.package_search_filters),
        coordinator,
        logger or _NullLogger(),
        "package_search(count)",
    )
    if not metadata:
        raise ValueError("Failed to fetch metadata")

    packages_count = min(metadata["result"]["count"], cfg.max_datasets)
    batches = range(start, packages_count, cfg.batch_fetch_metadata)

    for current_start in tqdm(
        batches,
        total=max(0, (packages_count - start + cfg.batch_fetch_metadata - 1) // cfg.batch_fetch_metadata),
        desc="Metadata",
        disable=not cfg.verbose,
    ):
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

                if not url or not resource_id:
                    continue

                resource_id = Path(
                    resource_id.replace("/", "-").replace("__", "--").replace(":", "-")
                ).stem

                resource_name = Path(
                    resource_name.strip()
                    .replace(" ", "-")
                    .replace(":", "-")
                    .replace("__", "--")
                    if resource_name
                    else ""
                ).stem

                package_resource_ids.append(resource_id)

                if cfg.save_with_resource_name and resource_name:
                    resource_id = f"{resource_name}{SEP}{resource_id}"

                if not url.startswith("http"):
                    url = f"{client.base_url}/{url.lstrip('/')}"
                resource_ids_urls.append((resource_id, url))

            if package_resource_ids:
                package["resources"] = [
                    rsc
                    for rsc in resources
                    if rsc["id"].replace("/", "-") in package_resource_ids
                ]
                package["num_resources"] = len(package_resource_ids)
                full_metadata.append(package)

        if cfg.use_existing_metadata or cfg.save_metadata:
            _save_metadata_checkpoint(
                cfg,
                resource_ids_urls,
                full_metadata,
                current_start + cfg.batch_fetch_metadata,
            )

    resource_ids_urls = resource_ids_urls[:packages_count]
    return resource_ids_urls, full_metadata


def ckan_download_datasets(cfg: CKANDownloadConfig, client: CKAN):
    policy = _resolve_request_policy(cfg, client)

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
    coordinator = RequestCoordinator(policy)

    try:
        try:
            if policy.session_warmup_url:
                logger.info(f"Warming up CKAN session via {policy.session_warmup_url}")
                client.warmup_session(policy.session_warmup_url)

            if rsc_url_path.exists() and cfg.metadata_path.exists() and cfg.use_existing_metadata:
                with open(rsc_url_path, "r") as file:
                    rsc_url = json.load(file)
                with open(cfg.metadata_path, "r") as file:
                    metadata = json.load(file)
            else:
                rsc_url, metadata = fetch_metadata(cfg, client, coordinator, logger)

                if cfg.save_metadata:
                    with open(cfg.metadata_path, "w") as file:
                        json.dump(metadata, file, indent=4)
                    with open(rsc_url_path, "w") as file:
                        json.dump(rsc_url, file, indent=4)

                _delete_metadata_checkpoint(cfg)
        finally:
            listener.stop()

        download_tabular_resources(rsc_url, cfg, client, coordinator)
    finally:
        client.close()


class _NullLogger:
    def info(self, *_args, **_kwargs) -> None:
        return None
