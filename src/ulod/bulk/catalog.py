"""One catalog download workflow for CKAN, Socrata and OpenDataSoft."""

from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from urllib.parse import urlparse

import requests

from .common import (
    HTTP,
    ResourceTooLarge,
    UnsupportedResource,
    atomic_json,
    choose_export,
    file_hash,
    fingerprint,
    read_json,
    safe_name,
    transfer,
)
from .configurations import DownloadConfig


@dataclass
class DownloadReport:
    source: str
    url: str
    destination: Path
    datasets: int = 0
    discovery_complete: bool = False
    metadata_complete: bool = True
    downloaded: int = 0
    existing: int = 0
    filtered: int = 0
    failed: int = 0
    unavailable: int = 0
    unsupported: int = 0
    limited: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """True only if discovery and all selected, supported data are complete."""
        return (
            self.discovery_complete
            and self.metadata_complete
            and not self.errors
            and not (
                self.failed or self.unavailable or self.unsupported or self.limited
            )
        )


def _adapter(source, url, cfg, http):
    name = {"ckan": "CKANAdapter", "socrata": "SocrataAdapter", "ods": "ODSAdapter"}[
        source
    ]
    return getattr(import_module(f"ulod.bulk.{source}"), name)(url, cfg, http)


def _discover(adapter, source, url, cfg, report):
    path = cfg.download_destination / "catalog.json"
    previous = read_json(path, None)
    if previous and (previous["source"], previous["url"]) != (source, url):
        raise ValueError(
            "Destination already contains another portal; use a separate directory"
        )
    state = (
        previous
        if previous and not cfg.refresh_metadata
        else {
            "version": 1,
            "source": source,
            "url": url,
            "entries": [],
            "complete": False,
            "total": None,
        }
    )
    state.pop("error", None)
    seen = {adapter.identifier(entry) for entry in state["entries"]}
    try:
        while not state["complete"]:
            offset = len(state["entries"])
            if cfg.max_datasets is not None and offset >= cfg.max_datasets:
                break
            entries, total = adapter.page(offset)
            if state["total"] is not None and state["total"] != total:
                raise ValueError(
                    "Catalog count changed during discovery; restart with refresh_metadata=True"
                )
            state["total"] = total
            if cfg.max_datasets is not None:
                entries = entries[: max(0, cfg.max_datasets - offset)]
            identifiers = [adapter.identifier(entry) for entry in entries]
            if seen.intersection(identifiers) or len(set(identifiers)) != len(
                identifiers
            ):
                raise ValueError(
                    "Catalog pagination repeated dataset IDs; snapshot is incomplete"
                )
            if not entries and offset < total:
                raise ValueError("Catalog stopped before the advertised total")
            state["entries"].extend(entries)
            seen.update(identifiers)
            if len(state["entries"]) > total:
                raise ValueError("Catalog contains more entries than advertised")
            state["complete"] = len(state["entries"]) == total
            atomic_json(path, state)
    except Exception as error:
        state["error"] = f"{type(error).__name__}: {error}"
        report.errors.append("Catalog discovery: " + state["error"])
    finally:
        atomic_json(path, state)
    entries = state["entries"]
    report.discovery_complete = state["complete"]
    if cfg.max_datasets is not None:
        if len(entries) > cfg.max_datasets:
            report.discovery_complete = False
        entries = entries[: cfg.max_datasets]
    return entries


def _completed(previous, record, destination):
    if not previous or previous.get("status") not in {"downloaded", "existing"}:
        return False
    if (
        previous.get("signature") != record["signature"]
        or previous.get("path") != record["path"]
    ):
        return False
    path = destination / record["path"]
    return (
        path.is_file()
        and path.stat().st_size == previous.get("size")
        and file_hash(path) == previous.get("sha256")
    )


def _download(http, resource, record, cfg):
    try:
        record.update(
            transfer(http, resource, cfg.download_destination / record["path"], cfg)
        )
        record["status"] = "downloaded"
    except UnsupportedResource as error:
        record.update(status="unsupported", error=str(error))
    except ResourceTooLarge as error:
        record.update(status="limited", error=str(error))
    except Exception as error:
        if isinstance(error, OSError) and not isinstance(
            error, requests.RequestException
        ):
            raise
        response = getattr(error, "response", None)
        status = (
            "unavailable"
            if response is not None and response.status_code in {401, 403, 404, 410}
            else "failed"
        )
        record.update(status=status, error=f"{type(error).__name__}: {error}")
    return record


def download_catalog(source: str, url: str, config: DownloadConfig) -> DownloadReport:
    """Save raw metadata and downloadable data; inspect ``report.success`` afterwards.

    ``source`` is ``ckan``, ``socrata`` or ``ods``. URLs are portal roots, not API
    endpoints. Network/discovery failures are recorded and return an unsuccessful
    report. Invalid configuration and local filesystem errors raise exceptions.
    Catalog snapshots are reused unless ``refresh_metadata=True``. Dataset details
    are re-read on every run; completed files are reused only when their metadata
    fingerprint, size and SHA-256 still match. Do not run two jobs in one destination.
    """
    source, url = source.lower(), url.rstrip("/")
    if source not in {"ckan", "socrata", "ods"}:
        raise ValueError("source must be ckan, socrata or ods")
    parsed = urlparse(url)
    if (
        parsed.scheme not in {"https", "http"}
        or not parsed.netloc
        or parsed.query
        or parsed.fragment
        or parsed.username
    ):
        raise ValueError(
            "url must be an HTTP(S) portal root without credentials, query or fragment"
        )
    cfg = config
    destination = cfg.download_destination
    destination.mkdir(parents=True, exist_ok=True)
    if cfg.verbose:
        print(f"Download destination: {cfg.download_destination}")
        
    manifest_path = destination / "manifest.json"
    previous = read_json(manifest_path, {})
    if previous and (previous.get("source"), previous.get("url")) != (source, url):
        raise ValueError(
            "Destination already contains another portal; use a separate directory"
        )
    catalog = read_json(destination / "catalog.json", {})
    if catalog and (catalog.get("source"), catalog.get("url")) != (source, url):
        raise ValueError(
            "Destination already contains another portal; use a separate directory"
        )
    old_records = {
        (r["dataset_id"], r["resource_id"]): r for r in previous.get("resources", [])
    }
    report = DownloadReport(source, url, destination)
    manifest = {
        "version": 1,
        "source": source,
        "url": url,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "datasets": {},
        "resources": [],
    }
    http = HTTP(url, cfg, source)
    adapter = None

    def save(*, final=False):
        counts = Counter(r["status"] for r in manifest["resources"])
        for name in (
            "downloaded",
            "existing",
            "filtered",
            "failed",
            "unavailable",
            "unsupported",
            "limited",
        ):
            setattr(report, name, counts[name])
        summary = asdict(report)
        summary["destination"] = str(destination)
        summary["success"] = report.success
        manifest["report"] = summary
        if final:
            atomic_json(manifest_path, manifest)
        atomic_json(destination / "download_report.json", summary)

    try:
        adapter = _adapter(source, url, cfg, http)
        entries = _discover(adapter, source, url, cfg, report)
        report.datasets = len(entries)
        # Preserve completed checkpoints for datasets not reached before interruption.
        active_ids = {adapter.identifier(entry) for entry in entries}
        manifest["resources"] = [
            dict(r) for r in old_records.values() if r["dataset_id"] in active_ids
        ]
        report.metadata_complete = False
        save()
        with ThreadPoolExecutor(max_workers=cfg.max_workers) as executor:
            for entry in entries:
                identifier = adapter.identifier(entry)
                folder = destination / "datasets" / safe_name(identifier)
                folder.mkdir(parents=True, exist_ok=True)
                checkpoint_path = folder / "downloads.json"
                checkpoint = read_json(checkpoint_path, {})
                if checkpoint and (checkpoint.get("source"), checkpoint.get("url")) != (
                    source,
                    url,
                ):
                    raise ValueError("Dataset checkpoint belongs to another portal")
                old_records.update(
                    {
                        (identifier, r["resource_id"]): r
                        for r in checkpoint.get("resources", [])
                    }
                )
                info = {"status": "pending", "errors": []}
                manifest["datasets"][identifier] = info
                try:
                    dataset = adapter.describe(entry)
                    dataset_signature = fingerprint(dataset.metadata)
                    atomic_json(folder / "metadata.json", dataset.metadata)
                    for name, value in dataset.extra_metadata.items():
                        atomic_json(folder / f"{safe_name(name)}.json", value)
                    info["errors"] = dataset.errors
                    info["status"] = (
                        "metadata_incomplete" if dataset.errors else "ready"
                    )
                    selected_dataset = cfg.dataset_filter is None or cfg.dataset_filter(
                        dataset.metadata
                    )
                    # Apply resource predicates before choosing the single generated export.
                    eligible = [
                        r
                        for r in dataset.resources
                        if cfg.resource_filter is None
                        or cfg.resource_filter(r.metadata)
                    ]
                    chosen = choose_export(
                        [r for r in eligible if r.kind == "export"], cfg
                    )
                    chosen_ids = {id(r) for r in chosen}
                    eligible_ids = {id(r) for r in eligible}
                    records, jobs, used_paths = [], [], set()
                    for resource in dataset.resources:
                        filename = (
                            safe_name(resource.id) + "." + (resource.format or "bin")
                        )
                        if filename in {
                            "metadata.json",
                            "exports.json",
                            "attachments.json",
                            "downloads.json",
                        }:
                            filename = "resource-" + filename
                        relative = (
                            (folder / filename).relative_to(destination).as_posix()
                        )
                        if relative in used_paths:
                            raise ValueError(
                                f"Duplicate resource output path: {relative}"
                            )
                        used_paths.add(relative)
                        record = {
                            "dataset_id": identifier,
                            "resource_id": resource.id,
                            "url": resource.url,
                            "format": resource.format,
                            "kind": resource.kind,
                            "metadata": resource.metadata,
                            "path": relative,
                            "signature": fingerprint(
                                [dataset_signature, resource.metadata, resource.url]
                            ),
                            "status": "pending",
                        }
                        if not selected_dataset or id(resource) not in eligible_ids:
                            record.update(
                                status="filtered", reason="Metadata predicate"
                            )
                        elif (
                            cfg.formats is not None
                            and resource.format not in cfg.formats
                        ):
                            record.update(status="filtered", reason="Format filter")
                        elif (
                            resource.kind == "export" and id(resource) not in chosen_ids
                        ):
                            record.update(
                                status="filtered",
                                reason="One generated export per dataset",
                            )
                        elif resource.reason:
                            record.update(status="unsupported", error=resource.reason)
                        elif not resource.url:
                            record.update(status="unavailable", error="No download URL")
                        else:
                            previous_record = old_records.get((identifier, resource.id))
                            if cfg.resume and _completed(
                                previous_record, record, destination
                            ):
                                record.update(
                                    {
                                        k: previous_record[k]
                                        for k in ("size", "sha256", "content_type")
                                    }
                                )
                                record["status"] = "existing"
                            else:
                                jobs.append((resource, record))
                        records.append(record)
                    manifest["resources"] = [
                        r
                        for r in manifest["resources"]
                        if r["dataset_id"] != identifier
                    ] + records
                    checkpoint = {"source": source, "url": url, "resources": records}
                    atomic_json(checkpoint_path, checkpoint)
                    save()
                    futures = {
                        executor.submit(
                            _download, http, resource, dict(record), cfg
                        ): record
                        for resource, record in jobs
                    }
                    for future in as_completed(futures):
                        futures[future].update(future.result())
                        atomic_json(checkpoint_path, checkpoint)
                    info["status"] = (
                        "metadata_incomplete"
                        if dataset.errors
                        else ("processed" if selected_dataset else "filtered")
                    )
                except Exception as error:
                    if isinstance(error, OSError) and not isinstance(
                        error, requests.RequestException
                    ):
                        raise
                    info.update(
                        status="metadata_incomplete",
                        errors=[f"{type(error).__name__}: {error}"],
                    )
                save()
        report.metadata_complete = all(
            d["status"] != "metadata_incomplete" for d in manifest["datasets"].values()
        )
        for identifier, info in manifest["datasets"].items():
            report.errors.extend(
                f"Dataset {identifier}: {error}" for error in info["errors"]
            )
    except BaseException as error:
        report.errors.append(f"Run interrupted: {type(error).__name__}: {error}")
        raise
    finally:
        try:
            save(final=True)
        finally:
            if adapter is not None:
                adapter.close()
            http.close()
    return report
