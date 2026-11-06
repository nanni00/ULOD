"""Small shared primitives for discovery, raw transfers and checkpoints."""

from __future__ import annotations

import hashlib
import json
import mimetypes
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote, unquote, urljoin, urlparse

import requests


@dataclass
class Resource:
    id: str
    url: str | None
    format: str = ""
    metadata: dict = field(default_factory=dict)
    kind: str = "file"
    reason: str | None = None


@dataclass
class Dataset:
    id: str
    metadata: dict
    resources: list[Resource] = field(default_factory=list)
    extra_metadata: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


class UnsupportedResource(ValueError):
    pass


class ResourceTooLarge(ValueError):
    pass


def normalize_format(value) -> str:
    value = str(value or "").lower().strip().lstrip(".").split(";", 1)[0].strip()
    aliases = {
        "text/csv": "csv",
        "application/csv": "csv",
        "comma-separated-values": "csv",
        "application/zip": "zip",
        "application/x-zip-compressed": "zip",
        "application/x-zip": "zip",
        "application/json": "json",
        "application/geo+json": "geojson",
        "application/vnd.geo+json": "geojson",
        "application/pdf": "pdf",
        "application/vnd.apache.parquet": "parquet",
        "text/tab-separated-values": "tsv",
        "application/x-ndjson": "jsonl",
        "ndjson": "jsonl",
        "application/octet-stream": "",
        "text/plain": "txt",
        "shapefile": "shp",
        "application/gzip": "gz",
    }
    if value in aliases:
        return aliases[value]
    if "/" in value:
        value = (mimetypes.guess_extension(value) or "").lstrip(".")
    return value if re.fullmatch(r"[a-z0-9][a-z0-9_+-]{0,30}", value) else ""


def infer_format(url="", declared="", filename="") -> str:
    extension = normalize_format(Path(unquote(urlparse(filename or url).path)).suffix)
    # A ZIP containing CSV is a ZIP, regardless of the publisher's CSV label.
    if extension in {"zip", "gz", "bz2", "xz", "7z"}:
        return extension
    return normalize_format(declared) or extension


def safe_name(value: str) -> str:
    name = quote(str(value), safe="-_.")
    if not name or name in {".", ".."}:
        name = "id-" + hashlib.sha256(str(value).encode()).hexdigest()[:16]
    if len(name) > 180:
        name = name[:140] + "-" + hashlib.sha256(str(value).encode()).hexdigest()[:24]
    return name


def fingerprint(value) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


def resource_id(metadata: dict, prefix="resource") -> str:
    return str(
        metadata.get("id")
        or metadata.get("identifier")
        or f"{prefix}-{fingerprint(metadata)[:20]}"
    )


def atomic_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as output:
        json.dump(value, output, ensure_ascii=False, indent=2)
        output.write("\n")
    temporary.replace(path)


def read_json(path: Path, default):
    if not path.exists():
        return default
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def file_hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def choose_export(resources: list[Resource], cfg) -> list[Resource]:
    allowed = [r for r in resources if cfg.formats is None or r.format in cfg.formats]
    if not allowed:
        return []
    return [
        min(
            allowed,
            key=lambda r: (r.format != cfg.export_format, r.format, r.url or ""),
        )
    ]


def paged_entries(page: dict, items_key: str, total_key: str, offset: int):
    """Validate a page so truncated or malformed responses cannot look complete."""
    entries, total = page[items_key], page[total_key]
    if not isinstance(entries, list) or not isinstance(total, int) or total < 0:
        raise ValueError("Invalid catalog page or total count")
    if not entries and offset < total:
        raise ValueError(f"Catalog ended at {offset}, before advertised total {total}")
    return entries, total


class HTTP:
    """One requests session per thread, with portal-scoped credentials and retries."""

    def __init__(self, url, cfg, source):
        self.url, self.cfg = url, cfg
        self.origin = urlparse(url)[:2]
        self.headers = dict(cfg.headers)
        if cfg.api_key and source in {"ckan", "ods"}:
            self.headers["Authorization"] = (
                "Apikey " if source == "ods" else ""
            ) + cfg.api_key
        if cfg.app_token and source == "socrata":
            self.headers["X-App-Token"] = cfg.app_token
        if cfg.access_token:
            self.headers["Authorization"] = (
                "OAuth " if source == "socrata" else "Bearer "
            ) + cfg.access_token
        self.local = threading.local()
        self.sessions = []
        self.lock = threading.Lock()
        self.next_request = 0.0

    def session(self):
        if not hasattr(self.local, "session"):
            self.local.session = requests.Session()
            # Never use ambient netrc credentials for catalog resource links.
            self.local.session.trust_env = False
            with self.lock:
                self.sessions.append(self.local.session)
        return self.local.session

    def throttle(self):
        with self.lock:
            delay = max(0, self.next_request - time.monotonic())
            self.next_request = time.monotonic() + delay + self.cfg.request_delay
        if delay:
            time.sleep(delay)

    def retry(self, operation):
        for attempt in range(self.cfg.retries + 1):
            try:
                self.throttle()
                return operation()
            except Exception as error:
                if isinstance(
                    error, (UnsupportedResource, ResourceTooLarge, OSError)
                ) and not isinstance(error, requests.RequestException):
                    raise
                response = getattr(error, "response", None)
                if response is not None and response.status_code not in {
                    408,
                    429,
                    500,
                    502,
                    503,
                    504,
                }:
                    raise
                if attempt == self.cfg.retries:
                    raise
                delay = self.cfg.retry_backoff * 2**attempt
                if response is not None and response.headers.get("Retry-After"):
                    retry_after = response.headers["Retry-After"]
                    try:
                        delay = max(delay, float(retry_after))
                    except ValueError:
                        try:
                            delay = max(
                                delay,
                                (
                                    parsedate_to_datetime(retry_after)
                                    - datetime.now(timezone.utc)
                                ).total_seconds(),
                            )
                        except (TypeError, ValueError):
                            pass
                time.sleep(delay)

    def get(self, url, **kwargs):
        """Follow redirects explicitly to keep all supplied credentials on-origin."""
        for _ in range(11):
            headers = self.headers if urlparse(url)[:2] == self.origin else {}
            response = self.session().get(
                url,
                headers=headers,
                timeout=self.cfg.timeout,
                allow_redirects=False,
                **kwargs,
            )
            if response.is_redirect:
                target = urljoin(url, response.headers["Location"])
                response.close()
                if urlparse(target).scheme not in {"http", "https"}:
                    raise UnsupportedResource("Unsupported redirect protocol")
                url = target
                kwargs.pop("params", None)
                continue
            try:
                response.raise_for_status()
            except Exception:
                response.close()
                raise
            return response
        raise requests.TooManyRedirects(url)

    def json(self, url, **kwargs):
        def request():
            with self.get(url, **kwargs) as response:
                return response.json()

        return self.retry(request)

    def close(self):
        for session in self.sessions:
            session.close()


def transfer(http: HTTP, resource: Resource, path: Path, cfg):
    """Restart interrupted streams and only expose a final path after completion."""
    if not resource.url or urlparse(resource.url).scheme not in {"http", "https"}:
        raise UnsupportedResource("Missing download URL or unsupported protocol")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".part")

    def attempt():
        try:
            with http.get(resource.url, stream=True) as response:
                content_type = (
                    response.headers.get("Content-Type", "").split(";", 1)[0].lower()
                )
                if content_type == "text/html" and resource.format not in {
                    "html",
                    "htm",
                }:
                    raise UnsupportedResource("Download URL returned an HTML page")
                if response.status_code == 206:
                    raise UnsupportedResource(
                        "Server returned an unsolicited partial response"
                    )
                expected = response.headers.get("Content-Length")
                expected = int(expected) if expected is not None else None
                if (
                    cfg.max_resource_size is not None
                    and expected is not None
                    and expected > cfg.max_resource_size
                ):
                    raise ResourceTooLarge("Content-Length exceeds max_resource_size")
                size = 0
                digest = hashlib.sha256()
                with temporary.open("wb") as output:
                    for chunk in response.iter_content(cfg.chunk_size):
                        if not chunk:
                            continue
                        size += len(chunk)
                        if (
                            cfg.max_resource_size is not None
                            and size > cfg.max_resource_size
                        ):
                            raise ResourceTooLarge("Stream exceeds max_resource_size")
                        output.write(chunk)
                        digest.update(chunk)
                if (
                    expected is not None
                    and not response.headers.get("Content-Encoding")
                    and size != expected
                ):
                    raise requests.ConnectionError(
                        f"Incomplete stream: {size} of {expected} bytes"
                    )
                temporary.replace(path)
                return {
                    "size": size,
                    "sha256": digest.hexdigest(),
                    "content_type": content_type,
                }
        finally:
            temporary.unlink(missing_ok=True)

    return http.retry(attempt)
