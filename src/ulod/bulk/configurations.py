"""Configuration for catalog downloads and the unchanged Data.gov downloader."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


@dataclass
class DownloadConfig:
    """Download an entire catalog unless filters or limits are explicitly supplied.

    ``formats`` selects native file/export formats, never converts files. Predicates
    receive original dataset/resource metadata. One destination belongs to one portal.
    ``resume`` reuses verified completed files; ``refresh_metadata`` restarts catalog
    discovery. Partial files are restarted, not appended with HTTP Range requests.
    Credentials/headers are sent only to the configured portal origin.
    """

    download_destination: Path | str
    formats: Sequence[str] | set[str] | str | None = None
    dataset_filter: Callable[[dict], bool] | None = None
    resource_filter: Callable[[dict], bool] | None = None
    export_format: str = "csv"
    page_size: int = 100
    max_datasets: int | None = None
    max_resource_size: int | None = None
    max_workers: int = 4
    timeout: float = 60
    retries: int = 3
    retry_backoff: float = 1
    request_delay: float = 0
    chunk_size: int = 1024 * 1024
    resume: bool = True
    refresh_metadata: bool = False
    headers: dict[str, str] = field(default_factory=dict)
    api_key: str | None = field(default=None, repr=False)
    app_token: str | None = field(default=None, repr=False)
    access_token: str | None = field(default=None, repr=False)
    verbose: bool = False

    def __post_init__(self):
        from .common import normalize_format

        self.download_destination = Path(self.download_destination)
        if self.formats is not None:
            values = [self.formats] if isinstance(self.formats, str) else self.formats
            self.formats = frozenset(normalize_format(value) for value in values)
            if "" in self.formats:
                raise ValueError("formats must contain valid extensions or MIME types")
        self.export_format = normalize_format(self.export_format)
        if not self.export_format:
            raise ValueError("export_format must be a valid extension or MIME type")
        for name in ("page_size", "max_workers", "chunk_size"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("max_datasets", "max_resource_size"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"{name} must be a non-negative integer or None")
        if (
            not isinstance(self.retries, int)
            or isinstance(self.retries, bool)
            or self.retries < 0
        ):
            raise ValueError("retries must be a non-negative integer")
        if self.timeout <= 0 or self.retry_backoff < 0 or self.request_delay < 0:
            raise ValueError(
                "timeout must be positive; retry_backoff/request_delay non-negative"
            )


@dataclass
class USDownloadConfig:
    """
    Configuration for raw bulk downloads from catalog.data.gov.
    """

    download_destination: Path

    mode: Literal["all", "updated-only"] = "all"
    formats: tuple[str, ...] | str | None = None

    # Data.gov search filters
    q: str = ""
    sort: Literal["relevance", "popularity", "distance", "last_harvested_date"] = (
        "last_harvested_date"
    )
    per_page: int = 100
    after: str | None = None
    org_slug: str | None = None
    org_type: str | None = None
    keyword: str | Sequence[str] | None = None
    spatial_filter: Literal["geospatial", "non-geospatial"] | None = None
    spatial_geometry: str | Mapping[str, Any] | None = None
    spatial_within: bool | None = None

    max_datasets: int = int(1e9)
    max_workers: int = 1
    max_resource_size: int | None = None
    chunk_size: int = 65536
    save_metadata: bool = True
    verbose: bool = True

    def __post_init__(self):
        if isinstance(self.download_destination, str):
            self.download_destination = Path(self.download_destination)

        if not self.download_destination.exists():
            raise FileNotFoundError(
                f"Download destination folder doesn't exist: {self.download_destination.resolve()}"
            )

        if self.mode not in {"all", "updated-only"}:
            raise ValueError("mode must be either 'all' or 'updated-only'")

        if isinstance(self.formats, str):
            self.formats = (self.formats,)
        elif self.formats is not None:
            self.formats = tuple(self.formats)

        if self.formats is not None:
            self.formats = tuple(
                fmt.lower().lstrip(".").strip() for fmt in self.formats if fmt.strip()
            )

        self.run_root_path: Path = self.download_destination
        self.log_folder_path: Path = self.download_destination / "logs"
        self.datasets_folder_path: Path = self.download_destination / "datasets"
        self.metadata_path: Path = self.download_destination / "metadata.json"
        self.manifest_path: Path = self.download_destination / "manifest.json"
