import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

_SIZE_PATTERN = re.compile(r"^(?P<value>\d+)(?P<unit>B|KB|MB|GB)$", re.IGNORECASE)
_SIZE_UNITS = {
    "B": 1,
    "KB": 1024,
    "MB": 1024**2,
    "GB": 1024**3,
}


def _parse_size_bytes(value: int | str | None, field_name: str) -> int | None:
    if value is None:
        return None

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer byte count or size string")  # noqa: TRY004

    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"{field_name} cannot be negative")
        return value

    if isinstance(value, str):
        match = _SIZE_PATTERN.fullmatch(value)
        if not match:
            raise ValueError(
                f"{field_name} must use compact units like '10MB', '1GB', "
                "'512KB' or '100B'"
            )
        return int(match.group("value")) * _SIZE_UNITS[match.group("unit").upper()]

    raise ValueError(f"{field_name} must be an integer byte count or size string")


def _parse_http_statuses(
    value: int | Sequence[int] | None,
    field_name: str,
) -> tuple[int, ...]:
    if value is None:
        return ()

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must contain integer HTTP status codes")  # noqa: TRY004

    if isinstance(value, int):
        statuses = (value,)
    else:
        statuses = tuple(value)

    for status in statuses:
        if isinstance(status, bool) or not isinstance(status, int):
            raise ValueError(f"{field_name} must contain integer HTTP status codes")  # noqa: TRY004
        if status < 100 or status > 599:
            raise ValueError(f"{field_name} contains invalid HTTP status {status}")

    return tuple(dict.fromkeys(statuses))


@dataclass
class CKANDownloadConfig:
    """
    Configuration for bulk downloads with CKAN endpoints.

    Attributes:
        download_destination: Where datasets will be stored locally.
        from_dataset_index: Starting index with respect to remote indexing system.

        max_datasets: Max number of resources to download.
        batch_fetch_metadata: Batch size for initial metadata downloading.
        use_existing_metadata: If True and metadata have already been downloaded,
            don't fetch them again.
        filter_resource_metadata: Boolean predicate to apply on metadata.
        package_search_filters: Dictionary with filters on the package_search API
            method.
        engine: Which dataframe library to use to read and store the datasets.
        download_format: The format with which the datasets will be stored locally.
        load_dataset_kwargs: Dictionary of parameters to pass to pandas/polar.read_csv.
        save_dataset_kwargs: Dictionary of parameters to pass to pandas.to_* or polars.write_csv
            dataframe storing a downloaded dataset. These parameters should be coherent with
            the specified download format.
        save_with_resource_name: Whether the resource name has to be prepended to the resource ID.
            If True, resulting names will be <resource name>::<resource ID>.<download format>
        save_metadata: Whether the fetched metadata have to be saved locally.
        accept_zip_files: Whether ZIP folders have to be extracted and stored locally.
        http_headers: Dictionary of HTTP parameters passed to a urllib3.PoolManager instance as
            parameter "header".
        max_resource_size: Max resource size as bytes or compact size string
            like "10MB" or "1GB".
        skip_resource_statuses: HTTP statuses that should skip a resource without
            retrying the download request.
        max_process_workers: Max number of concurrent processes.
        max_workers: Max number of threads per process.
    """

    download_destination: Path

    from_dataset_index: int = 0
    max_datasets: int = int(1e9)
    batch_fetch_metadata: int = 1000

    # Metadata handling
    use_existing_metadata: bool = True

    # Logic-specific filters
    filter_resource_metadata: Callable | None = None
    package_search_filters: dict = field(default_factory=dict)

    # Engine & Formats
    download_format: Literal["csv", "parquet", "json"] = "csv"

    # Boolean flags
    save_with_resource_name: bool = True
    save_metadata: bool = True
    accept_zip_files: bool = False  # Changed to True to match old 'accept_zip'

    # Networking
    http_headers: dict[str, Any] = field(default_factory=dict)
    connection_pool_kw: dict = field(default_factory=dict)
    max_resource_size: int | str | None = 2**20
    request_delay_s: float | None = None
    request_jitter_s: float | None = None
    retry_backoff_base_s: float | None = None
    cooldown_on_403_s: float | None = None
    max_consecutive_403: int | None = None
    session_warmup_url: str | None = None
    skip_resource_statuses: int | Sequence[int] | None = field(default_factory=tuple)

    # Concurrency
    max_workers: int = 1

    # Verbosity
    verbose: bool = False

    def __post_init__(self):
        """
        Validate input parameters and handle paths.
        """
        # Ensure path is a Path object
        if isinstance(self.download_destination, str):
            self.download_destination = Path(self.download_destination)

        if not self.download_destination.exists():
            raise FileNotFoundError(
                f"Download destination folder doesn't exist: {self.download_destination.resolve()}"
            )

        if self.accept_zip_files:
            raise NotImplementedError(
                "ZIP handling is not implemented for CKAN downloads"
            )

        if self.max_datasets < -1:
            raise ValueError("max_datasets must be -1 or a non-negative integer")
        if self.batch_fetch_metadata < 1:
            raise ValueError("batch_fetch_metadata must be greater than zero")
        if self.max_workers < 1:
            raise ValueError("max_workers must be greater than zero")

        self.max_resource_size = _parse_size_bytes(
            self.max_resource_size,
            "max_resource_size",
        )
        self.skip_resource_statuses = _parse_http_statuses(
            self.skip_resource_statuses,
            "skip_resource_statuses",
        )

        self.datasets_folder_path = self.download_destination / "datasets"
        self.log_folder_path = self.download_destination / "logs"
        self.metadata_path = self.download_destination / "metadata.json"


@dataclass
class ODSDownloadConfig:
    """
    Configuration for bulk downloads with ODS endpoints.

    Attributes:
        download_destination: Where datasets will be stored locally.
        from_dataset_index: Starting index with respect to remote indexing system.

        max_datasets: Max number of datasets to download.
        batch_fetch_metadata: Batch size for initial metadata downloading.
        use_existing_metadata: If True and metadata have already been downloaded,
            don't fetch them again.
        filter_resource_metadata: Boolean predicate to apply on metadata.
        package_search_filters: Dictionary with filters on the package_search API
            method.
        engine: Which dataframe library to use to read and store the datasets.
        download_format: The format with which the datasets will be stored locally.
        load_dataset_kwargs: Dictionary of parameters to pass to pandas/polar.read_csv.
        save_dataset_kwargs: Dictionary of parameters to pass to pandas.to_* or polars.write_csv
            dataframe storing a downloaded dataset. These parameters should be coherent with
            the specified download format.
        save_with_resource_name: Whether the resource name has to be prepended to the resource ID.
            If True, resulting names will be <resource name>::<resource ID>.<download format>
        save_metadata: Whether the fetched metadata have to be saved locally.
        accept_zip_files: Whether ZIP folders have to be extracted and stored locally.
        http_headers: Dictionary of HTTP parameters passed to a urllib3.PoolManager instance as
            parameter "header".
        max_resource_size: Max resource size as bytes.
        max_process_workers: Max number of concurrent processes.
        max_workers: Max number of threads per process.
    """

    download_destination: Path

    from_dataset_index: int = 0
    max_datasets: int = int(1e9)
    batch_fetch_metadata: int = 1000

    # Metadata handling
    use_existing_metadata: bool = True

    # Logic-specific filters

    # Engine & Formats
    download_format: Literal["csv", "parquet", "json"] = "csv"

    # Boolean flags
    save_with_resource_name: bool = True
    save_metadata: bool = True

    # Networking
    http_headers: dict[str, Any] = field(default_factory=dict)
    connection_pool_kw: dict = field(default_factory=dict)

    # Concurrency
    max_workers: int = 1

    # Verbosity
    verbose: bool = False

    def __post_init__(self):
        """
        Validate input parameters and handle paths.
        """
        # Ensure path is a Path object
        if isinstance(self.download_destination, str):
            self.download_destination = Path(self.download_destination)

        if not self.download_destination.exists():
            raise FileNotFoundError(
                f"Download destination folder doesn't exist: {self.download_destination.resolve()}"
            )

        # ... existing validation ...
        self.datasets_folder_path = self.download_destination / "datasets"
        self.log_folder_path = self.download_destination / "logs"
        self.metadata_path = self.download_destination / "metadata.json"


@dataclass
class SocrataDownloadConfig:
    """
    Configuration for bulk downloads with Socrata endpoints.
    """

    download_destination: Path
    from_dataset_index: int = 0
    max_datasets: int = int(1e9)

    # Metadata handling
    use_existing_metadata: bool = True
    skip_existing_datasets: bool = False

    download_format: Literal["csv", "parquet", "json"] = "csv"
    download_strategy: Literal["api", "export"] = "export"
    save_metadata: bool = True

    engine: Literal["pandas", "polars"] = "pandas"
    cast_datatypes: bool = False

    # Networking and Concurrency
    max_rows_per_dataset: int = 1000
    batch_rows_per_dataset: int = 50_000
    max_datasets_per_worker: int = 100
    max_workers: int = 1
    parquet_compression_level: int | None = None
    export_chunk_size: int = 1024 * 1024
    keep_intermediate_files: bool = False

    # Verbosity
    verbose: bool = False

    def __post_init__(self):
        # 1. Path validation (matching your old self.download_dst)
        if not self.download_destination.exists():
            raise FileNotFoundError(
                f"Directory doesn't exist: {self.download_destination}"
            )

        # # 2. Engine validation
        # if self.engine != "pandas":
        #     raise NotImplementedError(
        #         "Only 'pandas' engine is currently supported for Socrata."
        #     )

        # 3. Dynamic logic: batch_rows_per_dataset cannot exceed max_rows_per_dataset
        if self.max_rows_per_dataset > -1:
            self.batch_rows_per_dataset = min(
                self.max_rows_per_dataset, self.batch_rows_per_dataset
            )

        # 4. Initialize internal state/paths
        self._pbars: dict = {}
        self.log_folder_path: Path = self.download_destination / "logs"
        self.datasets_folder_path: Path = self.download_destination / "datasets"
        self.metadata_path: Path = self.download_destination / "metadata.json"


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
                fmt.lower().lstrip(".").strip()
                for fmt in self.formats
                if fmt.strip()
            )

        self.run_root_path: Path = self.download_destination
        self.log_folder_path: Path = self.download_destination / "logs"
        self.datasets_folder_path: Path = self.download_destination / "datasets"
        self.metadata_path: Path = self.download_destination / "metadata.json"
        self.manifest_path: Path = self.download_destination / "manifest.json"
