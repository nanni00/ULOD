from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, Optional


@dataclass
class CKANDownloadConfig:
    """
    Configuration for bulk downloads with CKAN endpoints.

    Attributes:
        download_destination: Where datasets will be stored locally.
        from_dataset_index: Starting index with respect to remote indexing system.

        max_datasets: Max number of datasets to download.
        batch_fetch_metadata: Batch size for initial metadata downloading.
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

    # Logic-specific filters
    filter_resource_metadata: Optional[Callable] = None
    package_search_filters: dict = field(default_factory=dict)

    # Engine & Formats
    download_format: Literal["csv", "parquet", "json"] = "csv"

    # Boolean flags
    save_with_resource_name: bool = True
    save_metadata: bool = True
    accept_zip_files: bool = False  # Changed to True to match old 'accept_zip'
    verbose: bool = False

    # Networking & Concurrency
    http_headers: dict[str, Any] = field(default_factory=dict)
    connection_pool_kw: dict = field(default_factory=dict)
    max_resource_size: int = 2**20

    max_workers: int = 1

    # Verbosity
    verbose: bool = True

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

    download_format: Literal["csv", "parquet", "json"] = "csv"
    save_metadata: bool = True

    engine: Literal["pandas", "polars"] = "pandas"
    cast_datatypes: bool = False

    # Networking and Concurrency
    max_rows_per_dataset: int = 1000
    batch_rows_per_dataset: int = 1000
    max_datasets_per_worker: int = 100
    max_workers: int = 1

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
        self.batch_rows_per_dataset = min(
            self.max_rows_per_dataset, self.batch_rows_per_dataset
        )

        # 4. Initialize internal state/paths
        self._pbars: dict = {}
        self.log_folder_path: Path = self.download_destination / "logs"
        self.datasets_folder_path: Path = self.download_destination / "datasets"
        self.metadata_path: Path = self.download_destination / "metadata.json"
