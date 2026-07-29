from copy import deepcopy
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
import polars as pl
import requests
from sodapy import Socrata

from ulod.base import Source
from ulod.socrata.utils import cast_socrata_types

__all__ = ["SocrataClient"]


class SocrataClient(Source):
    source_type = "socrata"

    def __init__(
        self,
        domain: str,
        app_token: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 20,
    ) -> None:
        self.domain = domain
        self.app_token = app_token
        self.timeout = timeout
        self.user = user
        self.password = password

        self._sodapy_configuration = {
            "domain": domain,
            "app_token": app_token,
            "username": user,
            "password": password,
            "timeout": timeout,
        }

    def get_datasets_information(self, limit: int, offset: int, **kwargs):
        with Socrata(**self._sodapy_configuration) as client:
            datasets_metadata = client.datasets(limit=limit, offset=offset, **kwargs)
            return datasets_metadata

    def get_dataset(
        self,
        id: str,
        format: Literal["csv", "json", "xml"],
        **kwargs,
    ) -> list:
        """
        Query the Sodapy client API and returns the required dataset
        as a list of records, through the desired format.

        :param id: the dataset ID.
        :param format: the format in which return the dataset records.
        :param kwargs: see sodapy.Socrata.get() **kwargs.
        :return: a list of records in CSV, JSON or XML format
        """
        limit = int(kwargs.pop("limit", 10))
        offset = int(kwargs.pop("offset", 0))
        batch_size = int(kwargs.pop("batch_size", min(limit, 1000)))

        with Socrata(**self._sodapy_configuration) as client:
            dataset = []

            while limit == -1 or len(dataset) < limit:
                if limit == -1:
                    current_batch_size = batch_size
                else:
                    current_batch_size = min(batch_size, limit - len(dataset))

                new_rows = client.get(
                    dataset_identifier=id,
                    content_type=format,
                    limit=current_batch_size,
                    offset=offset,
                    **kwargs,
                )

                if len(new_rows) == 0:
                    break

                dataset.extend(new_rows)
                offset += batch_size
        return dataset

    def get_dataset_metadata(self, id: str):
        with Socrata(**self._sodapy_configuration) as client:
            metadata = client.get_metadata(id)
        return metadata

    def _soda3_export_url(self, id: str, format: str) -> str:
        return f"https://{self.domain}/api/v3/views/{id}/export.{format}"

    def _download_export_to_file(
        self,
        id: str,
        file_name: Path,
        export_format: Literal["csv"] = "csv",
        chunk_size: int = 1024 * 1024,
        limit: int = -1,
    ) -> None:
        headers = {}
        auth = None
        params = {}

        if self.app_token:
            headers["X-App-Token"] = self.app_token

        if self.user and self.password:
            auth = (self.user, self.password)

        if limit > -1:
            params["query"] = f"SELECT * LIMIT {limit}"

        with requests.get(
            self._soda3_export_url(id, export_format),
            params=params,
            headers=headers,
            auth=auth,
            stream=True,
            timeout=self.timeout,
        ) as response:
            response.raise_for_status()

            with open(file_name, "wb") as file:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        file.write(chunk)

    def download_dataset_export(
        self,
        id: str,
        store_dst: Path,
        store_format: Literal["csv", "parquet"] = "parquet",
        engine: Literal["pandas", "polars"] = "polars",
        return_dataframe: bool = False,
        chunk_size: int = 1024 * 1024,
        limit: int = -1,
        parquet_compression_level: int | None = None,
        keep_intermediate_files: bool = False,
    ) -> None | pd.DataFrame | pl.DataFrame:
        assert store_dst.exists()
        if store_format not in {"csv", "parquet"}:
            raise ValueError("Socrata export downloads support only csv and parquet")

        file_name = store_dst.joinpath(f"{id}.{store_format}")

        if store_format == "csv":
            self._download_export_to_file(id, file_name, "csv", chunk_size, limit)
            return None

        csv_file_name = store_dst.joinpath(f"{id}.csv")
        tmp_csv_file_name = store_dst.joinpath(f"{id}.csv.tmp")
        intermediate_file_name = (
            csv_file_name if keep_intermediate_files else tmp_csv_file_name
        )

        self._download_export_to_file(
            id,
            intermediate_file_name,
            "csv",
            chunk_size,
            limit,
        )

        try:
            match engine:
                case "pandas":
                    df = pd.read_csv(intermediate_file_name)
                    df.to_parquet(file_name, index=False)
                case "polars":
                    df = pl.read_csv(intermediate_file_name)
                    df.write_parquet(
                        file_name,
                        compression_level=parquet_compression_level,
                    )
        finally:
            if not keep_intermediate_files:
                intermediate_file_name.unlink(missing_ok=True)

        if return_dataframe:
            return df

    def get_dataset_as_df(
        self,
        id: str,
        engine: Literal["pandas", "polars"] = "polars",
        cast_datatypes: bool = False,
        resource_metadata: Optional[dict] = None,
        batch_size: int = 1000,
        **kwargs,
    ) -> pd.DataFrame | pl.DataFrame:
        """
        Get the specified dataset through the Sodapy client and
        return it as a pandas or polars dataframe.

        :param id: the dataset ID.
        :param engine: the engine use for dataframe creation.
        :param cast_datatypes: if False, the data will be returned as fetched from remote.
            Otherwise, it will be casted according to metadata. If no metadata is passed,
            this is skipped.

        :param resource_metadata: A dictionary specifing information about the dataset.
        :param batch_size: How many records to fetch in each call to the API.
        :param kwargs: see SocrataClient.get_dataset **kwargs.
        :return: A pandas or polars dataframe storing the required dataset.
        """
        data = self.get_dataset(id, format="json", batch_size=batch_size, **kwargs)

        datatypes = []
        dtypes_mapping = {}
        columns = None

        if cast_datatypes and resource_metadata:
            resource_metadata = resource_metadata["resource"]
            columns = (
                {"fieldName": name, "dataTypeName": dtype, "format": format_}
                for name, dtype, format_ in zip(
                    resource_metadata["columns_field_name"],
                    resource_metadata["columns_datatype"],
                    resource_metadata["columns_format"],
                )
            )

            datatypes = [
                {
                    "name": column["fieldName"],
                    "data_type": column["dataTypeName"],
                    "format": column["format"],
                }
                for column in columns
            ]

            dtypes_mapping = cast_socrata_types(datatypes, engine)

        match engine:
            case "pandas":
                if cast_datatypes:
                    columns = list(dtypes_mapping.keys())
                df = pd.DataFrame(data, None, columns)  # ty: ignore

                if cast_datatypes:
                    for column, dtype in dtypes_mapping.items():
                        if dtype.startswith("date"):
                            df[column] = pd.to_datetime(df[column], format="mixed")
                        elif dtype in ["integer", "float"]:
                            df[column] = pd.to_numeric(
                                df[column], errors="coerce", downcast=dtype
                            )
                        else:
                            df = df.astype(
                                {column: dtype},
                            )
            case "polars":
                df = pl.DataFrame(
                    data, dtypes_mapping, orient="row", infer_schema_length=None
                )
        return df

    def get_and_store_dataset(
        self,
        id: str,
        store_dst: Path,
        store_format: Literal["csv", "json", "parquet"] = "parquet",
        engine: Literal["pandas", "polars"] = "polars",
        cast_datatypes: bool = False,
        resource_metadata: Optional[dict] = None,
        batch_size: int = 1000,
        return_dataframe: bool = False,
        parquet_compression_level: int | None = None,
        **kwargs,
    ) -> None | pd.DataFrame | pl.DataFrame:
        """
        Get the specified dataset through the Sodapy client and
        return it as a pandas or polars dataframe.

        :param id: the dataset ID.
        :param store_dst: A Path object pointing to store destination.
        :param store_format: [TODO:description]
        :param engine: the engine use for dataframe creation.
        :param cast_datatypes: if False, the data will be returned as fetched from remote.
            Otherwise, it will be casted according to metadata. If no metadata is passed,
            this is skipped.

        :param resource_metadata: A dictionary specifing information about the dataset.
        :param batch_size: How many records to fetch in each call to the API.
        :param return_dataframe: If True, returns the dataset as a pandas or polars dataframe.
        :param kwargs: see SocrataClient.get_dataset **kwargs.
        :return: A pandas or polars dataframe storing the required dataset, or None if
            return_dataframe is set to False.

        """
        assert store_dst.exists()
        file_name = store_dst.joinpath(f"{id}.{store_format}")

        df = self.get_dataset_as_df(
            id, engine, cast_datatypes, resource_metadata, batch_size, **kwargs
        )

        # TODO add save file options inside the client configuration
        if isinstance(df, pl.DataFrame):
            match store_format:
                case "csv":
                    df.write_csv(file_name)
                case "parquet":
                    df.write_parquet(
                        file_name,
                        compression_level=parquet_compression_level,
                    )
                case "json":
                    df.write_json(file_name)

        elif isinstance(df, pd.DataFrame):
            match store_format:
                case "csv":
                    df.to_csv(file_name, index=False)
                case "parquet":
                    df.to_parquet(file_name, index=False)
                case "json":
                    df.to_json(file_name, index=False)

        if return_dataframe:
            return df

    def clone(self):
        return deepcopy(self)
