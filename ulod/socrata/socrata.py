from copy import deepcopy
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
import polars as pl
from sodapy import Socrata

from .utils import cast_socrata_types


class SocrataClient:
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
                new_rows = client.get(
                    dataset_identifier=id,
                    content_type=format,
                    limit=batch_size,
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

        cast_datatypes = (
            cast_datatypes if cast_datatypes and resource_metadata else False
        )
        datatypes = []
        dtypes_cast = {}
        columns = None

        if cast_datatypes:
            if not resource_metadata:
                resource_metadata = self.get_dataset_metadata(id)
                columns = resource_metadata["columns"]
            else:
                resource_metadata = resource_metadata["resource"]
                columns = (
                    {"fieldName": name, "dataTypeName": dtype, "format": format}
                    for name, dtype, format in zip(
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

            dtypes_cast = cast_socrata_types(datatypes, engine)

        match engine:
            case "polars":
                df = pl.from_records(data, dtypes_cast)

            case "pandas":
                if cast_datatypes:
                    columns = list(dtypes_cast.keys())
                df = pd.DataFrame(data, None, columns)

                if cast_datatypes:
                    for column, dtype in dtypes_cast.items():
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

        match engine:
            case "polars":
                match store_format:
                    case "csv":
                        df.write_csv(file_name)
                    case "parquet":
                        df.write_parquet(file_name)
                    case "json":
                        df.write_json(file_name)

            case "pandas":
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
