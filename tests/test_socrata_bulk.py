from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.modules.setdefault("pandas", types.SimpleNamespace(DataFrame=object))
sys.modules.setdefault("requests", types.SimpleNamespace(get=None))
sys.modules.setdefault(
    "polars",
    types.SimpleNamespace(
        Date=object,
        Datetime=object,
        DataFrame=object,
        Float32=object,
        Int32=object,
        String=object,
    ),
)
sys.modules.setdefault("sodapy", types.SimpleNamespace(Socrata=object))
sys.modules.setdefault(
    "tqdm", types.SimpleNamespace(tqdm=lambda iterable=None, **_kwargs: iterable)
)

from ulod.bulk.configurations import SocrataDownloadConfig
from ulod.bulk.socrata import download_tabular_resources


def metadata(dataset_id: str):
    return {"resource": {"id": dataset_id}}


class FakeSocrataClient:
    def __init__(self):
        self.export_ids = []

    def download_dataset_export(
        self,
        id,
        store_dst,
        store_format,
        engine,
        chunk_size,
        limit,
        parquet_compression_level,
        keep_intermediate_files,
    ):
        self.export_ids.append(id)
        Path(store_dst, f"{id}.{store_format}").write_bytes(b"downloaded")


class FakeLogger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)

    def error(self, message):
        self.messages.append(message)


class FakeListener:
    def start(self):
        pass

    def stop(self):
        pass


class SocrataBulkTests(unittest.TestCase):
    def test_skip_existing_datasets_downloads_only_missing_final_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = SocrataDownloadConfig(
                download_destination=destination,
                download_format="parquet",
                download_strategy="export",
                engine="polars",
                skip_existing_datasets=True,
                verbose=False,
            )
            cfg.datasets_folder_path = destination / "datasets" / "parquet"
            cfg.datasets_folder_path.mkdir(parents=True)
            cfg.log_folder_path = destination / "log" / "download" / "run"
            cfg.log_folder_path.mkdir(parents=True)

            existing_file = cfg.datasets_folder_path / "existing.parquet"
            existing_file.write_bytes(b"already here")

            client = FakeSocrataClient()
            logger = FakeLogger()
            with patch(
                "ulod.bulk.socrata.init_logger",
                return_value=(logger, FakeListener()),
            ):
                _work, success_count = download_tabular_resources(
                    [metadata("existing"), metadata("missing")],
                    cfg,
                    client,
                )

            self.assertEqual(success_count, 1)
            self.assertEqual(client.export_ids, ["missing"])
            self.assertIn("[DATASET:existing][SKIPPED EXISTING]", logger.messages)
            self.assertEqual(existing_file.read_bytes(), b"already here")
            self.assertEqual(
                (cfg.datasets_folder_path / "missing.parquet").read_bytes(),
                b"downloaded",
            )


if __name__ == "__main__":
    unittest.main()
