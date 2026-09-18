from __future__ import annotations

import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.modules.setdefault(
    "wrapt_timeout_decorator",
    types.SimpleNamespace(timeout=lambda *_args, **_kwargs: lambda func: func),
)
sys.modules.setdefault(
    "tqdm",
    types.SimpleNamespace(tqdm=lambda iterable=None, **_kwargs: iterable),
)

from ulod.bulk.configurations import ODSDownloadConfig
from ulod.bulk.ods import (
    download_tabular_resources,
    fetch_metadata,
    filter_retrieved_metadata,
)


class FakeODSClient:
    def __init__(self):
        self.export_ids = []

    def export_dataset_in_format(self, dataset_id: str, format: str):
        self.export_ids.append(dataset_id)
        return f"downloaded:{dataset_id}:{format}"


class FakeMetadataClient:
    def __init__(self, dataset_ids: list[str]):
        self.dataset_ids = dataset_ids
        self.calls = []

    def catalog_datasets(self, limit: int, offset: int):
        self.calls.append((limit, offset))
        if limit == 0:
            return {"total_count": len(self.dataset_ids), "results": []}

        return {
            "results": [
                {"dataset_id": dataset_id}
                for dataset_id in self.dataset_ids[offset : offset + limit]
            ]
        }


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


def fake_write_data_to_disk(client, dataset_id, destination, format):
    response = client.export_dataset_in_format(dataset_id, format)
    Path(destination, f"{dataset_id}.{format}").write_text(response)


class ODSBulkTests(unittest.TestCase):
    def _cfg(self, destination: Path, **kwargs) -> ODSDownloadConfig:
        cfg = ODSDownloadConfig(
            download_destination=destination,
            download_format="csv",
            verbose=False,
            **kwargs,
        )
        cfg.datasets_folder_path = destination / "datasets" / "csv"
        cfg.datasets_folder_path.mkdir(parents=True)
        cfg.log_folder_path = destination / "log" / "download" / "run"
        cfg.log_folder_path.mkdir(parents=True)
        cfg.metadata_path = destination / "metadata" / "metadata.json"
        return cfg

    def test_download_handles_fewer_datasets_than_workers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = self._cfg(destination, max_workers=8)
            client = FakeODSClient()
            logger = FakeLogger()

            with patch(
                "ulod.bulk.ods.init_logger",
                return_value=(logger, FakeListener()),
            ):
                with patch(
                    "ulod.bulk.ods.write_data_to_disk",
                    side_effect=fake_write_data_to_disk,
                ):
                    _work, success_count = download_tabular_resources(
                        ["first", "second"],
                        cfg,
                        client,
                    )

            self.assertEqual(success_count, 2)
            self.assertEqual(set(client.export_ids), {"first", "second"})
            self.assertEqual(
                (cfg.datasets_folder_path / "first.csv").read_text(),
                "downloaded:first:csv",
            )
            self.assertEqual(
                (cfg.datasets_folder_path / "second.csv").read_text(),
                "downloaded:second:csv",
            )

    def test_skip_existing_datasets_downloads_only_missing_final_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = self._cfg(
                destination,
                max_workers=4,
                skip_existing_datasets=True,
            )
            existing_file = cfg.datasets_folder_path / "existing.csv"
            existing_file.write_text("already here")

            client = FakeODSClient()
            logger = FakeLogger()

            with patch(
                "ulod.bulk.ods.init_logger",
                return_value=(logger, FakeListener()),
            ):
                with patch(
                    "ulod.bulk.ods.write_data_to_disk",
                    side_effect=fake_write_data_to_disk,
                ):
                    _work, success_count = download_tabular_resources(
                        ["existing", "missing"],
                        cfg,
                        client,
                    )

            self.assertEqual(success_count, 1)
            self.assertEqual(client.export_ids, ["missing"])
            self.assertIn("[DATASET:existing][SKIPPED EXISTING]", logger.messages)
            self.assertEqual(existing_file.read_text(), "already here")
            self.assertEqual(
                (cfg.datasets_folder_path / "missing.csv").read_text(),
                "downloaded:missing:csv",
            )

    def test_filter_retrieved_metadata_writes_only_downloaded_datasets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = self._cfg(destination)
            (cfg.datasets_folder_path / "downloaded.csv").write_text("ok")

            metadata = [
                {"dataset_id": "downloaded", "title": "Downloaded"},
                {"dataset_id": "missing", "title": "Missing"},
            ]

            retrieved = filter_retrieved_metadata(metadata, cfg)

            output_path = cfg.metadata_path.parent / "metadata_retrieved_only.json"
            self.assertEqual(retrieved, [metadata[0]])
            self.assertEqual(json.loads(output_path.read_text()), [metadata[0]])

    def test_fetch_metadata_limits_from_starting_offset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = self._cfg(
                destination,
                from_dataset_index=1,
                max_datasets=2,
                batch_fetch_metadata=1,
            )
            client = FakeMetadataClient(["first", "second", "third", "fourth"])

            dataset_ids, metadata = fetch_metadata(cfg, client)

            self.assertEqual(client.calls, [(0, 0), (1, 1), (1, 2)])
            self.assertEqual(dataset_ids, ["second", "third"])
            self.assertEqual(
                metadata,
                [{"dataset_id": "second"}, {"dataset_id": "third"}],
            )


if __name__ == "__main__":
    unittest.main()
