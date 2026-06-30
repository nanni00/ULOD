from __future__ import annotations

import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.modules.setdefault(
    "tqdm", types.SimpleNamespace(tqdm=lambda iterable=None, **_kwargs: iterable)
)

from ulod.bulk.configurations import USDownloadConfig
from ulod.bulk.datagov import datagov_download_datasets
from ulod.sources.ckan import StreamResponse


def dataset(identifier, modified, distributions):
    dcat = {"identifier": identifier, "distribution": distributions}
    if modified is not None:
        dcat["modified"] = modified
    return {"dcat": dcat}


def distribution(identifier, url, format_="CSV"):
    return {
        "identifier": identifier,
        "downloadURL": url,
        "format": format_,
    }


class FakeUS:
    def __init__(self, datasets, payloads):
        self.datasets = datasets
        self.payloads = payloads
        self.search_kwargs = []
        self.stream_urls = []

    def iter_datasets(self, **kwargs):
        self.search_kwargs.append(kwargs)
        yield from self.datasets

    def stream_request(self, url):
        self.stream_urls.append(url)
        payload = self.payloads[url]
        return StreamResponse(
            status=200,
            headers={"Content-Length": str(len(payload))},
            _iter_content=lambda _chunk_size: iter([payload]),
            _close=lambda: None,
        )


class DataGovBulkTests(unittest.TestCase):
    def test_downloads_raw_matching_formats_under_date_folder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            datasets = [
                dataset(
                    "dataset-one",
                    "2026-05-10",
                    [
                        distribution("csv-file", "https://example.test/data.csv", "CSV"),
                        distribution("json-file", "https://example.test/data.json", "JSON"),
                        distribution("xml-file", "https://example.test/data.xml", "XML"),
                    ],
                )
            ]
            client = FakeUS(
                datasets,
                {
                    "https://example.test/data.csv": b"a,b\n1,2\n",
                    "https://example.test/data.json": b'{"a": 1}\n',
                },
            )
            cfg = USDownloadConfig(
                download_destination=destination,
                formats=("csv", "json"),
                verbose=False,
            )

            with patch("ulod.bulk.datagov._run_date", return_value="11_05_26"):
                with patch("ulod.bulk.datagov._run_time", return_value="13_14_15"):
                    _work, success_count = datagov_download_datasets(cfg, client)

            run_root = destination / "11_05_26"
            self.assertEqual(success_count, 2)
            self.assertEqual(
                sorted(client.stream_urls),
                [
                    "https://example.test/data.csv",
                    "https://example.test/data.json",
                ],
            )
            self.assertEqual(
                (run_root / "datasets" / "csv" / "dataset-one__csv-file.csv").read_bytes(),
                b"a,b\n1,2\n",
            )
            self.assertEqual(
                (
                    run_root / "datasets" / "json" / "dataset-one__json-file.json"
                ).read_bytes(),
                b'{"a": 1}\n',
            )
            self.assertTrue(
                (run_root / "metadata" / "metadata_13_14_15.json").exists()
            )
            self.assertTrue(
                (run_root / "metadata" / "manifest_13_14_15.json").exists()
            )

    def test_updated_only_uses_previous_dataset_modified_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            previous_metadata = destination / "10_05_26" / "metadata"
            previous_metadata.mkdir(parents=True)
            with open(previous_metadata / "manifest_01_00_00.json", "w") as file:
                json.dump(
                    {
                        "datasets": {
                            "unchanged": {"modified": "2026-05-01"},
                            "changed": {"modified": "2026-05-01"},
                        }
                    },
                    file,
                )

            datasets = [
                dataset(
                    "unchanged",
                    "2026-05-01",
                    [distribution("r", "https://example.test/unchanged.csv")],
                ),
                dataset(
                    "changed",
                    "2026-05-02",
                    [distribution("r", "https://example.test/changed.csv")],
                ),
                dataset(
                    "new",
                    "2026-05-01",
                    [distribution("r", "https://example.test/new.csv")],
                ),
                dataset(
                    "missing",
                    None,
                    [distribution("r", "https://example.test/missing.csv")],
                ),
            ]
            client = FakeUS(
                datasets,
                {
                    "https://example.test/changed.csv": b"changed",
                    "https://example.test/new.csv": b"new",
                    "https://example.test/missing.csv": b"missing",
                },
            )
            cfg = USDownloadConfig(
                download_destination=destination,
                mode="updated-only",
                formats="csv",
                verbose=False,
            )

            with patch("ulod.bulk.datagov._run_date", return_value="11_05_26"):
                with patch("ulod.bulk.datagov._run_time", return_value="13_14_15"):
                    _work, success_count = datagov_download_datasets(cfg, client)

            self.assertEqual(success_count, 3)
            self.assertEqual(
                sorted(client.stream_urls),
                [
                    "https://example.test/changed.csv",
                    "https://example.test/missing.csv",
                    "https://example.test/new.csv",
                ],
            )
            self.assertFalse(
                (
                    destination
                    / "11_05_26"
                    / "datasets"
                    / "csv"
                    / "unchanged__r.csv"
                ).exists()
            )
            self.assertTrue(
                (
                    destination
                    / "11_05_26"
                    / "datasets"
                    / "csv"
                    / "missing__r.csv"
                ).exists()
            )


if __name__ == "__main__":
    unittest.main()
