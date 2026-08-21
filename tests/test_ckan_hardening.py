from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from threading import Lock
import sys
import types
from unittest.mock import patch

sys.modules.setdefault(
    "requests",
    types.SimpleNamespace(Session=lambda: None, Response=object),
)
sys.modules.setdefault(
    "urllib3",
    types.SimpleNamespace(request=lambda *_args, **_kwargs: None),
)
sys.modules.setdefault(
    "wrapt_timeout_decorator",
    types.SimpleNamespace(timeout=lambda _seconds: (lambda func: func)),
)
sys.modules.setdefault(
    "tqdm",
    types.SimpleNamespace(tqdm=lambda iterable=None, **_kwargs: iterable),
)

from ulod.bulk.ckan import (
    CKANRequestPolicy,
    EdgeProtectionBlockedError,
    RequestCoordinator,
    _NullLogger,
    _request_json_with_retries,
    _save_metadata_checkpoint,
    download_tabular_resources,
    fetch_metadata,
    stream_data_to_disk,
)
from ulod.bulk.configurations import CKANDownloadConfig
from ulod.ckan import Madrid, StreamResponse
from ulod.utils.exceptions import HTTPResourceError


class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        json_data=None,
        chunks=None,
        headers=None,
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data if json_data is not None else {}
        self._chunks = chunks if chunks is not None else []
        self.headers = headers or {}
        self.closed = False

    def json(self):
        return self._json_data

    def iter_content(self, _chunk_size: int):
        yield from self._chunks

    def close(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(self, responses) -> None:
        self._responses = list(responses)
        self.calls = []
        self.closed = False

    def get(self, url, headers=None, stream=False, **kwargs):
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "stream": stream,
                "kwargs": kwargs,
            }
        )
        return self._responses.pop(0)

    def close(self) -> None:
        self.closed = True


class FakeMetadataClient:
    base_url = "https://example.test"

    def __init__(self, resources_by_start=None, count: int = 4) -> None:
        self.calls = []
        self.resources_by_start = resources_by_start
        self.count = count

    def package_search(self, *, start: int, rows: int, **_kwargs):
        self.calls.append((start, rows))
        if rows == 0:
            return {"result": {"count": self.count}}

        if self.resources_by_start is not None:
            return {"result": {"results": self.resources_by_start.get(start, [])}}

        return {
            "result": {
                "results": [
                    {
                        "resources": [
                            {
                                "url": f"/download/{start}.csv",
                                "id": f"resource-{start}",
                                "name": f"resource {start}",
                            }
                        ]
                    }
                ]
            }
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


class FakeDownloadClient:
    def __init__(self, statuses=None, headers=None):
        self.statuses = statuses or {}
        self.headers = headers or {}
        self.stream_urls = []

    def stream_request(self, url: str):
        self.stream_urls.append(url)
        status = self.statuses.get(url, 200)
        return StreamResponse(
            status=status,
            headers=self.headers.get(url, {"Content-Length": "4"}),
            _iter_content=lambda _chunk_size: iter([b"data"]),
            _close=lambda: None,
        )


class CKANHardeningTests(unittest.TestCase):
    def test_max_resource_size_accepts_compact_human_readable_values(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)

            cases = {
                "100B": 100,
                "512KB": 512 * 1024,
                "10MB": 10 * 1024**2,
                "1GB": 1024**3,
                "10mb": 10 * 1024**2,
                2048: 2048,
                None: None,
            }

            for value, expected in cases.items():
                with self.subTest(value=value):
                    cfg = CKANDownloadConfig(
                        download_destination=destination,
                        max_resource_size=value,
                    )
                    self.assertEqual(cfg.max_resource_size, expected)

    def test_max_resource_size_rejects_invalid_values(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)

            for value in ["", "10 MB", "1.5GB", "-1MB", "5TB", -1, True]:
                with self.subTest(value=value):
                    with self.assertRaises(ValueError):
                        CKANDownloadConfig(
                            download_destination=destination,
                            max_resource_size=value,
                        )

    def test_skip_resource_statuses_normalizes_and_validates_status_codes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = CKANDownloadConfig(
                download_destination=destination,
                skip_resource_statuses=[403, 403, 429],
            )

            self.assertEqual(cfg.skip_resource_statuses, (403, 429))

            for value in [99, 600, "403", True, [403, "429"]]:
                with self.subTest(value=value):
                    with self.assertRaises(ValueError):
                        CKANDownloadConfig(
                            download_destination=destination,
                            skip_resource_statuses=value,
                        )

    def test_accept_zip_files_is_not_implemented(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(NotImplementedError):
                CKANDownloadConfig(
                    download_destination=Path(tmpdir),
                    accept_zip_files=True,
                )

    def test_madrid_reuses_same_session_for_warmup_metadata_and_downloads(self):
        client = Madrid(
            headers={"User-Agent": "test-agent"},
            connection_kw={"timeout": 5},
        )
        fake_session = FakeSession(
            [
                FakeResponse(status_code=200),
                FakeResponse(status_code=200, json_data={"result": {"count": 1}}),
                FakeResponse(
                    status_code=200,
                    chunks=[b"col1,col2\n", b"1,2\n"],
                    headers={"Content-Length": "14"},
                ),
            ]
        )
        client._session = fake_session
        client._session_lock = Lock()

        client.warmup_session()
        metadata = client.package_search(start=0, rows=0)
        stream = client.stream_request("https://datos.madrid.es/dataset/test.csv")
        chunks = list(stream.iter_content(16))
        stream.close()

        self.assertEqual(metadata["result"]["count"], 1)
        self.assertEqual(chunks, [b"col1,col2\n", b"1,2\n"])
        self.assertEqual(
            [call["stream"] for call in fake_session.calls],
            [False, False, True],
        )
        self.assertTrue(
            all(call["headers"]["Accept-Language"] for call in fake_session.calls)
        )

    def test_retry_backoff_resets_after_success(self):
        coordinator = RequestCoordinator(
            CKANRequestPolicy(
                retry_backoff_base_s=2.0,
                cooldown_on_403_s=5.0,
                max_consecutive_403=3,
            ),
            sleep_fn=lambda *_args: None,
            jitter_fn=lambda _start, _end: 0.0,
        )

        first = coordinator.register_status(403, 1)
        coordinator.register_status(200, 1)
        second = coordinator.register_status(403, 1)

        self.assertTrue(first.retry)
        self.assertEqual(first.delay_s, 7.0)
        self.assertTrue(second.retry)
        self.assertEqual(second.delay_s, 7.0)

    def test_retries_503_then_succeeds(self):
        coordinator = RequestCoordinator(
            CKANRequestPolicy(retry_backoff_base_s=2.0),
            sleep_fn=lambda *_args: None,
            jitter_fn=lambda _start, _end: 0.0,
        )
        attempts = iter(
            [
                HTTPResourceError("https://example.test/api", 503),
                {"result": {"count": 1}},
            ]
        )

        def operation():
            result = next(attempts)
            if isinstance(result, Exception):
                raise result
            return result

        with patch("ulod.bulk.ckan.time.sleep") as sleep_mock:
            result = _request_json_with_retries(
                operation,
                coordinator,
                _NullLogger(),
                "package_search",
            )

        self.assertEqual(result["result"]["count"], 1)
        sleep_mock.assert_called_once_with(2.0)

    def test_stops_after_configured_consecutive_403s(self):
        coordinator = RequestCoordinator(
            CKANRequestPolicy(
                retry_backoff_base_s=2.0,
                cooldown_on_403_s=5.0,
                max_consecutive_403=2,
            ),
            sleep_fn=lambda *_args: None,
            jitter_fn=lambda _start, _end: 0.0,
        )

        def operation():
            raise HTTPResourceError("https://example.test/api", 403)

        with patch("ulod.bulk.ckan.time.sleep"):
            with self.assertRaises(EdgeProtectionBlockedError):
                _request_json_with_retries(
                    operation,
                    coordinator,
                    _NullLogger(),
                    "package_search",
                )

    def test_fetch_metadata_resumes_from_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            metadata_dir = destination / "metadata"
            metadata_dir.mkdir(parents=True, exist_ok=True)

            cfg = CKANDownloadConfig(
                download_destination=destination,
                max_datasets=4,
                batch_fetch_metadata=2,
                save_with_resource_name=False,
                verbose=False,
            )
            cfg.metadata_path = metadata_dir / "metadata.json"

            _save_metadata_checkpoint(
                cfg,
                [("resource-0", "https://example.test/download/0.csv")],
                [{"resources": [{"id": "resource-0"}], "num_resources": 1}],
                next_start=2,
            )

            client = FakeMetadataClient()
            coordinator = RequestCoordinator(
                CKANRequestPolicy(),
                sleep_fn=lambda *_args: None,
                jitter_fn=lambda _start, _end: 0.0,
            )

            resource_ids_urls, full_metadata = fetch_metadata(
                cfg,
                client,
                coordinator,
            )

        self.assertEqual(client.calls, [(0, 0), (2, 2)])
        self.assertEqual(len(resource_ids_urls), 2)
        self.assertEqual(len(full_metadata), 2)

    def test_fetch_metadata_limits_resources_not_packages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = CKANDownloadConfig(
                download_destination=destination,
                max_datasets=3,
                batch_fetch_metadata=1,
                save_with_resource_name=False,
                verbose=False,
            )
            cfg.metadata_path = destination / "metadata" / "metadata.json"
            cfg.metadata_path.parent.mkdir(parents=True)

            resources_by_start = {
                0: [
                    {
                        "resources": [
                            {"url": "/a.csv", "id": "a", "name": "A"},
                            {"url": "/b.csv", "id": "b", "name": "B"},
                        ]
                    }
                ],
                1: [
                    {
                        "resources": [
                            {"url": "/c.csv", "id": "c", "name": "C"},
                            {"url": "/d.csv", "id": "d", "name": "D"},
                        ]
                    }
                ],
                2: [
                    {
                        "resources": [
                            {"url": "/e.csv", "id": "e", "name": "E"},
                        ]
                    }
                ],
            }
            client = FakeMetadataClient(resources_by_start, count=3)
            coordinator = RequestCoordinator(
                CKANRequestPolicy(),
                sleep_fn=lambda *_args: None,
                jitter_fn=lambda _start, _end: 0.0,
            )

            resource_ids_urls, full_metadata = fetch_metadata(
                cfg,
                client,
                coordinator,
            )

        self.assertEqual(client.calls, [(0, 0), (0, 1), (1, 1)])
        self.assertEqual(
            resource_ids_urls,
            [
                ("a", "https://example.test/a.csv"),
                ("b", "https://example.test/b.csv"),
                ("c", "https://example.test/c.csv"),
            ],
        )
        self.assertEqual(len(full_metadata), 2)
        self.assertEqual([pkg["num_resources"] for pkg in full_metadata], [2, 1])

    def test_fetch_metadata_minus_one_means_unlimited_resources(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = CKANDownloadConfig(
                download_destination=destination,
                max_datasets=-1,
                batch_fetch_metadata=1,
                save_with_resource_name=False,
                verbose=False,
            )
            cfg.metadata_path = destination / "metadata" / "metadata.json"
            cfg.metadata_path.parent.mkdir(parents=True)

            resources_by_start = {
                0: [{"resources": [{"url": "/a.csv", "id": "a", "name": "A"}]}],
                1: [{"resources": [{"url": "/b.csv", "id": "b", "name": "B"}]}],
            }
            client = FakeMetadataClient(resources_by_start, count=2)
            coordinator = RequestCoordinator(
                CKANRequestPolicy(),
                sleep_fn=lambda *_args: None,
                jitter_fn=lambda _start, _end: 0.0,
            )

            resource_ids_urls, _full_metadata = fetch_metadata(
                cfg,
                client,
                coordinator,
            )

        self.assertEqual(client.calls, [(0, 0), (0, 1), (1, 1)])
        self.assertEqual(len(resource_ids_urls), 2)

    def test_download_queue_downloads_all_resources_with_more_resources_than_workers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = CKANDownloadConfig(
                download_destination=destination,
                max_workers=2,
                verbose=False,
            )
            cfg.datasets_folder_path = destination / "datasets"
            cfg.log_folder_path = destination / "logs"
            cfg.log_folder_path.mkdir()
            client = FakeDownloadClient()
            coordinator = RequestCoordinator(
                CKANRequestPolicy(),
                sleep_fn=lambda *_args: None,
                jitter_fn=lambda _start, _end: 0.0,
            )
            logger = FakeLogger()

            with patch(
                "ulod.bulk.ckan.init_logger",
                return_value=(logger, FakeListener()),
            ):
                _work, success_count = download_tabular_resources(
                    [
                        ("one", "https://example.test/one.csv"),
                        ("two", "https://example.test/two.csv"),
                        ("three", "https://example.test/three.csv"),
                    ],
                    cfg,
                    client,
                    coordinator,
                )

        self.assertEqual(success_count, 3)
        self.assertEqual(
            sorted(client.stream_urls),
            [
                "https://example.test/one.csv",
                "https://example.test/three.csv",
                "https://example.test/two.csv",
            ],
        )

    def test_download_queue_handles_fewer_resources_than_workers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = CKANDownloadConfig(
                download_destination=destination,
                max_workers=5,
                verbose=False,
            )
            cfg.datasets_folder_path = destination / "datasets"
            cfg.log_folder_path = destination / "logs"
            cfg.log_folder_path.mkdir()
            client = FakeDownloadClient()
            coordinator = RequestCoordinator(
                CKANRequestPolicy(),
                sleep_fn=lambda *_args: None,
                jitter_fn=lambda _start, _end: 0.0,
            )

            with patch(
                "ulod.bulk.ckan.init_logger",
                return_value=(FakeLogger(), FakeListener()),
            ):
                _work, success_count = download_tabular_resources(
                    [("one", "https://example.test/one.csv")],
                    cfg,
                    client,
                    coordinator,
                )

        self.assertEqual(success_count, 1)
        self.assertEqual(client.stream_urls, ["https://example.test/one.csv"])

    def test_failed_resource_does_not_stop_unrelated_downloads(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = CKANDownloadConfig(
                download_destination=destination,
                max_workers=2,
                verbose=False,
            )
            cfg.datasets_folder_path = destination / "datasets"
            cfg.log_folder_path = destination / "logs"
            cfg.log_folder_path.mkdir()
            client = FakeDownloadClient(
                statuses={"https://example.test/bad.csv": 500},
            )
            coordinator = RequestCoordinator(
                CKANRequestPolicy(),
                sleep_fn=lambda *_args: None,
                jitter_fn=lambda _start, _end: 0.0,
            )
            logger = FakeLogger()

            with patch(
                "ulod.bulk.ckan.init_logger",
                return_value=(logger, FakeListener()),
            ):
                _work, success_count = download_tabular_resources(
                    [
                        ("good-one", "https://example.test/good-one.csv"),
                        ("bad", "https://example.test/bad.csv"),
                        ("good-two", "https://example.test/good-two.csv"),
                    ],
                    cfg,
                    client,
                    coordinator,
                )

        self.assertEqual(success_count, 2)
        self.assertTrue(any("[RESOURCE:bad]" in msg for msg in logger.messages))

    def test_configured_statuses_skip_without_retry_or_global_403_abort(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = CKANDownloadConfig(
                download_destination=destination,
                max_workers=1,
                skip_resource_statuses=(403,),
                verbose=False,
            )
            cfg.datasets_folder_path = destination / "datasets"
            cfg.log_folder_path = destination / "logs"
            cfg.log_folder_path.mkdir()
            client = FakeDownloadClient(
                statuses={
                    "https://example.test/protected.csv": 403,
                    "https://example.test/open.csv": 200,
                },
            )
            coordinator = RequestCoordinator(
                CKANRequestPolicy(max_consecutive_403=1),
                sleep_fn=lambda *_args: None,
                jitter_fn=lambda _start, _end: 0.0,
            )
            logger = FakeLogger()

            with patch(
                "ulod.bulk.ckan.init_logger",
                return_value=(logger, FakeListener()),
            ):
                _work, success_count = download_tabular_resources(
                    [
                        ("protected", "https://example.test/protected.csv"),
                        ("open", "https://example.test/open.csv"),
                    ],
                    cfg,
                    client,
                    coordinator,
                )

        self.assertEqual(success_count, 1)
        self.assertEqual(
            client.stream_urls,
            [
                "https://example.test/protected.csv",
                "https://example.test/open.csv",
            ],
        )
        self.assertTrue(
            any("skipping HTTP 403 without retry" in msg for msg in logger.messages)
        )
        self.assertTrue(any("[RESOURCE:protected]" in msg for msg in logger.messages))

    def test_oversized_resource_is_skipped_and_malformed_content_length_is_ignored(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = CKANDownloadConfig(
                download_destination=destination,
                max_resource_size="3B",
                max_workers=1,
                verbose=False,
            )
            cfg.datasets_folder_path = destination / "datasets"
            cfg.log_folder_path = destination / "logs"
            cfg.log_folder_path.mkdir()
            client = FakeDownloadClient(
                headers={
                    "https://example.test/large.csv": {"Content-Length": "4"},
                    "https://example.test/unknown.csv": {"Content-Length": "n/a"},
                },
            )
            logger = FakeLogger()
            coordinator = RequestCoordinator(
                CKANRequestPolicy(),
                sleep_fn=lambda *_args: None,
                jitter_fn=lambda _start, _end: 0.0,
            )

            with patch(
                "ulod.bulk.ckan.init_logger",
                return_value=(logger, FakeListener()),
            ):
                _work, success_count = download_tabular_resources(
                    [
                        ("large", "https://example.test/large.csv"),
                        ("unknown", "https://example.test/unknown.csv"),
                    ],
                    cfg,
                    client,
                    coordinator,
                )
            unknown_file_exists = (destination / "datasets" / "unknown.csv").exists()

        self.assertEqual(success_count, 1)
        self.assertTrue(any("[RESOURCE:large]" in msg for msg in logger.messages))
        self.assertTrue(unknown_file_exists)

    def test_zip_payloads_are_not_implemented(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            response = StreamResponse(
                status=200,
                headers={},
                _iter_content=lambda _chunk_size: iter([b"\x50\x4b\x03\x04data"]),
                _close=lambda: None,
            )

            with self.assertRaises(NotImplementedError):
                stream_data_to_disk(response, "archive", Path(tmpdir), "csv")


if __name__ == "__main__":
    unittest.main()
