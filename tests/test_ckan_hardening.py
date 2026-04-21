from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from threading import Lock
import sys
import types
from unittest.mock import patch

sys.modules.setdefault(
    "wrapt_timeout_decorator",
    types.SimpleNamespace(timeout=lambda _seconds: (lambda func: func)),
)
sys.modules.setdefault("tqdm", types.SimpleNamespace(tqdm=lambda iterable=None, **_kwargs: iterable))

from ulod.bulk.ckan import (
    CKANRequestPolicy,
    EdgeProtectionBlockedError,
    RequestCoordinator,
    _NullLogger,
    _request_json_with_retries,
    _save_metadata_checkpoint,
    fetch_metadata,
)
from ulod.bulk.configurations import CKANDownloadConfig
from ulod.countries.spain import Madrid
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

    def __init__(self) -> None:
        self.calls = []

    def package_search(self, *, start: int, rows: int, **_kwargs):
        self.calls.append((start, rows))
        if rows == 0:
            return {"result": {"count": 4}}

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


class CKANHardeningTests(unittest.TestCase):
    def test_madrid_reuses_same_session_for_warmup_metadata_and_downloads(self):
        client = Madrid(headers={"User-Agent": "test-agent"}, connection_kw={"timeout": 5})
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
        self.assertEqual([call["stream"] for call in fake_session.calls], [False, False, True])
        self.assertTrue(all(call["headers"]["Accept-Language"] for call in fake_session.calls))

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


if __name__ == "__main__":
    unittest.main()
