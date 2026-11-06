"""Exercise real SDKs and the public workflow with deterministic HTTP responses."""

from __future__ import annotations

import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import requests

from ulod import DownloadConfig, download_catalog
from ulod.bulk.common import HTTP, infer_format, normalize_format, safe_name


class Response(requests.Response):
    def __init__(self, body=b"", status=200, headers=None, interrupted=False):
        super().__init__()
        self.status_code = status
        self._content = (
            json.dumps(body).encode() if isinstance(body, (list, dict)) else body
        )
        self._content_consumed = True
        if isinstance(body, (list, dict)):
            self.headers["Content-Type"] = "application/json"
        self.headers.update(headers or {})
        self.url = "https://portal.test"
        self.interrupted = interrupted

    def iter_content(self, chunk_size=1, **kwargs):
        if self.interrupted:
            yield b"partial"
            raise requests.ConnectionError("stream interrupted")
        yield from super().iter_content(chunk_size, **kwargs)


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.destination = Path(self.tmp.name)
        self.calls = []

    def cfg(self, **kwargs):
        return DownloadConfig(
            self.destination, retries=0, retry_backoff=0, max_workers=1, **kwargs
        )

    def route(self, handler):
        def get(session, url, **kwargs):
            params = dict(parse_qs(urlparse(url).query))
            supplied = kwargs.get("params") or {}
            params.update(dict(supplied))
            self.calls.append((urlparse(url).path, params, kwargs))
            return handler(urlparse(url).path, params, kwargs)

        patcher = patch.object(requests.Session, "get", get)
        patcher.start()
        self.addCleanup(patcher.stop)

    def ckan(self, datasets, payloads=None, page_hook=None):
        payloads = payloads or {}

        def handler(path, params, kwargs):
            if path.endswith("package_search"):
                if page_hook:
                    special = page_hook(params)
                    if special is not None:
                        return special
                start = params["start"]
                result = {
                    "count": len(datasets),
                    "results": datasets[start : start + params["rows"]],
                }
                return Response({"success": True, "result": result})
            if path.endswith("package_show"):
                result = next(d for d in datasets if d["id"] == params["id"])
                return Response({"success": True, "result": result})
            body = payloads[path]
            if isinstance(body, list):
                return body.pop(0)
            return body if isinstance(body, Response) else Response(body)

        self.route(handler)

    def manifest(self):
        return json.loads((self.destination / "manifest.json").read_text())

    def test_ckan_mixed_formats_raw_zip_and_nested_paths(self):
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w") as archive:
            archive.writestr("data.csv", "a,b\n1,2\n")
        zipped = stream.getvalue()
        metadata = [
            {
                "id": "package-one",
                "custom": {"keep": True},
                "resources": [
                    {
                        "id": "csv-id",
                        "url": "https://portal.test/data.csv",
                        "format": "TEXT/CSV; charset=utf-8",
                    },
                    {
                        "id": "zip-id",
                        "url": "https://portal.test/data.zip",
                        "format": "CSV",
                    },
                    {
                        "id": "pdf-id",
                        "url": "https://portal.test/data.pdf",
                        "format": "PDF",
                    },
                ],
            },
            {
                "id": "package-two",
                "resources": [
                    {
                        "id": "image",
                        "url": "https://portal.test/photo.png",
                        "format": "image/png",
                    }
                ],
            },
        ]
        self.ckan(metadata, {"/data.csv": b"a,b\n1,2\n", "/data.zip": zipped})
        report = download_catalog(
            "ckan",
            "https://portal.test",
            self.cfg(formats={"CSV", ".ZIP"}, page_size=1),
        )
        self.assertTrue(report.success)
        self.assertEqual(
            (report.datasets, report.downloaded, report.filtered), (2, 2, 2)
        )
        self.assertEqual(
            (self.destination / "datasets/package-one/zip-id.zip").read_bytes(), zipped
        )
        self.assertEqual(
            json.loads(
                (self.destination / "datasets/package-one/metadata.json").read_text()
            ),
            metadata[0],
        )
        self.assertTrue(
            (self.destination / "datasets/package-two/metadata.json").exists()
        )
        self.assertFalse(any("___" in str(p) for p in self.destination.rglob("*")))
        self.assertEqual(
            [
                p["start"]
                for path, p, _ in self.calls
                if path.endswith("package_search")
            ],
            [0, 1],
        )
        self.assertEqual(
            len(json.loads((self.destination / "catalog.json").read_text())["entries"]),
            2,
        )

    def test_unfiltered_download_accepts_non_tabular_and_large_files(self):
        body = b"\x00\xff" * (1024 * 1024)
        self.ckan(
            [
                {
                    "id": "pkg",
                    "resources": [
                        {
                            "id": "binary",
                            "url": "https://portal.test/image.png",
                            "format": "PNG",
                        }
                    ],
                }
            ],
            {"/image.png": body},
        )
        report = download_catalog("ckan", "https://portal.test", self.cfg())
        self.assertTrue(report.success)
        self.assertEqual(
            (self.destination / "datasets/pkg/binary.png").read_bytes(), body
        )

    def test_resume_hash_validation_and_metadata_refresh(self):
        datasets = [
            {
                "id": "pkg",
                "resources": [
                    {"id": "r", "url": "https://portal.test/data.csv", "format": "CSV"}
                ],
            }
        ]
        self.ckan(datasets, {"/data.csv": b"abc"})
        first = download_catalog("ckan", "https://portal.test", self.cfg())
        second = download_catalog("ckan", "https://portal.test", self.cfg())
        self.assertTrue(first.success and second.success)
        self.assertEqual(second.existing, 1)
        self.assertEqual(sum(path == "/data.csv" for path, _, _ in self.calls), 1)
        (self.destination / "datasets/pkg/r.csv").write_bytes(b"bad")
        third = download_catalog("ckan", "https://portal.test", self.cfg())
        self.assertEqual(third.downloaded, 1)
        datasets[0]["metadata_modified"] = "updated"
        fourth = download_catalog(
            "ckan", "https://portal.test", self.cfg(refresh_metadata=True)
        )
        self.assertEqual(fourth.downloaded, 1)

    def test_retries_restart_partial_stream_and_recover_next_run(self):
        datasets = [
            {
                "id": "pkg",
                "resources": [
                    {"id": "r", "url": "https://portal.test/data.csv", "format": "csv"}
                ],
            }
        ]
        responses = [Response(interrupted=True), Response(b"complete")]
        self.ckan(datasets, {"/data.csv": responses})
        cfg = self.cfg()
        cfg.retries = 1
        report = download_catalog("ckan", "https://portal.test", cfg)
        self.assertTrue(report.success)
        self.assertEqual(
            (self.destination / "datasets/pkg/r.csv").read_bytes(), b"complete"
        )
        self.assertFalse(list(self.destination.rglob("*.part")))
        (self.destination / "datasets/pkg/r.csv").unlink()
        responses.append(Response(interrupted=True))
        failed = download_catalog("ckan", "https://portal.test", self.cfg())
        self.assertFalse(failed.success)
        self.assertEqual(failed.failed, 1)
        self.assertFalse((self.destination / "datasets/pkg/r.csv").exists())
        responses.append(Response(b"recovered"))
        resumed = download_catalog("ckan", "https://portal.test", self.cfg())
        self.assertTrue(resumed.success)

    def test_discovery_failure_keeps_checkpoint_and_cannot_report_success(self):
        datasets = [{"id": "one", "resources": []}, {"id": "two", "resources": []}]
        fail = [True]

        def page_hook(params):
            if params["start"] == 1 and fail[0]:
                return Response({"error": "server down"}, status=503)

        self.ckan(datasets, page_hook=page_hook)
        failed = download_catalog("ckan", "https://portal.test", self.cfg(page_size=1))
        self.assertFalse(failed.discovery_complete)
        self.assertFalse(failed.success)
        checkpoint = json.loads((self.destination / "catalog.json").read_text())
        self.assertEqual(len(checkpoint["entries"]), 1)
        fail[0] = False
        self.calls.clear()
        resumed = download_catalog("ckan", "https://portal.test", self.cfg(page_size=1))
        self.assertTrue(resumed.discovery_complete)
        self.assertEqual(
            [
                p["start"]
                for path, p, _ in self.calls
                if path.endswith("package_search")
            ],
            [1],
        )

    def test_repeated_pages_are_incomplete(self):
        datasets = [{"id": "one", "resources": []}, {"id": "two", "resources": []}]

        def page_hook(params):
            return Response(
                {"success": True, "result": {"count": 2, "results": datasets[:1]}}
            )

        self.ckan(datasets, page_hook=page_hook)
        report = download_catalog("ckan", "https://portal.test", self.cfg(page_size=1))
        self.assertFalse(report.discovery_complete)
        self.assertIn("repeated", report.errors[0])

    def test_limits_predicates_and_unsupported_resources(self):
        self.ckan(
            [
                {
                    "id": "pkg",
                    "resources": [
                        {
                            "id": "big",
                            "url": "https://portal.test/big.csv",
                            "format": "csv",
                        },
                        {
                            "id": "missing",
                            "url": "https://portal.test/missing.pdf",
                            "format": "pdf",
                        },
                        {
                            "id": "service",
                            "url": "https://portal.test/wms",
                            "format": "WMS",
                        },
                        {
                            "id": "filtered",
                            "url": "https://portal.test/no.csv",
                            "format": "csv",
                        },
                    ],
                }
            ],
            {
                "/big.csv": Response(b"too large", headers={"Content-Length": "9"}),
                "/missing.pdf": Response(status=404),
            },
        )
        report = download_catalog(
            "ckan",
            "https://portal.test",
            self.cfg(
                max_resource_size=3, resource_filter=lambda r: r["id"] != "filtered"
            ),
        )
        self.assertEqual(
            (report.limited, report.unavailable, report.unsupported, report.filtered),
            (1, 1, 1, 1),
        )
        self.assertFalse(report.success)
        self.assertFalse(any(p == "/no.csv" for p, _, _ in self.calls))
        filtered = download_catalog(
            "ckan", "https://portal.test", self.cfg(dataset_filter=lambda _: False)
        )
        self.assertTrue(filtered.success)
        self.assertEqual(filtered.filtered, 4)

    def test_max_datasets_does_not_claim_entire_catalog_complete(self):
        self.ckan([{"id": "one", "resources": []}, {"id": "two", "resources": []}])
        report = download_catalog(
            "ckan", "https://portal.test", self.cfg(max_datasets=1)
        )
        self.assertEqual(report.datasets, 1)
        self.assertFalse(report.discovery_complete)

    def test_ods_exports_attachment_alternative_and_metadata(self):
        detail = {
            "dataset_id": "ods",
            "has_records": True,
            "alternative_exports": [
                {
                    "id": "native",
                    "url": "https://portal.test/native.zip",
                    "title": "native.zip",
                }
            ],
        }
        base = "/api/explore/v2.1/catalog/datasets"

        def handler(path, params, kwargs):
            if path == base:
                self.assertLessEqual(params["limit"], 100)
                return Response({"total_count": 1, "results": [detail]})
            if path == base + "/ods":
                return Response(detail)
            if path == base + "/ods/exports":
                return Response(
                    {
                        "links": [
                            {"rel": fmt, "href": "https://portal.test/export." + fmt}
                            for fmt in ["json", "csv"]
                        ]
                    }
                )
            if path == base + "/ods/attachments":
                return Response(
                    {
                        "attachments": [
                            {
                                "href": "https://portal.test/notes.pdf",
                                "metas": {
                                    "id": "notes",
                                    "title": "notes.pdf",
                                    "mimetype": "application/pdf",
                                },
                            }
                        ]
                    }
                )
            return Response(b"bytes")

        self.route(handler)
        report = download_catalog("ods", "https://portal.test", self.cfg())
        self.assertTrue(report.success)
        self.assertEqual(report.downloaded, 3)
        self.assertFalse(any(p == "/export.json" for p, _, _ in self.calls))
        self.assertTrue((self.destination / "datasets/ods/attachments.json").exists())
        self.calls.clear()
        report = download_catalog(
            "ods", "https://portal.test", self.cfg(formats={"json"})
        )
        self.assertTrue(report.success)
        self.assertEqual(report.downloaded, 1)
        self.assertTrue(any(p == "/export.json" for p, _, _ in self.calls))

    def test_ods_attachment_failure_is_metadata_incomplete(self):
        base = "/api/explore/v2.1/catalog/datasets"

        def handler(path, params, kwargs):
            if path == base:
                return Response({"total_count": 1, "results": [{"dataset_id": "one"}]})
            if path == base + "/one":
                return Response({"dataset_id": "one", "has_records": False})
            return Response(status=503)

        self.route(handler)
        report = download_catalog("ods", "https://portal.test", self.cfg())
        self.assertFalse(report.success)
        self.assertFalse(report.metadata_complete)
        self.assertIn("Attachment discovery", report.errors[0])

    def test_socrata_all_types_pagination_exports_files_attachments(self):
        catalog = [
            {"resource": {"id": i, "type": kind}}
            for i, kind in [("table", "dataset"), ("file", "file"), ("story", "story")]
        ]

        def handler(path, params, kwargs):
            if path == "/api/catalog/v1":
                self.assertNotIn("only", params)
                self.assertEqual(params["domains"], "portal.test")
                start = params["offset"]
                return Response(
                    {"resultSetSize": 3, "results": catalog[start : start + 2]}
                )
            if path == "/api/views/table.json":
                return Response({"id": "table", "viewType": "tabular", "metadata": {}})
            if path == "/api/views/file.json":
                return Response(
                    {
                        "id": "file",
                        "viewType": "blobby",
                        "blobId": "b",
                        "blobFilename": "data.zip",
                        "blobMimeType": "application/zip",
                        "metadata": {
                            "attachments": [
                                {"assetId": "older", "filename": "older.zip"}
                            ]
                        },
                    }
                )
            if path == "/api/views/story.json":
                return Response({"id": "story", "viewType": "story", "metadata": {}})
            return Response(b"raw")

        self.route(handler)
        report = download_catalog(
            "socrata", "https://portal.test", self.cfg(page_size=2)
        )
        self.assertTrue(report.discovery_complete)
        self.assertEqual((report.downloaded, report.unsupported), (3, 1))
        self.assertFalse(report.success)
        self.assertEqual(
            [p["offset"] for path, p, _ in self.calls if path == "/api/catalog/v1"],
            [0, 2],
        )
        self.assertTrue((self.destination / "datasets/file/file.zip").exists())
        self.assertTrue(
            (self.destination / "datasets/file/attachment-older.zip").exists()
        )
        self.assertTrue(
            any(p == "/api/v3/views/table/export.csv" for p, _, _ in self.calls)
        )
        self.assertTrue(any(p == "/api/views/file/files/b" for p, _, _ in self.calls))
        self.assertTrue(all("$limit" not in p for _, p, _ in self.calls))

    def test_html_and_truncated_responses_are_not_successful_files(self):
        self.ckan(
            [
                {
                    "id": "pkg",
                    "resources": [
                        {
                            "id": "html",
                            "url": "https://portal.test/landing",
                            "format": "csv",
                        },
                        {
                            "id": "short",
                            "url": "https://portal.test/short",
                            "format": "pdf",
                        },
                    ],
                }
            ],
            {
                "/landing": Response(b"<html>", headers={"Content-Type": "text/html"}),
                "/short": Response(b"ab", headers={"Content-Length": "10"}),
            },
        )
        report = download_catalog("ckan", "https://portal.test", self.cfg())
        self.assertEqual((report.unsupported, report.failed), (1, 1))
        self.assertFalse(list(self.destination.rglob("*.part")))
        self.assertFalse((self.destination / "datasets/pkg/short.pdf").exists())

    def test_destination_identity_and_safe_names(self):
        original = {
            "source": "ods",
            "url": "https://other.test",
            "entries": [],
            "complete": True,
        }
        (self.destination / "catalog.json").write_text(json.dumps(original))
        with self.assertRaises(ValueError):
            download_catalog("ckan", "https://portal.test", self.cfg())
        self.assertEqual(
            json.loads((self.destination / "catalog.json").read_text()), original
        )
        self.assertFalse((self.destination / "manifest.json").exists())
        self.assertNotIn("/", safe_name("../../escape"))
        self.assertNotEqual(safe_name(".."), "..")
        self.assertNotEqual(safe_name("a/b"), safe_name("a%2Fb"))

    def test_format_aliases(self):
        self.assertEqual(normalize_format("application/x-zip-compressed"), "zip")
        self.assertEqual(infer_format("https://x/data.zip", "CSV"), "zip")
        self.assertEqual(
            normalize_format("image/jpeg"),
            "jpg"
            if __import__("mimetypes").guess_extension("image/jpeg") == ".jpg"
            else "jpe",
        )
        self.assertEqual(normalize_format("application/octet-stream"), "")

    def test_credentials_not_forwarded_to_external_redirects(self):
        cfg = self.cfg(headers={"X-Private": "secret"}, api_key="token")
        seen = []

        def handler(path, params, kwargs):
            seen.append(dict(kwargs["headers"]))
            if path == "/redirect":
                return Response(
                    status=302, headers={"Location": "https://outside.test/file"}
                )
            return Response(b"ok")

        self.route(handler)
        http = HTTP("https://portal.test", cfg, "ckan")
        try:
            with http.get("https://portal.test/redirect") as response:
                self.assertEqual(response.content, b"ok")
        finally:
            http.close()
        self.assertEqual(seen[0]["Authorization"], "token")
        self.assertEqual(seen[1], {})

    def test_rate_limit_honors_retry_after(self):
        responses = [
            Response(status=429, headers={"Retry-After": "2"}),
            Response({"ok": True}),
        ]
        self.route(lambda *_: responses.pop(0))
        cfg = self.cfg()
        cfg.retries = 1
        http = HTTP("https://portal.test", cfg, "ods")
        try:
            with patch("ulod.bulk.common.time.sleep") as sleep:
                self.assertEqual(http.json("https://portal.test/test"), {"ok": True})
                sleep.assert_called_once_with(2.0)
        finally:
            http.close()


if __name__ == "__main__":
    unittest.main()
