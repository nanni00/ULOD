from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ulod.bulk.ckan import ckan_download_datasets
from ulod.bulk.configurations import (
    CKANDownloadConfig,
    ODSDownloadConfig,
    SocrataDownloadConfig,
)
from ulod.bulk.ods import ods_download_datasets
from ulod.bulk.socrata import socrata_download_datasets
from ulod.bulk.utils import write_download_report


class FakeCKANClient:
    def __init__(self):
        self.closed = False

    def default_bulk_download_policy(self):
        return {}

    def close(self):
        self.closed = True


class FakeLogger:
    def info(self, _message):
        pass


class FakeListener:
    def start(self):
        pass

    def stop(self):
        pass


class BulkDownloadReportTests(unittest.TestCase):
    def test_writes_brief_markdown_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)

            report_path = write_download_report(
                destination,
                source="ODS",
                started_at=datetime(2026, 9, 19, 8, 30, tzinfo=timezone.utc),
                elapsed_seconds=65.25,
                total_documents=10,
                successful_downloads=7,
                skipped_documents=2,
                retrieved_documents=9,
                output_format="csv",
                metadata_source="fetched from portal",
            )

            self.assertEqual(report_path, destination / "download_report.md")
            report = report_path.read_text(encoding="utf-8")
            self.assertIn("# Bulk download report", report)
            self.assertIn("| Source | ODS |", report)
            self.assertIn("| Started at | 2026-09-19T08:30:00+00:00 |", report)
            self.assertIn("| Total time | 1m 5.25s |", report)
            self.assertIn("| Total documents | 10 |", report)
            self.assertIn("| Download attempts | 8 |", report)
            self.assertIn("| Successful downloads | 7 |", report)
            self.assertIn("| Failed downloads | 1 |", report)
            self.assertIn("| Success rate | 87.5% |", report)
            self.assertIn("| Skipped existing documents | 2 |", report)
            self.assertIn("| Documents available after run | 9 |", report)

    def test_ods_bulk_download_writes_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = ODSDownloadConfig(
                destination,
                use_existing_metadata=False,
            )
            metadata = [{"dataset_id": "one"}, {"dataset_id": "two"}]

            with (
                patch(
                    "ulod.bulk.ods.fetch_metadata",
                    return_value=(["one", "two"], metadata),
                ),
                patch(
                    "ulod.bulk.ods.download_tabular_resources",
                    return_value=([], 1),
                ),
                patch(
                    "ulod.bulk.ods.filter_retrieved_metadata",
                    return_value=[metadata[0]],
                ),
            ):
                ods_download_datasets(cfg, object())

            report = (destination / "download_report.md").read_text()
            self.assertIn("| Source | ODS |", report)
            self.assertIn("| Total documents | 2 |", report)
            self.assertIn("| Successful downloads | 1 |", report)

    def test_socrata_bulk_download_writes_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = SocrataDownloadConfig(
                destination,
                use_existing_metadata=False,
            )
            metadata = [{"resource": {"id": "one"}}]

            with (
                patch("ulod.bulk.socrata.fetch_metadata", return_value=metadata),
                patch(
                    "ulod.bulk.socrata.download_tabular_resources",
                    return_value=([], 1),
                ),
                patch(
                    "ulod.bulk.socrata.filter_retrieved_metadata",
                    return_value=metadata,
                ),
            ):
                socrata_download_datasets(cfg, object())

            report = (destination / "download_report.md").read_text()
            self.assertIn("| Source | Socrata |", report)
            self.assertIn("| Successful downloads | 1 |", report)

    def test_ckan_bulk_download_writes_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir)
            cfg = CKANDownloadConfig(
                destination,
                use_existing_metadata=False,
            )
            client = FakeCKANClient()
            resources = [("one", "https://example.test/one.csv", "csv")]
            metadata = [{"resources": [{"id": "one"}]}]

            with (
                patch(
                    "ulod.bulk.ckan.init_logger",
                    return_value=(FakeLogger(), FakeListener()),
                ),
                patch(
                    "ulod.bulk.ckan.fetch_metadata",
                    return_value=(resources, metadata),
                ),
                patch(
                    "ulod.bulk.ckan.download_tabular_resources",
                    return_value=([], 1),
                ),
                patch("ulod.bulk.ckan.rename_resource_name_files_with_package_id"),
                patch(
                    "ulod.bulk.ckan.filter_retrieved_metadata",
                    return_value=metadata,
                ),
            ):
                ckan_download_datasets(cfg, client)

            report = (destination / "download_report.md").read_text()
            self.assertIn("| Source | CKAN |", report)
            self.assertIn("| Documents available after run | 1 |", report)
            self.assertTrue(client.closed)


if __name__ == "__main__":
    unittest.main()
