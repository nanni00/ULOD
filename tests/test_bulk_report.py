from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from ulod.bulk.utils import write_download_report


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
