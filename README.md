# UseLess Open Data

ULOD downloads open-data catalogs, original metadata and native files from CKAN,
Socrata and OpenDataSoft. It does not convert dataframes or restrict discovery to
tabular datasets. ZIPs, PDFs, images and other published files remain unchanged.

Requires Python 3.13+. Install with `pip install -e .` or `uv sync`.

## Download a catalog

```python
from ulod import DownloadConfig, download_catalog

report = download_catalog(
    "ckan",
    "https://open.canada.ca/data",
    DownloadConfig("downloads/canada", formats={"csv", "zip"}),
)
print(report)
print(report.success)
```

Use the same interface for all three sources:

| Source | Example portal root |
| --- | --- |
| `ckan` | `https://open.canada.ca/data` |
| `socrata` | `https://data.cityofnewyork.us` |
| `ods` | `https://opendata.paris.fr` |

The URL includes a deployment subpath, if any, but excludes the API endpoint.
Each destination belongs to one portal. Directories are created automatically.

## Selection and exports

- `formats=None` (default): accept all native file formats.
- `formats={"csv", "zip"}`: accept CSV **or** ZIP; this does not create ZIPs or
  convert another format into CSV. Extensions and MIME aliases are normalized.
- `formats=set()`: collect metadata without downloading data files.
- `dataset_filter` and `resource_filter`: predicates receiving the original
  dataset/resource metadata. Socrata's discovery entries are also in `catalog.json`.
  Generated export resources have a `format`/`url` descriptor; ODS uses its original
  export link descriptor. Metadata is preserved even for excluded datasets/files.
- There are no default dataset, row or file-size caps. Optional `max_datasets` and
  `max_resource_size` (bytes) explicitly bound a run.

Socrata and ODS download **one generated export per dataset**, plus all matching
files/attachments. `export_format="csv"` is preferred. If it is unavailable or
filtered out, the first allowed format in alphabetical order is chosen. Resource
predicates apply before export selection. No matching export means no generated
export is downloaded. Alternative ODS exports are native files and are retained.

Socrata uses SODA3 full exports (CSV, TSV, XLSX; KML/KMZ for spatial columns), native
geospatial exports and file attachments. It does not use the deprecated SODA1
`rows` endpoints. Public full exports can be anonymous; provide `app_token` for
portal quotas or `access_token` for OAuth access. JSON record querying is separate
from the supported full file exports. See the [SODA3 migration guide](https://support.socrata.com/hc/en-us/articles/43491231777047-Using-the-SODA3-API).

CKAN uses `ckanapi.RemoteCKAN` for package discovery and full metadata. Socrata uses
`sodapy` for full asset metadata, with explicit catalog pagination because its
fixed-size listing method rejects short final pages on larger catalogs. ODS uses
the [Explore v2.1 API](https://help.opendatasoft.com/apis/ods-explore-v2/) directly.

## Files and completion

```text
downloads/canada/
├── catalog.json                 # original catalog entries + discovery checkpoint
├── manifest.json                # aggregate resource URLs, paths, outcomes, hashes
├── download_report.json         # completion status and counters
└── datasets/
    └── <packageID>/
        ├── metadata.json        # full original package metadata
        ├── downloads.json       # incremental resource checkpoint
        ├── <resourceID>.csv
        └── <resourceID>.zip
```

Socrata/ODS use dataset IDs as folders too; exports are named `export-<format>.<ext>`
and attachments have stable IDs. ODS also saves original `exports.json` and
`attachments.json` responses. Unsafe identifier characters are percent-encoded;
IDs, URLs and original metadata remain in the manifest. Unknown formats use `.bin`.
Unknown formats do not match an explicit format allowlist.

`report.success` is true only when discovery, metadata and all selected downloads
are complete. Missing links, permissions, unsupported service/visualization assets,
size limits and failed transfers are visible in the manifest. Filtering does not
remove catalog metadata; intentionally filtered files are not failures. A
`max_datasets` run that covers only part of the catalog is explicitly incomplete.

Discovery checks totals and duplicate IDs; failures preserve a partial snapshot
and cannot report success. This is an API snapshot, not an atomic backup of a
changing server. Portal APIs may impose discovery caps; those failures are reported.
ULOD downloads advertised files/exports, not arbitrary websites linked in text or
external service databases. WMS/WFS and unsupported visualization types retain
metadata and an unsupported outcome.

## Resume and networking

- `resume=True`: reuse a completed file only if its metadata fingerprint, size and
  SHA-256 match. Dataset metadata is fetched again on each run.
- `refresh_metadata=True`: restart catalog discovery to include new/removed
  datasets. By default, discovery resumes/reuses `catalog.json`.
- Files are streamed to `.part`, then atomically renamed. An interrupted file
  restarts from byte zero; completed files survive. Per-dataset checkpoints are
  updated after each transfer; the aggregate manifest is written when the run exits.
- Files left by earlier runs are not deleted when filters or remote catalogs change.
  The new manifest describes the current run. Old flat layouts are never migrated
  or treated as completed downloads automatically.
- Defaults: `max_workers=4`, `timeout=60`, `retries=3`, exponential
  `retry_backoff=1`, `request_delay=0`, `chunk_size=1048576`. Concurrency is across
  a dataset's resources; metadata is collected sequentially. Rate limits honor
  `Retry-After` for HTTP requests.
- Use `api_key` for CKAN/ODS, `app_token`/`access_token` for Socrata, and `headers`
  for custom portal headers. Portal credentials are not sent to external files.
  Supply credentials for each portal separately; do not run simultaneous jobs in
  the same destination.

Local filesystem errors and invalid configurations raise exceptions. Remote
failures are recorded in the returned report. Keyboard interrupts save checkpoints
and then propagate. An abrupt process termination may leave a `.part` file, which
will be overwritten on retry.

## Migration from 0.2

Version 0.3 replaces `CKANDownloadConfig`, `SocrataDownloadConfig`,
`ODSDownloadConfig`, the three old bulk functions and all CKAN/Socrata/ODS
client/portal classes with `DownloadConfig` and `download_catalog`.

Replace client construction with a source name and portal root. Use `formats` for
selection, `export_format` for generated exports, and `resource_filter` instead of
`filter_resource_metadata`. Remove `engine`, `cast_datatypes`, conversion options,
`accept_zip_files`, `save_with_resource_name`, and row limits. Dataset limits use
`None` for unlimited; file-size limits use integer bytes. Start a new output folder
for the new layout. Parquet conversion, if needed, is a separate downstream step.

WBO and UN are unchanged. Their imports and dependencies remain available.
Data.gov retains `USDownloadConfig`, `ulod.us.us.US`, and
`ulod.bulk.datagov_download_datasets`, including its existing layout and behavior.
The old snapshot descriptions in `README_uk.md` and `README_nyc.md`, and the
Parquet statistics script, describe historical collections.

## Examples and tests

```shell
python examples/example_ckan.py downloads/canada --formats csv zip
python examples/example_socrata.py downloads/nyc --formats csv zip
python examples/example_ods.py downloads/paris
python -m unittest discover -s tests -v
```

Each example accepts `--url`, `--max-datasets`, `--max-resource-size`, `--workers`
and `--refresh-metadata`. Omit `--formats` for all formats or pass it without values
for metadata only. Credentials come from `CKAN_API_KEY`, `ODS_API_KEY`,
`SOCRATA_APP_TOKEN` and `SOCRATA_ACCESS_TOKEN` environment variables.
