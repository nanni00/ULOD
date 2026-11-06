# Tabular download examples

Run from the repository root after installing ULOD:

```sh
python examples/example_ckan_tabular.py
python examples/example_socrata_tabular.py
python examples/example_ods_tabular.py
```

Each script has three editable limits:

- `MAX_DATASETS = 50`: inspect the first 50 catalog entries/packages, before filtering.
- `MAX_DOWNLOADS = 10`: select at most 10 matching data files. Existing files and
  failed attempts also consume this budget; fewer than 10 new files may result.
- `MAX_WORKERS = 2`: at most two concurrent download **threads** within a dataset.
  ULOD does not launch parallel processes or process datasets concurrently.

CKAN selects CSV, TSV, Excel and Parquet files. Socrata/ODS select CSV exports and
CSV attachments. ZIPs and generic JSON are excluded because they are not
necessarily tabular. Files are preserved in their original formats.

The shared `tabular_filter.py` uses `resource_filter` to cap matching files;
`max_datasets` alone would not cap files in multi-resource CKAN packages. Create a
new predicate for each run. The examples retain metadata for every inspected
entry, even after the file budget is reached.

Outputs go under `downloads/`. A bounded run normally has
`report.discovery_complete=False` and `report.success=False` because it deliberately
does not mirror the whole catalog. Inspect the download counters and manifest for
individual outcomes. Remove `max_datasets` to discover the full catalog while
keeping the file budget.
