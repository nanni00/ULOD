"""Collect basic statistics for crawled open-data table collections.

The script reads Parquet metadata through Polars and writes summary files to
the project root by default. It never writes into the source dataset tree.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import polars as pl


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_ROOT = Path("/data/datasets/open_data")
BYTES_PER_MB = 1024 * 1024


@dataclass(frozen=True)
class TableStats:
    collection: str
    table_name: str
    table_path: Path
    file_size_mb: float
    row_count: int | None
    column_count: int | None
    is_non_empty: bool | None
    estimated_cell_count: int | None
    column_names: list[str]
    status: str
    error: str


@dataclass(frozen=True)
class CollectionStats:
    collection: str
    collection_path: Path
    table_count: int
    readable_table_count: int
    unreadable_table_count: int
    non_empty_table_count: int
    empty_table_count: int
    non_empty_table_percent: float | None
    total_rows: int
    total_columns: int
    total_cells: int
    non_empty_total_rows: int
    non_empty_total_columns: int
    non_empty_total_cells: int
    collection_size_mb: float
    parquet_size_mb: float
    row_min: int | None
    row_mean: float | None
    row_median: float | None
    row_max: int | None
    column_min: int | None
    column_mean: float | None
    column_median: float | None
    column_max: int | None
    non_empty_row_min: int | None
    non_empty_row_mean: float | None
    non_empty_row_median: float | None
    non_empty_row_max: int | None
    non_empty_column_min: int | None
    non_empty_column_mean: float | None
    non_empty_column_median: float | None
    non_empty_column_max: int | None
    top_largest: list[TableStats]
    top_tallest: list[TableStats]
    top_widest: list[TableStats]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report statistics for Parquet tables in open-data collections.",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help=f"Root containing collection directories. Default: {DEFAULT_DATA_ROOT}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT,
        help=f"Directory where reports are written. Default: {PROJECT_ROOT}",
    )
    parser.add_argument(
        "--collections",
        nargs="+",
        help="Optional collection names to scan. Defaults to all collection directories.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of largest/tallest/widest tables to show in the Markdown report.",
    )
    return parser.parse_args()


def bytes_to_mb(size_bytes: int) -> float:
    return size_bytes / BYTES_PER_MB


def rounded_mb(size_bytes: int) -> float:
    return round(bytes_to_mb(size_bytes), 3)


def truncate_error(error: Exception, limit: int = 500) -> str:
    text = str(error).replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def discover_collections(data_root: Path, requested: list[str] | None) -> list[Path]:
    if not data_root.exists():
        raise SystemExit(f"Data root does not exist: {data_root}")
    if not data_root.is_dir():
        raise SystemExit(f"Data root is not a directory: {data_root}")

    if requested:
        collection_paths = [data_root / collection for collection in requested]
        missing = [path.name for path in collection_paths if not path.is_dir()]
        if missing:
            raise SystemExit(
                "Requested collection directories do not exist: "
                + ", ".join(sorted(missing)),
            )
        return collection_paths

    return sorted(path for path in data_root.iterdir() if path.is_dir())


def iter_files(path: Path) -> Iterable[Path]:
    for child in path.rglob("*"):
        if child.is_file():
            yield child


def directory_size_bytes(path: Path) -> int:
    return sum(file_path.stat().st_size for file_path in iter_files(path))


def read_table_stats(collection: str, table_path: Path) -> TableStats:
    file_size_mb = rounded_mb(table_path.stat().st_size)

    try:
        lazy_frame = pl.scan_parquet(table_path)
        schema = lazy_frame.collect_schema()
        row_count = int(
            lazy_frame.select(pl.len().cast(pl.UInt64).alias("row_count"))
            .collect()
            .item(),
        )
        column_names = list(schema.names())
        column_count = len(column_names)
        is_non_empty = row_count > 0 and column_count > 0
        return TableStats(
            collection=collection,
            table_name=table_path.name,
            table_path=table_path,
            file_size_mb=file_size_mb,
            row_count=row_count,
            column_count=column_count,
            is_non_empty=is_non_empty,
            estimated_cell_count=row_count * column_count,
            column_names=column_names,
            status="ok",
            error="",
        )
    except Exception as exc:
        return TableStats(
            collection=collection,
            table_name=table_path.name,
            table_path=table_path,
            file_size_mb=file_size_mb,
            row_count=None,
            column_count=None,
            is_non_empty=None,
            estimated_cell_count=None,
            column_names=[],
            status="error",
            error=truncate_error(exc),
        )


def numeric_summary(
    values: list[int],
) -> tuple[int | None, float | None, float | None, int | None]:
    if not values:
        return None, None, None, None
    return min(values), statistics.fmean(values), statistics.median(values), max(values)


def summarize_collection(
    collection_path: Path,
    table_stats: list[TableStats],
    top_n: int,
) -> CollectionStats:
    readable = [table for table in table_stats if table.status == "ok"]
    non_empty = [table for table in readable if table.is_non_empty]
    row_counts = [table.row_count for table in readable if table.row_count is not None]
    column_counts = [
        table.column_count for table in readable if table.column_count is not None
    ]
    non_empty_row_counts = [
        table.row_count for table in non_empty if table.row_count is not None
    ]
    non_empty_column_counts = [
        table.column_count for table in non_empty if table.column_count is not None
    ]
    row_min, row_mean, row_median, row_max = numeric_summary(row_counts)
    column_min, column_mean, column_median, column_max = numeric_summary(column_counts)
    non_empty_row_min, non_empty_row_mean, non_empty_row_median, non_empty_row_max = (
        numeric_summary(non_empty_row_counts)
    )
    (
        non_empty_column_min,
        non_empty_column_mean,
        non_empty_column_median,
        non_empty_column_max,
    ) = numeric_summary(non_empty_column_counts)

    parquet_size_bytes = sum(table.table_path.stat().st_size for table in table_stats)
    non_empty_table_percent = (
        len(non_empty) / len(readable) * 100 if readable else None
    )

    return CollectionStats(
        collection=collection_path.name,
        collection_path=collection_path,
        table_count=len(table_stats),
        readable_table_count=len(readable),
        unreadable_table_count=len(table_stats) - len(readable),
        non_empty_table_count=len(non_empty),
        empty_table_count=len(readable) - len(non_empty),
        non_empty_table_percent=non_empty_table_percent,
        total_rows=sum(row_counts),
        total_columns=sum(column_counts),
        total_cells=sum(
            table.estimated_cell_count
            for table in readable
            if table.estimated_cell_count is not None
        ),
        non_empty_total_rows=sum(non_empty_row_counts),
        non_empty_total_columns=sum(non_empty_column_counts),
        non_empty_total_cells=sum(
            table.estimated_cell_count
            for table in non_empty
            if table.estimated_cell_count is not None
        ),
        collection_size_mb=rounded_mb(directory_size_bytes(collection_path)),
        parquet_size_mb=rounded_mb(parquet_size_bytes),
        row_min=row_min,
        row_mean=row_mean,
        row_median=row_median,
        row_max=row_max,
        column_min=column_min,
        column_mean=column_mean,
        column_median=column_median,
        column_max=column_max,
        non_empty_row_min=non_empty_row_min,
        non_empty_row_mean=non_empty_row_mean,
        non_empty_row_median=non_empty_row_median,
        non_empty_row_max=non_empty_row_max,
        non_empty_column_min=non_empty_column_min,
        non_empty_column_mean=non_empty_column_mean,
        non_empty_column_median=non_empty_column_median,
        non_empty_column_max=non_empty_column_max,
        top_largest=sorted(readable, key=lambda table: table.file_size_mb, reverse=True)[
            :top_n
        ],
        top_tallest=sorted(
            readable,
            key=lambda table: table.row_count or 0,
            reverse=True,
        )[:top_n],
        top_widest=sorted(
            readable,
            key=lambda table: table.column_count or 0,
            reverse=True,
        )[:top_n],
    )


def format_int(value: int | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:,}"


def format_float(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.{digits}f}"


def escape_markdown_cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def markdown_table(headers: list[str], rows: list[list[object]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append(
            "| " + " | ".join(escape_markdown_cell(value) for value in row) + " |",
        )
    return lines


def table_preview_rows(tables: list[TableStats]) -> list[list[object]]:
    return [
        [
            table.table_name,
            format_int(table.row_count),
            format_int(table.column_count),
            format_float(table.file_size_mb, 3),
        ]
        for table in tables
    ]


def write_table_csv(output_path: Path, table_stats: list[TableStats]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "collection",
                "table_name",
                "table_path",
                "file_size_mb",
                "row_count",
                "column_count",
                "is_non_empty",
                "estimated_cell_count",
                "column_names",
                "status",
                "error",
            ],
        )
        writer.writeheader()
        for table in table_stats:
            writer.writerow(
                {
                    "collection": table.collection,
                    "table_name": table.table_name,
                    "table_path": str(table.table_path),
                    "file_size_mb": table.file_size_mb,
                    "row_count": table.row_count if table.row_count is not None else "",
                    "column_count": (
                        table.column_count if table.column_count is not None else ""
                    ),
                    "is_non_empty": (
                        table.is_non_empty if table.is_non_empty is not None else ""
                    ),
                    "estimated_cell_count": (
                        table.estimated_cell_count
                        if table.estimated_cell_count is not None
                        else ""
                    ),
                    "column_names": json.dumps(table.column_names, ensure_ascii=False),
                    "status": table.status,
                    "error": table.error,
                },
            )


def write_collection_markdown(
    output_path: Path,
    collection_stats: list[CollectionStats],
    top_n: int,
    data_root: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Open Data Collection Statistics",
        "",
        f"Source data root: `{data_root}`",
        "",
        "## Collections",
        "",
    ]
    lines.extend(
        markdown_table(
            [
                "Collection",
                "Tables",
                "Readable",
                "Unreadable",
                "Non-empty",
                "Empty",
                "Non-empty %",
                "Rows",
                "Columns",
                "Cells",
                "Collection MB",
                "Parquet MB",
            ],
            [
                [
                    stats.collection,
                    format_int(stats.table_count),
                    format_int(stats.readable_table_count),
                    format_int(stats.unreadable_table_count),
                    format_int(stats.non_empty_table_count),
                    format_int(stats.empty_table_count),
                    format_float(stats.non_empty_table_percent),
                    format_int(stats.total_rows),
                    format_int(stats.total_columns),
                    format_int(stats.total_cells),
                    format_float(stats.collection_size_mb, 3),
                    format_float(stats.parquet_size_mb, 3),
                ]
                for stats in collection_stats
            ],
        ),
    )

    for stats in collection_stats:
        lines.extend(
            [
                "",
                f"## {stats.collection}",
                "",
                f"- Collection path: `{stats.collection_path}`",
                f"- Tables: {format_int(stats.table_count)}",
                f"- Readable tables: {format_int(stats.readable_table_count)}",
                f"- Unreadable tables: {format_int(stats.unreadable_table_count)}",
                f"- Non-empty tables: {format_int(stats.non_empty_table_count)}",
                f"- Empty tables: {format_int(stats.empty_table_count)}",
                f"- Non-empty share: {format_float(stats.non_empty_table_percent)}%",
                f"- Total rows: {format_int(stats.total_rows)}",
                f"- Total columns: {format_int(stats.total_columns)}",
                f"- Estimated cells: {format_int(stats.total_cells)}",
                f"- Non-empty table rows: {format_int(stats.non_empty_total_rows)}",
                f"- Non-empty table columns: {format_int(stats.non_empty_total_columns)}",
                f"- Non-empty table cells: {format_int(stats.non_empty_total_cells)}",
                f"- Collection disk usage: {format_float(stats.collection_size_mb, 3)} MB",
                f"- Parquet table disk usage: {format_float(stats.parquet_size_mb, 3)} MB",
                "",
            ],
        )
        lines.extend(
            markdown_table(
                ["Metric", "Min", "Mean", "Median", "Max"],
                [
                    [
                        "Rows per table",
                        format_int(stats.row_min),
                        format_float(stats.row_mean),
                        format_float(stats.row_median),
                        format_int(stats.row_max),
                    ],
                    [
                        "Columns per table",
                        format_int(stats.column_min),
                        format_float(stats.column_mean),
                        format_float(stats.column_median),
                        format_int(stats.column_max),
                    ],
                    [
                        "Rows per non-empty table",
                        format_int(stats.non_empty_row_min),
                        format_float(stats.non_empty_row_mean),
                        format_float(stats.non_empty_row_median),
                        format_int(stats.non_empty_row_max),
                    ],
                    [
                        "Columns per non-empty table",
                        format_int(stats.non_empty_column_min),
                        format_float(stats.non_empty_column_mean),
                        format_float(stats.non_empty_column_median),
                        format_int(stats.non_empty_column_max),
                    ],
                ],
            ),
        )

        for title, tables in [
            (f"Top {top_n} Largest Tables", stats.top_largest),
            (f"Top {top_n} Tallest Tables", stats.top_tallest),
            (f"Top {top_n} Widest Tables", stats.top_widest),
        ]:
            lines.extend(["", f"### {title}", ""])
            if tables:
                lines.extend(
                    markdown_table(
                        ["Table", "Rows", "Columns", "File MB"],
                        table_preview_rows(tables),
                    ),
                )
            else:
                lines.append("No readable tables found.")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def scan_collection(collection_path: Path, top_n: int) -> tuple[list[TableStats], CollectionStats]:
    parquet_dir = collection_path / "datasets" / "parquet"
    table_paths = sorted(parquet_dir.glob("*.parquet")) if parquet_dir.is_dir() else []

    table_stats = [
        read_table_stats(collection_path.name, table_path) for table_path in table_paths
    ]
    return table_stats, summarize_collection(collection_path, table_stats, top_n)


def main() -> None:
    args = parse_args()
    if args.top_n < 1:
        raise SystemExit("--top-n must be at least 1")

    data_root = args.data_root.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()

    if output_dir == data_root or data_root in output_dir.parents:
        raise SystemExit(
            "Refusing to write reports inside the source dataset tree: "
            f"{output_dir}",
        )

    collections = discover_collections(data_root, args.collections)
    if not collections:
        raise SystemExit(f"No collection directories found under: {data_root}")

    all_table_stats: list[TableStats] = []
    all_collection_stats: list[CollectionStats] = []

    for collection_path in collections:
        print(f"Scanning collection: {collection_path.name}", flush=True)
        table_stats, collection_stats = scan_collection(collection_path, args.top_n)
        all_table_stats.extend(table_stats)
        all_collection_stats.append(collection_stats)
        print(
            "  "
            f"{collection_stats.readable_table_count:,}/"
            f"{collection_stats.table_count:,} readable tables, "
            f"{collection_stats.collection_size_mb:,.3f} MB",
            flush=True,
        )

    table_csv_path = output_dir / "table_statistics.csv"
    collection_markdown_path = output_dir / "collection_statistics.md"
    write_table_csv(table_csv_path, all_table_stats)
    write_collection_markdown(
        collection_markdown_path,
        all_collection_stats,
        args.top_n,
        data_root,
    )

    print(f"Wrote table statistics: {table_csv_path}")
    print(f"Wrote collection summary: {collection_markdown_path}")


if __name__ == "__main__":
    main()
