"""The watermark: which publication of each file the pipeline has processed.

ops.etl_watermark holds one row per source file: the publication time and size
of the archive it came from, as the Volume's manifest recorded them, and the
batch of the gold checks that passed on it. The gold checks notebook writes it
last, and only when every check matched its expectation, so a file is in it
once bronze, silver, gold and both sets of checks hold it.

A month of the window is pending when the manifest dates its archive differently
from the watermark, or the watermark does not hold it: a month new to the
pipeline, or one republished since it was processed. Every notebook from bronze
to the gold checks works out the pending months the same way before writing
anything, and processes those only. The watermark changes only at the end, so
a run that stops leaves them pending for the next.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from config import MANIFEST_NAME, archive_name, csv_name, months_in_window
from manifest import Publication, read_manifest

WATERMARK_SCHEMA = (
    "source_file string, period string, published_at string, archive_bytes bigint, "
    "batch_id string, processed_at timestamp"
)


def landed_csv_paths(landing: Path, periods: Sequence[str]) -> list[Path]:
    """The CSV of each month in the landing Volume, in the order of periods."""
    return [landing / csv_name(period) for period in periods]


def pending_periods(
    periods: Sequence[str],
    *,
    manifest: dict[str, Publication],
    processed: dict[str, str],
) -> tuple[str, ...]:
    """The months of periods whose landed publication the watermark does not hold.

    manifest is the Volume's manifest, by archive; processed, the publication
    time the watermark holds for each file, by CSV name. Raises ValueError for
    a month whose archive the manifest does not name: the landing did not land
    it, and its publication is unknown.
    """
    unrecorded = []
    pending = []
    for period in periods:
        landed = manifest.get(archive_name(period))
        if landed is None:
            unrecorded.append(archive_name(period))
        elif processed.get(csv_name(period)) != landed.published_at:
            pending.append(period)
    if unrecorded:
        raise ValueError(
            f"Not in the Volume's manifest: {', '.join(unrecorded)}. "
            "Land them with the landing task"
        )
    return tuple(pending)


def read_watermark(spark: SparkSession, table: str) -> dict[str, str]:
    """The publication time of each file in the watermark, or nothing before the first."""
    if not spark.catalog.tableExists(table):
        return {}
    return {
        row["source_file"]: row["published_at"]
        for row in spark.table(table).select("source_file", "published_at").collect()
    }


def pending_in_window(
    spark: SparkSession, landing: Path, table: str, start: str, end: str | None
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The months of the window the Volume holds, and the pending ones among them.

    landing holds the manifest; table is the watermark. The window runs from
    start to end, or to the last month the manifest names.
    """
    manifest = read_manifest(landing / MANIFEST_NAME)
    window = months_in_window(manifest, start, end)
    pending = pending_periods(
        window, manifest=manifest, processed=read_watermark(spark, table)
    )
    return window, pending


def watermark_rows(
    current: Iterable[Sequence[Any]],
    periods: Sequence[str],
    *,
    manifest: dict[str, Publication],
    batch_id: str,
    processed_at: datetime,
) -> list[tuple[Any, ...]]:
    """The rows of the watermark once these months are recorded.

    current holds the rows of the table, in the order of WATERMARK_SCHEMA. Each
    month's file takes the publication the manifest records for its archive,
    in place of the row it had; the other files keep theirs.
    """
    rows = {row[0]: tuple(row) for row in current}
    for period in periods:
        landed = manifest[archive_name(period)]
        name = csv_name(period)
        rows[name] = (
            name,
            period,
            landed.published_at,
            landed.size,
            batch_id,
            processed_at,
        )
    return [rows[name] for name in sorted(rows)]


def record_watermark(
    spark: SparkSession,
    table: str,
    periods: Sequence[str],
    *,
    manifest: dict[str, Publication],
    batch_id: str,
    processed_at: datetime,
) -> None:
    """Record these months in the watermark, rewriting its few rows in one overwrite."""
    current = spark.table(table).collect() if spark.catalog.tableExists(table) else []
    rows = watermark_rows(
        current,
        periods,
        manifest=manifest,
        batch_id=batch_id,
        processed_at=processed_at,
    )
    (
        spark.createDataFrame(rows, WATERMARK_SCHEMA)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table)
    )
