# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 - Gold quality checks (EU Trade Lakehouse)
# MAGIC
# MAGIC Runs the eight data quality checks of the gold layer: over both facts, per file of the
# MAGIC pending months, and over the dimensions they point to; check 08 over every month of the
# MAGIC facts. Each check of a fact writes one row per fact and file to `ops.dq_check_results`,
# MAGIC and each check of a dimension one row per dimension, without a file, next to the failures
# MAGIC it is expected to find. A count off its expectation, up or down, fails the run once the
# MAGIC results are written. When every one matches, the pending months go to the watermark.
# MAGIC
# MAGIC Responsibilities:
# MAGIC   1. Read the files of the pending months (`src/watermark.py`) from both facts and from
# MAGIC      silver, and stop if one is missing. With none pending, end without writing.
# MAGIC   2. Read the five dimensions, every version.
# MAGIC   3. Run every check: see `src/gold_checks.py`, which also declares the expected failures.
# MAGIC   4. Append the results to `ops.dq_check_results`, read them back and compare, by the rule
# MAGIC      of the silver checks.
# MAGIC   5. Record the publication of each pending month in `ops.etl_watermark`.
# MAGIC
# MAGIC Reads from:  the two facts and the five dimensions of `eu_trade.gold`, and
# MAGIC              `eu_trade.silver.trade_detail` and `eu_trade.silver.trade_totals`; the
# MAGIC              manifest in the Volume
# MAGIC Writes to:   `eu_trade.ops.dq_check_results` and `eu_trade.ops.etl_watermark`

# COMMAND ----------

# DBTITLE 1,Imports
import os
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# The notebook runs from the notebooks folder of the repository, in a Git
# folder or as a task of the Job. The code it shares with the other notebooks
# lives in src/, next to that folder. Run from anywhere else, the imports below
# would fail without saying why.
if not os.path.isfile("../src/config.py"):
    raise FileNotFoundError(
        f"{os.path.abspath('../src')} holds no config.py: run the notebook from "
        "the notebooks folder"
    )
sys.path.insert(0, os.path.abspath("../src"))

from config import (
    COUNTRY_DIM_TABLE,
    DETAIL_FACT_TABLE,
    DQ_RESULTS_TABLE,
    FLOW_DIM_TABLE,
    GOLD_SCHEMA,
    MANIFEST_NAME,
    OPS_SCHEMA,
    PERIOD_DIM_TABLE,
    PRODUCT_DIM_TABLE,
    SILVER_DETAIL_TABLE,
    SILVER_SCHEMA,
    SILVER_TOTALS_TABLE,
    STAT_PROCEDURE_DIM_TABLE,
    TOTALS_FACT_TABLE,
    VOLUME_PATH,
    WATERMARK_TABLE,
    qualified,
    window_from,
)
from gold_checks import CHECKS, EVERY_MONTH_CHECKS, all_checks, expected_failures
from manifest import read_manifest
from silver_checks import expected_bounds, mismatches
from watermark import landed_csv_paths, pending_in_window, record_watermark

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Run settings
# The window of months: from start_period to end_period, or to the last month
# the Volume holds while end_period is empty. The Job fills both from its
# parameters of the same names. Created in a cell of their own, because a
# widget created by a command that fails is never shown.
dbutils.widgets.text("start_period", "202601")
dbutils.widgets.text("end_period", "")

# COMMAND ----------

# DBTITLE 1,Parameters
start, end = window_from(
    dbutils.widgets.get("start_period"), dbutils.widgets.get("end_period")
)
# The tables the checks read, by the name all_checks() gives each: the facts
# per file of the pending months, the dimensions whole. Silver is what the
# facts are compared with.
fact_tables = {
    "detail": qualified(GOLD_SCHEMA, DETAIL_FACT_TABLE),
    "totals": qualified(GOLD_SCHEMA, TOTALS_FACT_TABLE),
}
silver_tables = {
    "detail": qualified(SILVER_SCHEMA, SILVER_DETAIL_TABLE),
    "totals": qualified(SILVER_SCHEMA, SILVER_TOTALS_TABLE),
}
dim_tables = {
    name: qualified(GOLD_SCHEMA, name)
    for name in (
        COUNTRY_DIM_TABLE,
        PRODUCT_DIM_TABLE,
        FLOW_DIM_TABLE,
        STAT_PROCEDURE_DIM_TABLE,
        PERIOD_DIM_TABLE,
    )
}
tables = {**fact_tables, **dim_tables}
results_table = qualified(OPS_SCHEMA, DQ_RESULTS_TABLE)
watermark_table = qualified(OPS_SCHEMA, WATERMARK_TABLE)
landing = Path(VOLUME_PATH)

# Every result of this run carries its batch, and the comparison reads back
# only those.
batch_id = str(uuid.uuid4())
run_at = datetime.now(UTC)

# A count is expected_failures; a range, expected_min and expected_max.
RESULTS_SCHEMA = (
    "check_name string, layer string, table_name string, source_file string, "
    "batch_id string, run_at timestamp, rows_checked bigint, rows_failed bigint, "
    "expected_failures bigint, severity string, expected_min bigint, "
    "expected_max bigint"
)

print(f"Results table: {results_table}")
print(f"Window: from {start} to {end or 'the last month landed'}")
print(f"Batch {batch_id} at {run_at.isoformat()}")

# COMMAND ----------

# DBTITLE 1,Pending months
# The same months 01_bronze_ingest loaded: the watermark changes only at the
# end of this notebook.
window, pending = pending_in_window(spark, landing, watermark_table, start, end)
print(f"Landed in the window: {', '.join(window)}")
print(f"Pending: {', '.join(pending) or 'none'}")
if not pending:
    message = "Nothing pending: every month of the window is processed"
    print(message)
    # The SDK types dbutils as a union with its remote proxy, whose notebook
    # utility takes no argument; the runtime's exit() takes the value.
    dbutils.notebook.exit(message)  # type: ignore[call-arg]

source_files = [path.name for path in landed_csv_paths(landing, pending)]

# COMMAND ----------

# DBTITLE 1,Read the files of the pending months from the facts and from silver
facts = {
    name: spark.table(table).filter(F.col("_source_file").isin(source_files))
    for name, table in fact_tables.items()
}
# Every month of each fact, for the checks that read them all, and its files.
every_month = {name: spark.table(table) for name, table in fact_tables.items()}
every_file = {
    name: sorted(
        row["_source_file"] for row in df.select("_source_file").distinct().collect()
    )
    for name, df in every_month.items()
}
print(f"Every month of the facts: {every_file['detail']}")
silver = {
    name: spark.table(table).filter(F.col("_source_file").isin(source_files))
    for name, table in silver_tables.items()
}
for table, df in (
    *((fact_tables[name], df) for name, df in facts.items()),
    *((silver_tables[name], df) for name, df in silver.items()),
):
    rows = {
        row["_source_file"]: row["count"]
        for row in df.groupBy("_source_file").count().collect()
    }
    missing = sorted(set(source_files) - rows.keys())
    if missing:
        raise AssertionError(
            f"Not in {table}: {missing}. "
            "Run 02_silver_transform and 04_gold_dimensional_model first"
        )
    print(f"{table}: " + ", ".join(f"{f} {rows[f]:,}" for f in source_files))

# COMMAND ----------

# DBTITLE 1,Read the dimensions
# Every version, whatever file its facts came from.
dims = {name: spark.table(table) for name, table in dim_tables.items()}
for name, df in dims.items():
    print(f"{dim_tables[name]}: {df.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Run the checks
# A check of a fact writes a row per file of the pending months, or of every
# month for the EVERY_MONTH_CHECKS, and a file it returns no row for was not
# evaluated: rows_failed is NULL. A check of a dimension writes one row,
# without a file.
results = []
for check_name, table, result in all_checks(
    detail=facts["detail"],
    totals=facts["totals"],
    every_detail=every_month["detail"],
    every_totals=every_month["totals"],
    silver_detail=silver["detail"],
    silver_totals=silver["totals"],
    dim_country=dims[COUNTRY_DIM_TABLE],
    dim_product=dims[PRODUCT_DIM_TABLE],
    dim_flow=dims[FLOW_DIM_TABLE],
    dim_stat_procedure=dims[STAT_PROCEDURE_DIM_TABLE],
    dim_period=dims[PERIOD_DIM_TABLE],
):
    started = time.perf_counter()
    by_file = {row["_source_file"]: row for row in result.collect()}
    print(f"{check_name:<30} {table:<18} {time.perf_counter() - started:6.1f} s")
    files: list[str | None]
    if table not in fact_tables:
        files = [None]
    elif check_name in EVERY_MONTH_CHECKS:
        files = [*every_file[table]]
    else:
        files = [*source_files]
    for source_file in files:
        counts = by_file.get(source_file)
        rows_checked = counts["rows_checked"] if counts else None
        expected, expected_min, expected_max = expected_bounds(
            expected_failures(check_name), rows_checked
        )
        results.append(
            (
                check_name,
                "gold",
                tables[table],
                source_file,
                batch_id,
                run_at,
                rows_checked,
                counts["rows_failed"] if counts else None,
                expected,
                "error",
                expected_min,
                expected_max,
            )
        )

# COMMAND ----------

# DBTITLE 1,Append the results to ops.dq_check_results
# Written before the comparison, so a run that fails leaves its results behind.
# mergeSchema adds expected_min and expected_max to a table written before them.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified(OPS_SCHEMA)}")
(
    spark.createDataFrame(results, RESULTS_SCHEMA)
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(results_table)
)

# COMMAND ----------

# DBTITLE 1,Compare the results with their expected failures
# Read back from the table: the comparison judges what was recorded.
written = [
    row.asDict()
    for row in spark.table(results_table)
    .filter(F.col("batch_id") == batch_id)
    .orderBy("check_name", "table_name", "source_file")
    .collect()
]


def count(value: int | None) -> str:
    return "-" if value is None else f"{value:,}"


def expectation(row: dict[str, Any]) -> str:
    """The count expected, or the range."""
    if row["expected_min"] is not None:
        return f"{row['expected_min']:,} to {row['expected_max']:,}"
    return count(row["expected_failures"])


print(
    f"{'check':<30} {'table':<24} {'file':<30} {'checked':>11} {'failed':>9} {'expected':>20}"
)
for row in written:
    print(
        f"{row['check_name']:<30} {row['table_name']:<24} {row['source_file'] or '-':<30} "
        f"{count(row['rows_checked']):>11} {count(row['rows_failed']):>9} "
        f"{expectation(row):>20}"
    )

if len(written) != len(results):
    raise AssertionError(
        f"{len(results)} results written, {len(written)} read back for batch {batch_id}"
    )
problems = mismatches(written, CHECKS)
if problems:
    raise AssertionError(
        f"{len(problems)} result(s) off their expected failures:\n"
        + "\n".join(problems)
    )
print(f"All {len(written)} results match their expected failures")

# COMMAND ----------

# DBTITLE 1,Record the pending months in the watermark
# Only now, with every check of both layers matched: the publication of each
# pending month, as the Volume's manifest records it, is processed. The next run
# finds them done, until one is landed again.
record_watermark(
    spark,
    watermark_table,
    pending,
    manifest=read_manifest(landing / MANIFEST_NAME),
    batch_id=batch_id,
    processed_at=run_at,
)
for entry in spark.table(watermark_table).orderBy("source_file").collect():
    print(
        f"{entry['source_file']:<30} {entry['published_at']}  "
        f"{entry['archive_bytes']:>12,} bytes  {entry['batch_id']}"
    )

# COMMAND ----------

# DBTITLE 1,Summary
failing = sum(1 for row in written if row["rows_failed"])
not_evaluated = sum(1 for row in written if row["rows_failed"] is None)
print(
    f"Batch {batch_id}: {len(written)} results, {failing} with expected failures, "
    f"{not_evaluated} not evaluated"
)
print(f"{results_table}: {spark.table(results_table).count():,} rows in total")
