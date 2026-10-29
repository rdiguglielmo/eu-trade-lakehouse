# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Silver quality checks (EU Trade Lakehouse)
# MAGIC
# MAGIC Runs the twelve data quality checks of the silver layer, seventeen counting their
# MAGIC parts, over the files of the pending months. Each writes one row per table it reads
# MAGIC and per file to `ops.dq_check_results`, next to the failures it is expected to find:
# MAGIC a count or a range. A count off its expectation, up or down, fails
# MAGIC the run once the results are written.
# MAGIC
# MAGIC Responsibilities:
# MAGIC   1. Read the files of the pending months (`src/watermark.py`) from silver and bronze,
# MAGIC      and stop if one is missing. With none pending, end without writing.
# MAGIC   2. Read the code lists, the euro area seed and the copies of the published figures.
# MAGIC   3. Run every check: see `src/silver_checks.py`, which also declares the expected
# MAGIC      failures.
# MAGIC   4. Append the results to `ops.dq_check_results`, read them back and compare.
# MAGIC
# MAGIC Reads from:  `eu_trade.silver.trade_detail`, `eu_trade.silver.trade_totals` and
# MAGIC              `eu_trade.bronze.comext_trade`; the code lists and the published figures
# MAGIC              in the Volume; `reference/euro_area_members.csv`
# MAGIC Writes to:   `eu_trade.ops.dq_check_results`

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
    BRONZE_SCHEMA,
    BRONZE_TRADE_TABLE,
    CODE_LISTS_SUBDIR,
    DQ_RESULTS_TABLE,
    OPS_SCHEMA,
    PUBLISHED_SUBDIR,
    REFERENCE_DIR,
    SILVER_DETAIL_TABLE,
    SILVER_SCHEMA,
    SILVER_TOTALS_TABLE,
    VOLUME_PATH,
    WATERMARK_TABLE,
    qualified,
    window_from,
)
from silver_checks import (
    all_checks,
    expected_bounds,
    expected_failures,
    mismatches,
    read_code_list,
    read_euro_area,
    read_published,
)
from watermark import landed_csv_paths, pending_in_window

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
# The tables the checks read, by the name all_checks() gives each.
tables = {
    "bronze": qualified(BRONZE_SCHEMA, BRONZE_TRADE_TABLE),
    "detail": qualified(SILVER_SCHEMA, SILVER_DETAIL_TABLE),
    "totals": qualified(SILVER_SCHEMA, SILVER_TOTALS_TABLE),
}
results_table = qualified(OPS_SCHEMA, DQ_RESULTS_TABLE)
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
# The same months 01_bronze_ingest loaded: the watermark has not changed since.
window, pending = pending_in_window(
    spark, landing, qualified(OPS_SCHEMA, WATERMARK_TABLE), start, end
)
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

# DBTITLE 1,Read the files of the pending months from silver and bronze
inputs = {
    name: spark.table(table).filter(F.col("_source_file").isin(source_files))
    for name, table in tables.items()
}
for name, df in inputs.items():
    rows = {
        row["_source_file"]: row["count"]
        for row in df.groupBy("_source_file").count().collect()
    }
    missing = sorted(set(source_files) - rows.keys())
    if missing:
        raise AssertionError(
            f"Not in {tables[name]}: {missing}. "
            "Run 01_bronze_ingest and 02_silver_transform first"
        )
    print(f"{tables[name]}: " + ", ".join(f"{f} {rows[f]:,}" for f in source_files))

# COMMAND ----------

# DBTITLE 1,Read the code lists, the euro area seed and the published figures
code_lists = landing / CODE_LISTS_SUBDIR
valid_columns = ("code", "valid_from", "valid_to", "description")
partners_iso = read_code_list(
    spark,
    str(code_lists / "PARTNERS_ISO.txt"),
    ("code", "valid_from", "valid_to", "name"),
)
cn = read_code_list(spark, str(code_lists / "CN.txt"), valid_columns)
procedures = read_code_list(
    spark, str(code_lists / "STATISTICAL_PROCEDURES.txt"), valid_columns
)
geozones = read_code_list(
    spark,
    str(code_lists / "GEOZONES-PARTNERS_ISO.txt"),
    ("zone", "code", "valid_from", "valid_to"),
)
euro_area = read_euro_area(spark, str(REFERENCE_DIR / "euro_area_members.csv"))

# The copy saved with each file when it was landed. A file without one is not
# evaluated by check 10.
copies = [landing / PUBLISHED_SUBDIR / name for name in source_files]
with_copy = {path.name for path in copies if path.is_file()}
published = read_published(
    spark, [str(path) for path in copies if path.name in with_copy]
)
for copy in (
    published.groupBy("_source_file", "table_updated")
    .count()
    .orderBy("_source_file")
    .collect()
):
    print(
        f"Published figures for {copy['_source_file']}: {copy['count']}, "
        f"updated {copy['table_updated']}"
    )
print(f"Without a copy: {[p.name for p in copies if not p.is_file()] or 'none'}")

# COMMAND ----------

# DBTITLE 1,Run the checks
# A file a check returns no row for was not evaluated: rows_failed is NULL.
results = []
for check_name, table, result in all_checks(
    bronze=inputs["bronze"],
    detail=inputs["detail"],
    totals=inputs["totals"],
    partners_iso=partners_iso,
    cn=cn,
    procedures=procedures,
    geozones=geozones,
    euro_area=euro_area,
    published=published,
):
    started = time.perf_counter()
    by_file = {row["_source_file"]: row for row in result.collect()}
    print(f"{check_name:<30} {table:<7} {time.perf_counter() - started:6.1f} s")
    for source_file in source_files:
        counts = by_file.get(source_file)
        rows_checked = counts["rows_checked"] if counts else None
        expected, expected_min, expected_max = expected_bounds(
            expected_failures(check_name, has_copy=source_file in with_copy),
            rows_checked,
        )
        results.append(
            (
                check_name,
                "silver",
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
    f"{'check':<30} {'table':<22} {'file':<30} {'checked':>11} {'failed':>9} {'expected':>20}"
)
for row in written:
    print(
        f"{row['check_name']:<30} {row['table_name']:<22} {row['source_file']:<30} "
        f"{count(row['rows_checked']):>11} {count(row['rows_failed']):>9} "
        f"{expectation(row):>20}"
    )

if len(written) != len(results):
    raise AssertionError(
        f"{len(results)} results written, {len(written)} read back for batch {batch_id}"
    )
problems = mismatches(written)
if problems:
    raise AssertionError(
        f"{len(problems)} result(s) off their expected failures:\n"
        + "\n".join(problems)
    )
print(f"All {len(written)} results match their expected failures")

# COMMAND ----------

# DBTITLE 1,Summary
failing = sum(1 for row in written if row["rows_failed"])
not_evaluated = sum(1 for row in written if row["rows_failed"] is None)
print(
    f"Batch {batch_id}: {len(written)} results, {failing} with expected failures, "
    f"{not_evaluated} not evaluated"
)
print(f"{results_table}: {spark.table(results_table).count():,} rows in total")
