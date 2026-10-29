# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 - Silver transform (EU Trade Lakehouse)
# MAGIC
# MAGIC Turns the bronze rows of the pending months into two typed tables: the rows by
# MAGIC product in `trade_detail`, and the `TOTAL` rows, which sum the others, in
# MAGIC `trade_totals`. Neither holds XI or XU, since GB is exactly their sum.
# MAGIC
# MAGIC Responsibilities:
# MAGIC   1. Read from bronze the files of the pending months only (`src/watermark.py`), and stop
# MAGIC      if one is missing. With none pending, end without writing.
# MAGIC   2. Type, name and split the rows: see `src/silver.py`.
# MAGIC   3. Replace the pending months in both tables, so a re-run writes the same rows again
# MAGIC      instead of adding them.
# MAGIC   4. Check the tables against bronze: per file, the bronze rows are the detail, the
# MAGIC      totals and the XI and XU rows left out.
# MAGIC
# MAGIC The data quality checks run in `03_silver_quality_checks`.
# MAGIC
# MAGIC Reads from:  `eu_trade.bronze.comext_trade`
# MAGIC Writes to:   `eu_trade.silver.trade_detail` and `eu_trade.silver.trade_totals`

# COMMAND ----------

# DBTITLE 1,Imports
import os
import sys
import uuid
from pathlib import Path

from databricks.sdk.runtime import dbutils, display
from pyspark.sql import DataFrame, SparkSession
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
    OPS_SCHEMA,
    SILVER_DETAIL_TABLE,
    SILVER_SCHEMA,
    SILVER_TOTALS_TABLE,
    VOLUME_PATH,
    WATERMARK_TABLE,
    qualified,
    window_from,
)
from silver import UK_SPLIT_PARTNERS, split_bronze
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
bronze_table = qualified(BRONZE_SCHEMA, BRONZE_TRADE_TABLE)
detail_table = qualified(SILVER_SCHEMA, SILVER_DETAIL_TABLE)
totals_table = qualified(SILVER_SCHEMA, SILVER_TOTALS_TABLE)
landing = Path(VOLUME_PATH)

batch_id = str(uuid.uuid4())

print(f"Source table: {bronze_table}")
print(f"Target tables: {detail_table}, {totals_table}")
print(f"Window: from {start} to {end or 'the last month landed'}")
print(f"Batch {batch_id}")

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

# The files of the pending months, as bronze names them.
source_files = [path.name for path in landed_csv_paths(landing, pending)]

# The months this run replaces in both tables. A month an earlier run wrote,
# and this one does not process, stays as it is.
months = ", ".join(f"'{period}'" for period in pending)
replaced_months = f"period IN ({months})"

# COMMAND ----------

# DBTITLE 1,Read the files of the pending months from bronze
df_bronze = spark.table(bronze_table).filter(F.col("_source_file").isin(source_files))

loaded_files = {
    row["_source_file"] for row in df_bronze.select("_source_file").distinct().collect()
}
missing_files = sorted(set(source_files) - loaded_files)
if missing_files:
    raise AssertionError(
        f"Not in {bronze_table}: {missing_files}. Run 01_bronze_ingest first"
    )
print(f"Reading {len(source_files)} file(s) from bronze: {source_files}")

# COMMAND ----------

# DBTITLE 1,Create the silver schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified(SILVER_SCHEMA)}")

# COMMAND ----------

# DBTITLE 1,Type, split and write the two tables
# Each table is overwritten only in the pending months, partition by
# partition, so re-running silver replaces its rows rather than duplicating
# them. Delta checks that every row written falls inside those months.
df_detail, df_totals = split_bronze(df_bronze)


def write_months(df: DataFrame, table: str) -> int:
    """Replace the pending months of a table with df, and count what was written."""
    (
        df.withColumn("_batch_id", F.lit(batch_id))
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replaced_months)
        .partitionBy("period")
        .saveAsTable(table)
    )
    return spark.table(table).filter(F.col("_batch_id") == batch_id).count()


detail_written = write_months(df_detail, detail_table)
totals_written = write_months(df_totals, totals_table)
print(f"detail_written: {detail_written:,}")
print(f"totals_written: {totals_written:,}")

# COMMAND ----------

# DBTITLE 1,Check the tables against bronze
# Per file, every bronze row is in the detail, in the totals, or is an XI or XU
# row, which silver leaves out. A file with rows of an earlier batch, or a file
# the configuration does not name in the pending months, fails too.
bronze_by_file = df_bronze.groupBy("_source_file").agg(
    F.count("*").alias("bronze_rows"),
    F.sum(F.col("PARTNER").isin(*UK_SPLIT_PARTNERS).cast("int")).alias("xi_xu_rows"),
)


def rows_by_file(table: str, name: str) -> DataFrame:
    return (
        spark.table(table)
        .filter(F.col("period").isin(*pending))
        .groupBy("_source_file")
        .agg(
            F.count("*").alias(f"{name}_rows"),
            F.countDistinct("_batch_id").alias(f"{name}_batches"),
        )
    )


check = (
    bronze_by_file.join(
        rows_by_file(detail_table, "detail"), "_source_file", "full_outer"
    )
    .join(rows_by_file(totals_table, "totals"), "_source_file", "full_outer")
    .orderBy("_source_file")
)
display(check)

failures = [
    row["_source_file"]
    for row in check.collect()
    if None in (row["bronze_rows"], row["detail_rows"], row["totals_rows"])
    or row["bronze_rows"] != row["detail_rows"] + row["totals_rows"] + row["xi_xu_rows"]
    or row["detail_batches"] != 1
    or row["totals_batches"] != 1
]
if failures:
    raise AssertionError(f"Silver does not match bronze: {failures}")
print(f"Silver matches bronze in its {len(source_files)} source files")

# COMMAND ----------

# DBTITLE 1,Table history
for table in (detail_table, totals_table):
    display(
        spark.sql(f"DESCRIBE HISTORY {table}").select(
            "version", "timestamp", "operation", "operationParameters"
        )
    )

# COMMAND ----------

# DBTITLE 1,Sample rows
for table in (detail_table, totals_table):
    spark.table(table).printSchema()
    display(spark.table(table).limit(5))

# COMMAND ----------

# DBTITLE 1,Summary
print(f"Batch {batch_id}: {detail_written:,} detail and {totals_written:,} totals rows")
for table in (detail_table, totals_table):
    print(f"{table}: {spark.table(table).count():,} rows in total")
