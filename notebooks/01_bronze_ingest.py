# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 - Bronze ingest (EU Trade Lakehouse)
# MAGIC
# MAGIC Loads the monthly COMEXT files from the landing Volume into the bronze table as a
# MAGIC faithful copy: every source column as text, plus the ingestion metadata.
# MAGIC
# MAGIC Responsibilities:
# MAGIC   1. Load only the pending months of the window: new, or landed again from a
# MAGIC      later publication than the watermark holds (`src/watermark.py`). With none pending,
# MAGIC      end without writing.
# MAGIC   2. Read the CSV of each pending month by name, never every file in the Volume.
# MAGIC   3. Replace each pending month whole, so bronze holds one publication of it.
# MAGIC   4. Fail on a header that differs from the expected one other than in case, or on a
# MAGIC      malformed row.
# MAGIC   5. Check the table against the files: rows per file, period, empty fields.
# MAGIC
# MAGIC Reads from:  `/Volumes/eu_trade/landing/comext/`
# MAGIC Writes to:   `eu_trade.bronze.comext_trade`

# COMMAND ----------

# DBTITLE 1,Imports
import functools
import operator
import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from databricks.sdk.runtime import dbutils, display
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

from bronze import read_landed_csv
from config import (
    BRONZE_SCHEMA,
    BRONZE_TRADE_TABLE,
    EXPECTED_HEADER,
    OPS_SCHEMA,
    VOLUME_PATH,
    WATERMARK_TABLE,
    qualified,
    window_from,
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
bronze_table = qualified(BRONZE_SCHEMA, BRONZE_TRADE_TABLE)
landing = Path(VOLUME_PATH)

batch_id = str(uuid.uuid4())
ingested_at = datetime.now(UTC)

print(f"Target table: {bronze_table}")
print(f"Window: from {start} to {end or 'the last month landed'}")
print(f"Batch {batch_id} at {ingested_at.isoformat()}")

# COMMAND ----------

# DBTITLE 1,Pending months
# The months of the window landed from a publication the watermark does not
# hold: new to the pipeline, or republished since they were processed. The gold
# checks write the watermark last, so a run that stopped leaves them pending.
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

# The CSV of each pending month, by name.
source_files = {path.name: str(path) for path in landed_csv_paths(landing, pending)}
print(f"To load: {sorted(source_files)}")

# COMMAND ----------

# DBTITLE 1,Create the bronze schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified(BRONZE_SCHEMA)}")

# COMMAND ----------

# DBTITLE 1,Read the files as text and replace their months in bronze
# Every column as text, the header of each file checked against EXPECTED_HEADER
# without regard to case, and a malformed row fatal: see src/bronze.py. Each
# pending month is replaced whole, partition by partition, so bronze holds one
# publication of it; the one before can be read by time travel until the VACUUM
# of predictive optimization removes its files, about 7 days later. All files
# go in one write, so a failure leaves the table as it was.
replaced_months = ", ".join(f"'{period}'" for period in pending)
df_bronze = (
    read_landed_csv(spark, list(source_files.values()))
    .withColumn("_ingested_at", F.lit(ingested_at))
    .withColumn("_source_file", F.col("_metadata.file_name"))
    .withColumn("_batch_id", F.lit(batch_id))
)
(
    df_bronze.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"PERIOD IN ({replaced_months})")
    .partitionBy("PERIOD")
    .saveAsTable(bronze_table)
)
total_written = spark.table(bronze_table).filter(F.col("_batch_id") == batch_id).count()

print(f"total_written: {total_written:,}")

# COMMAND ----------

# DBTITLE 1,Check the table against the files
# The rows of each file in bronze against the lines of the file itself, read as
# plain text: a count that does not go through the CSV parser. Minus one for
# the header.
file_lines = (
    spark.read.text(list(source_files.values()))
    .groupBy(F.col("_metadata.file_name").alias("_source_file"))
    .agg((F.count("*") - 1).alias("file_data_lines"))
)

empty_fields = functools.reduce(
    operator.add, (F.col(name).isNull().cast("int") for name in EXPECTED_HEADER)
)
period_of_file = F.regexp_extract("_source_file", r"_(\d{6})\.csv$", 1)

bronze_by_file = (
    spark.table(bronze_table)
    .filter(F.col("_source_file").isin(*source_files))
    .groupBy("_source_file")
    .agg(
        F.count("*").alias("bronze_rows"),
        F.sum((F.col("PERIOD") != period_of_file).cast("int")).alias("period_mismatch"),
        F.sum(empty_fields).alias("null_fields"),
        F.countDistinct("_batch_id").alias("batches"),
        F.sum(F.col("PRODUCT_NC").startswith("0").cast("int")).alias("nc_leading_zero"),
        F.sum((F.col("PRODUCT_NC") == "TOTAL").cast("int")).alias("nc_total"),
    )
)

check = file_lines.join(bronze_by_file, "_source_file", "full_outer").orderBy(
    "_source_file"
)
display(check)

failures = [
    row["_source_file"]
    for row in check.collect()
    if row["bronze_rows"] != row["file_data_lines"]
    or row["period_mismatch"] != 0
    or row["null_fields"] != 0
    or row["batches"] != 1
]
if failures:
    raise AssertionError(f"Bronze does not match its source files: {failures}")
print(f"Bronze matches its {len(source_files)} source files")

# COMMAND ----------

# DBTITLE 1,Table history
display(
    spark.sql(f"DESCRIBE HISTORY {bronze_table}").select(
        "version", "timestamp", "operation", "operationMetrics"
    )
)

# COMMAND ----------

# DBTITLE 1,Sample rows
spark.table(bronze_table).printSchema()
display(spark.table(bronze_table).limit(10))

# COMMAND ----------

# DBTITLE 1,Summary
print(f"Batch {batch_id}: {total_written:,} rows from {len(source_files)} file(s)")
print(f"{bronze_table}: {spark.table(bronze_table).count():,} rows in total")
