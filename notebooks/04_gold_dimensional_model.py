# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 - Gold dimensional model (EU Trade Lakehouse)
# MAGIC
# MAGIC Builds the star schema over silver: `fct_trade_detail` and `fct_trade_totals`, one fact
# MAGIC per silver table, and the five dimensions they point to. `dim_country` and `dim_product`
# MAGIC hold one row per version of a code, and each fact row takes the version that covers its
# MAGIC month through a surrogate key.
# MAGIC
# MAGIC Responsibilities:
# MAGIC   1. Read the files of the pending months (`src/watermark.py`) from silver, and stop if
# MAGIC      one is missing. With none pending, end without writing.
# MAGIC   2. Build the dimensions from the code lists and the seeds: see `src/gold.py`.
# MAGIC   3. Merge `dim_country` and `dim_product` into their tables on `(code, valid_from)`, and
# MAGIC      replace the three small dimensions.
# MAGIC   4. Build both facts against the dimension tables, and replace the pending months.
# MAGIC   5. Check the facts against silver: per file, every silver row is one fact row, of one
# MAGIC      batch, with every key resolved.
# MAGIC
# MAGIC Reads from:  `eu_trade.silver.trade_detail` and `eu_trade.silver.trade_totals`; the code
# MAGIC              lists in the Volume; `reference/euro_area_members.csv` and
# MAGIC              `reference/partner_special_codes.csv`
# MAGIC Writes to:   the seven tables of `eu_trade.gold`

# COMMAND ----------

# DBTITLE 1,Imports
import functools
import operator
import os
import sys
import uuid
from pathlib import Path

from databricks.sdk.runtime import dbutils, display
from delta.tables import DeltaTable
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
    CODE_LISTS_SUBDIR,
    COUNTRY_DIM_TABLE,
    DETAIL_FACT_TABLE,
    FLOW_DIM_TABLE,
    GOLD_SCHEMA,
    OPS_SCHEMA,
    PERIOD_DIM_TABLE,
    PRODUCT_DIM_TABLE,
    REFERENCE_DIR,
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
from gold import (
    build_dim_country,
    build_dim_period,
    build_dim_product,
    build_fct_trade_detail,
    build_fct_trade_totals,
    merge_dimension,
    read_special_codes,
)
from silver_checks import read_code_list, read_euro_area
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
silver_tables = {
    "detail": qualified(SILVER_SCHEMA, SILVER_DETAIL_TABLE),
    "totals": qualified(SILVER_SCHEMA, SILVER_TOTALS_TABLE),
}
fact_tables = {
    "detail": qualified(GOLD_SCHEMA, DETAIL_FACT_TABLE),
    "totals": qualified(GOLD_SCHEMA, TOTALS_FACT_TABLE),
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

landing = Path(VOLUME_PATH)

batch_id = str(uuid.uuid4())

print(f"Source tables: {', '.join(silver_tables.values())}")
print(f"Target schema: {qualified(GOLD_SCHEMA)}")
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

source_files = [path.name for path in landed_csv_paths(landing, pending)]

# The months this run replaces in both facts. A month an earlier run wrote,
# and this one does not process, stays as it is.
months = ", ".join(f"'{period}'" for period in pending)
replaced_months = f"period IN ({months})"

# COMMAND ----------

# DBTITLE 1,Read the files of the pending months from silver
silver = {
    name: spark.table(table).filter(F.col("_source_file").isin(source_files))
    for name, table in silver_tables.items()
}
silver_rows = {}
for name, df in silver.items():
    silver_rows[name] = {
        row["_source_file"]: row["count"]
        for row in df.groupBy("_source_file").count().collect()
    }
    missing = sorted(set(source_files) - silver_rows[name].keys())
    if missing:
        raise AssertionError(
            f"Not in {silver_tables[name]}: {missing}. Run 02_silver_transform first"
        )
    print(
        f"{silver_tables[name]}: "
        + ", ".join(f"{f} {silver_rows[name][f]:,}" for f in source_files)
    )

# COMMAND ----------

# DBTITLE 1,Read the code lists and the seeds
code_lists = landing / CODE_LISTS_SUBDIR


def code_list(name: str, *columns: str) -> DataFrame:
    return read_code_list(spark, str(code_lists / name), columns)


partners_iso = code_list("PARTNERS_ISO.txt", "code", "valid_from", "valid_to", "name")
relation = code_list(
    "GEOZONES-PARTNERS_ISO.txt", "zone", "code", "valid_from", "valid_to"
)
zone_names = code_list("GEOZONES.txt", "zone", "valid_from", "valid_to", "zone_name")
cn = code_list("CN.txt", "code", "valid_from", "valid_to", "description")
sections = code_list("CN_SECTIONS.txt", "section_code", "section_description")
chapters = code_list("CN_SECTIONS-CHAPTERS.txt", "section_code", "hs2")
flows = code_list("FLOW.txt", "flow", "flow_name")
procedures = code_list(
    "STATISTICAL_PROCEDURES.txt",
    "stat_procedure",
    "valid_from",
    "valid_to",
    "description",
)
euro_area = read_euro_area(spark, str(REFERENCE_DIR / "euro_area_members.csv"))
special = read_special_codes(spark, str(REFERENCE_DIR / "partner_special_codes.csv"))

# COMMAND ----------

# DBTITLE 1,Build the dimensions
# dim_period holds every month of silver, not only the pending ones, so
# that a fact month an earlier run wrote keeps its row.
dim_country = build_dim_country(partners_iso, relation, zone_names, euro_area, special)
dim_product = build_dim_product(cn, sections, chapters)
dim_period = build_dim_period(*(spark.table(table) for table in silver_tables.values()))

# COMMAND ----------

# DBTITLE 1,Merge the versioned dimensions, replace the others
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified(GOLD_SCHEMA)}")

for name, dim, code_column in (
    (COUNTRY_DIM_TABLE, dim_country, "country_code"),
    (PRODUCT_DIM_TABLE, dim_product, "product_nc"),
):
    table = dim_tables[name]
    if not spark.catalog.tableExists(table):
        dim.limit(0).write.format("delta").saveAsTable(table)
    counts = merge_dimension(DeltaTable.forName(spark, table), dim, code_column)
    print(
        f"{table}: {counts['updated']:,} updated, {counts['inserted']:,} inserted, "
        f"{counts['missing']:,} missing from the source; "
        f"{spark.table(table).count():,} versions after"
    )

for name, dim in (
    (FLOW_DIM_TABLE, flows),
    (STAT_PROCEDURE_DIM_TABLE, procedures),
    (PERIOD_DIM_TABLE, dim_period),
):
    table = dim_tables[name]
    dim.write.format("delta").mode("overwrite").saveAsTable(table)
    print(f"{table}: {spark.table(table).count():,} rows")

# COMMAND ----------

# DBTITLE 1,Build and write the facts
# Built against the dimension tables, so that a fact joins what was recorded.
# Each fact is overwritten only in the pending months, partition by partition,
# as silver is.
stored_country = spark.table(dim_tables[COUNTRY_DIM_TABLE])
stored_product = spark.table(dim_tables[PRODUCT_DIM_TABLE])
facts = {
    "detail": build_fct_trade_detail(silver["detail"], stored_country, stored_product),
    "totals": build_fct_trade_totals(silver["totals"], stored_country),
}

for name, fact in facts.items():
    (
        fact.withColumn("_batch_id", F.lit(batch_id))
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replaced_months)
        .partitionBy("period")
        .saveAsTable(fact_tables[name])
    )

# COMMAND ----------

# DBTITLE 1,Check the facts against silver
# Per file, each silver row is one fact row: a key that two versions resolved
# would add rows. The rows are of this batch only, and every key resolved.
failures = []
for name, table in fact_tables.items():
    keys = [c for c in spark.table(table).columns if c.endswith("_key")]
    check = (
        spark.table(table)
        .filter(F.col("period").isin(*pending))
        .groupBy("_source_file")
        .agg(
            F.count("*").alias("fact_rows"),
            F.countDistinct("_batch_id").alias("batches"),
            F.max(F.col("_batch_id") == batch_id).alias("this_batch"),
            F.count(
                F.when(
                    functools.reduce(operator.or_, [F.col(k).isNull() for k in keys]),
                    True,
                )
            ).alias("null_keys"),
        )
        .orderBy("_source_file")
    )
    display(check)
    by_file = {row["_source_file"]: row for row in check.collect()}
    for source_file in sorted(set(source_files) | by_file.keys()):
        row = by_file.get(source_file)
        if (
            row is None
            or row["fact_rows"] != silver_rows[name].get(source_file)
            or row["batches"] != 1
            or not row["this_batch"]
            or row["null_keys"] != 0
        ):
            failures.append(f"{table}, {source_file}")
if failures:
    raise AssertionError(f"Gold does not match silver: {failures}")
print(f"Both facts match silver in their {len(source_files)} source files")

# COMMAND ----------

# DBTITLE 1,Table history
for table in (*dim_tables.values(), *fact_tables.values()):
    display(
        spark.sql(f"DESCRIBE HISTORY {table}").select(
            "version", "timestamp", "operation", "operationMetrics"
        )
    )

# COMMAND ----------

# DBTITLE 1,Sample rows
for table in (*dim_tables.values(), *fact_tables.values()):
    spark.table(table).printSchema()
    display(spark.table(table).limit(5))

# COMMAND ----------

# DBTITLE 1,Summary
print(
    f"Batch {batch_id}: "
    + ", ".join(
        f"{sum(silver_rows[name].values()):,} {name} fact rows" for name in fact_tables
    )
)
for table in (*dim_tables.values(), *fact_tables.values()):
    print(f"{table}: {spark.table(table).count():,} rows in total")
