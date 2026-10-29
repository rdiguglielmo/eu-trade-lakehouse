# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06 - Gold views (EU Trade Lakehouse)
# MAGIC
# MAGIC Creates the views that answer the business questions over the gold star schema, checks
# MAGIC them against the facts, and runs the query that states each answer for the latest month
# MAGIC loaded. The views read every month of the facts, not only the pending ones.
# MAGIC
# MAGIC Responsibilities:
# MAGIC   1. Create the views of `sql/gold_views.sql` in the gold schema: see `src/gold_views.py`.
# MAGIC   2. Check them against the facts: each view is unique on its grain and keeps the value of
# MAGIC      the fact rows it reads in every month, and the run stops otherwise.
# MAGIC   3. Run the queries of `sql/analysis_queries.sql`, one per question, and show the answers.
# MAGIC
# MAGIC Reads from:  the two facts and the dimensions of `eu_trade.gold`
# MAGIC Writes to:   the views of `eu_trade.gold`

# COMMAND ----------

# DBTITLE 1,Imports
import functools
import operator
import os
import sys

from databricks.sdk.runtime import display
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

from config import GOLD_SCHEMA, qualified
from gold_views import QUESTIONS_FILE, VIEWS, create_views, statements

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Create the views
# The SQL names the facts and the dimensions without a schema: each view
# resolves them in the schema current when it is created.
spark.sql(f"USE {qualified(GOLD_SCHEMA)}")
created = create_views(spark)
if created != list(VIEWS):
    raise AssertionError(f"Created {created}, expected {list(VIEWS)}")
print(f"Views in {qualified(GOLD_SCHEMA)}: {', '.join(created)}")

# COMMAND ----------

# DBTITLE 1,Check the views against the facts
# Per month, each view adds up to the value_eur of the fact rows it reads, and
# no row repeats the view's grain: a view that lost, added or split rows stops
# the run here.
failures = []
for name, (grain, rows_read, value_columns) in VIEWS.items():
    view = spark.table(qualified(GOLD_SCHEMA, name))
    repeated = view.groupBy(*grain).count().filter(F.col("count") > 1).count()
    value = functools.reduce(operator.add, [F.col(c) for c in value_columns])
    kept = {
        row["period"]: (row["rows"], row["value_eur"])
        for row in view.groupBy("period")
        .agg(F.count("*").alias("rows"), F.sum(value).alias("value_eur"))
        .collect()
    }
    recorded = {
        row["period"]: row["value_eur"]
        for row in spark.sql(rows_read)
        .groupBy("period")
        .agg(F.sum("value_eur").alias("value_eur"))
        .collect()
    }
    for period in sorted(kept.keys() | recorded.keys()):
        rows, value_eur = kept.get(period, (0, None))
        print(
            f"{name:<28} {period} {rows:>7,} rows {value_eur or 0:>24,.3f} EUR, "
            f"rows read {recorded.get(period) or 0:>24,.3f}"
        )
        if value_eur != recorded.get(period):
            failures.append(
                f"{name}, {period}: {value_eur} against {recorded.get(period)}"
            )
    if repeated:
        failures.append(f"{name}: {repeated:,} values of the grain repeated")
if failures:
    raise AssertionError(
        "The views do not match the fact rows they read:\n" + "\n".join(failures)
    )
print(
    f"All {len(VIEWS)} views keep the value of the fact rows they read, once per grain"
)

# COMMAND ----------

# DBTITLE 1,Answer the questions
# One query per question over the views, for the latest month loaded.
for statement in statements(QUESTIONS_FILE):
    question = next(
        line.removeprefix("-- ")
        for line in statement.splitlines()
        if line.startswith("-- Question")
    )
    answer = spark.sql(statement)
    print(f"{question} {answer.count():,} rows")
    display(answer)
