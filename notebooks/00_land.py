# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Land (EU Trade Lakehouse)
# MAGIC
# MAGIC Lands the monthly COMEXT files of the window, and the code lists, in the landing
# MAGIC Volume: from the Eurostat bulk download facility, through the driver's disk, to
# MAGIC `/Volumes/eu_trade/landing/comext/`. The first task of the Job.
# MAGIC
# MAGIC Responsibilities:
# MAGIC   1. Read the listing of the bulk download facility, and the months of the window in
# MAGIC      it.
# MAGIC   2. Compare each month with the Volume's manifest: new, current, or republished since
# MAGIC      it was landed (`src/land.py`).
# MAGIC   3. Land each new or republished month: its CSV, and the copy of the figures Eurostat
# MAGIC      publishes for it, which the silver checks reconcile against. Record it in the
# MAGIC      manifest.
# MAGIC   4. Download every code list, over the one the Volume holds.
# MAGIC
# MAGIC Needs `py7zr`, which the Job installs in the environment of this task.
# MAGIC
# MAGIC Reads from:  `https://ec.europa.eu/eurostat/api/dissemination/`
# MAGIC Writes to:   `/Volumes/eu_trade/landing/comext/`

# COMMAND ----------

# DBTITLE 1,Imports
import logging
import os
import sys
from pathlib import Path

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

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

from config import LANDING_SCHEMA, LANDING_VOLUME, VOLUME_PATH, qualified, window_from
from land import land

spark = SparkSession.builder.getOrCreate()

# The landing logs each month it plans, downloads and copies.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    force=True,
)

# COMMAND ----------

# DBTITLE 1,Run settings
# The window of months: from start_period to end_period, or to the month
# before the last one the listing holds while end_period is empty, since the
# newest month lacks most of its intra-EU trade (src/land.py). The Job fills
# both from its parameters of the same names. Created in a cell of their own,
# because a widget created by a command that fails is never shown.
dbutils.widgets.text("start_period", "202601")
dbutils.widgets.text("end_period", "")

# COMMAND ----------

# DBTITLE 1,Parameters
start, end = window_from(
    dbutils.widgets.get("start_period"), dbutils.widgets.get("end_period")
)
volume = Path(VOLUME_PATH)

print(f"Target Volume: {volume}")
print(f"Window: from {start} to {end or 'the month before the last one listed'}")

# COMMAND ----------

# DBTITLE 1,Create the landing schema and Volume
# Left as they are if they exist. The catalog is not created here.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified(LANDING_SCHEMA)}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {qualified(LANDING_SCHEMA, LANDING_VOLUME)}")

# COMMAND ----------

# DBTITLE 1,Land the window
planned = land(volume, start, end)

# COMMAND ----------

# DBTITLE 1,Summary
landed = [month.period for month in planned if month.status != "current"]
print(
    f"{len(planned)} month(s) in the window, {len(landed)} landed: {landed or 'none'}"
)
