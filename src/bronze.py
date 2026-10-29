"""How the bronze layer reads a landed file.

Every column is read as text: a typed reader turns 02013000 into 2013000.
The header of each file is checked against EXPECTED_HEADER, and a malformed
row fails the read instead of being filled with nulls.

Two things this reader lets through:

- A header that differs only in case. Spark compares column names without
  case by default, so a lowercase header loads, and every later step resolves
  its columns the same way.
- An empty field, which Spark reads as NULL: its nullValue is "" by default.
  The bronze notebook counts the NULL fields of each file and expects none.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from config import EXPECTED_HEADER

RAW_SCHEMA = StructType([StructField(name, StringType()) for name in EXPECTED_HEADER])


def read_landed_csv(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Read landed CSV files as the 18 source columns, all of them text.

    enforceSchema=false checks the header of each file against RAW_SCHEMA,
    position by position, and fails if a name differs other than in case.
    FAILFAST fails on a row with more or fewer fields than the header. Spark
    reads lazily, so either failure is raised by the first action on the
    DataFrame, not by this call.
    """
    return (
        spark.read.format("csv")
        .schema(RAW_SCHEMA)
        .option("header", True)
        .option("enforceSchema", False)
        .option("mode", "FAILFAST")
        .load(paths)
    )
