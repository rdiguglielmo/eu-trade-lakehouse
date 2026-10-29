"""The bronze reader on a local Spark: what it refuses, and what it lets through.

Uses the module's Spark session from conftest.py. Run without these tests with
-m "not spark".
"""

from __future__ import annotations

import functools
import operator
from collections.abc import Callable
from pathlib import Path

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from bronze import read_landed_csv
from config import EXPECTED_HEADER

pytestmark = pytest.mark.spark

WriteCase = Callable[[str, Path], Path]


def read_case(spark: SparkSession, write_case: WriteCase, tmp_path: Path, case: str):
    return read_landed_csv(spark, [str(write_case(case, tmp_path / f"{case}.csv"))])


def null_fields(spark_df) -> int:
    nulls = functools.reduce(
        operator.add, (F.col(name).isNull().cast("int") for name in EXPECTED_HEADER)
    )
    return spark_df.select(F.sum(nulls)).first()[0]


@pytest.mark.parametrize(("case", "nulls"), [("good", 0), ("empty_field", 1)])
def test_reader_loads_every_row(
    spark: SparkSession, write_case: WriteCase, tmp_path: Path, case: str, nulls: int
) -> None:
    df = read_case(spark, write_case, tmp_path, case)
    assert (df.count(), null_fields(df)) == (3, nulls)


def test_reader_keeps_the_leading_zero(
    spark: SparkSession, write_case: WriteCase, tmp_path: Path
) -> None:
    df = read_case(spark, write_case, tmp_path, "good")
    assert df.select("PRODUCT_NC").first()[0] == "02013000"


def test_reader_lets_a_lowercase_header_through(
    spark: SparkSession, write_case: WriteCase, tmp_path: Path
) -> None:
    # Spark compares column names without case, so this file loads.
    # Every later step resolves its columns the same way.
    df = read_case(spark, write_case, tmp_path, "lowercase_header")
    assert (df.columns, df.count()) == (list(EXPECTED_HEADER), 3)


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("extra_field", "MALFORMED_CSV_RECORD"),
        ("missing_field", "MALFORMED_CSV_RECORD"),
        ("bad_header", "CSV header does not conform to the schema"),
    ],
)
def test_reader_fails_on_the_first_action(
    spark: SparkSession, write_case: WriteCase, tmp_path: Path, case: str, message: str
) -> None:
    # The read itself succeeds: Spark reads lazily. On a local Spark the error
    # arrives from the JVM through py4j.
    df = read_case(spark, write_case, tmp_path, case)
    with pytest.raises(Py4JJavaError, match=message):
        df.collect()
