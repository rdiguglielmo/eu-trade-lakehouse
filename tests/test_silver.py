"""The silver transformation on a local Spark, over real rows of the landed files.

Every row below is a line of full_v2_202605.csv, from the 2026-09-15
publication, unless a test changes a field to make its point. XI and XU are
lines of full_partxixu_v2_202605.csv, the subset of the month with the United
Kingdom's partners, which splits GB into Northern Ireland and the rest. The rows reach
silver as in the notebook: written to a landed file and read with the bronze
reader. Uses the module's Spark session from conftest.py. Run without these
tests with -m "not spark".
"""

from __future__ import annotations

import csv
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.errors import NumberFormatException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from bronze import read_landed_csv
from config import EXPECTED_HEADER
from silver import split_bronze

pytestmark = pytest.mark.spark

SOURCE_FILE = "full_v2_202605.csv"

TOTAL = "AT,GB,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202605,109475558,109475558,6811188,0"
XI = (
    "AT,XI,L,04032031,02231,1051,1051,112,113101,01,2,1,NO_SU,202605,11148,11148,3228,0"
)
XU = (
    "AT,XU,M,02013000,01112,1011,1011,122,113101,01,2,1,NO_SU,202605,17172,17172,1643,0"
)
NC8 = (
    "AT,GB,E,02013000,01112,1011,1011,122,113101,01,2,1,NO_SU,202605,17172,17172,1643,0"
)
CHAPTER_X = (
    "AT,GB,E,28XXXXXX,522XX,20XX,20XX,700,21XXXX,06,1,1,NO_SU,202605,2713,2713,51,0"
)
HS4_X = (
    "DE,GB,E,2833XXXX,52345,2013,2013,220,211220,06,2,1,NO_SU,202605,8859,8859,3435,0"
)
HS6_X = "BE,GB,E,283321XX,52349,2013,2013,220,211220,06,1,1,NO_SU,202605,6,6,2,0"
# Less than 1 EUR, written without a leading zero, and no weight.
BELOW_ONE_EUR = (
    "CZ,GB,E,29212900,51452,2014,2014,220,211220,06,2,1,NO_SU,202605,.494,12,0,0"
)
# A real supplementary unit, with a quantity of 0.
REAL_UNIT = "AT,GB,E,22089069,11249,1101,1101,122,113101,04,2,1,L_ALC_100PCT,202605,522,522,30,0"

ALL_ROWS = (TOTAL, XI, XU, NC8, CHAPTER_X, HS4_X, HS6_X, BELOW_ONE_EUR, REAL_UNIT)


def bronze(spark: SparkSession, folder: Path, *lines: str, **changes: str) -> DataFrame:
    """Bronze rows from source lines, with fields replaced by name if asked.

    Read from a file rather than built from a Python list: on this machine
    every query over a list waits about ten seconds for its Python worker,
    and the CSV reader needs none.
    """
    path = folder / SOURCE_FILE
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(EXPECTED_HEADER)
        for line in lines:
            fields = dict(zip(EXPECTED_HEADER, line.split(","), strict=True))
            fields.update(changes)
            writer.writerow(fields.values())
    return read_landed_csv(spark, [str(path)]).withColumn(
        "_source_file", F.col("_metadata.file_name")
    )


def only(df: DataFrame) -> dict[str, object]:
    rows = df.collect()
    assert len(rows) == 1
    return rows[0].asDict()


def test_a_dataframe_built_from_python_rows_reads(spark: SparkSession) -> None:
    # Reading a DataFrame built from a Python list takes a Python worker, which
    # Spark starts with PYSPARK_PYTHON: conftest.py sets it.
    df = spark.createDataFrame([("AT", "GB")], "REPORTER string, PARTNER string")
    assert [tuple(row) for row in df.collect()] == [("AT", "GB")]


def test_xi_and_xu_are_left_out_and_the_totals_go_apart(
    spark: SparkSession, tmp_path: Path
) -> None:
    detail, totals = split_bronze(bronze(spark, tmp_path, *ALL_ROWS))
    rows = detail.collect()
    assert sorted(row["product_nc"] for row in rows) == [
        "02013000",
        "22089069",
        "283321XX",
        "2833XXXX",
        "28XXXXXX",
        "29212900",
    ]
    assert {(row["partner"], row["_source_file"]) for row in rows} == {
        ("GB", SOURCE_FILE)
    }
    total = only(totals)
    assert (total["reporter"], total["partner"], total["value_eur"]) == (
        "AT",
        "GB",
        Decimal("109475558.000"),
    )


@pytest.mark.parametrize(
    ("line", "levels"),
    [
        (NC8, ("02", "0201", "020130", False)),
        (CHAPTER_X, ("28", None, None, True)),
        (HS4_X, ("28", "2833", None, True)),
        (HS6_X, ("28", "2833", "283321", True)),
    ],
)
def test_a_product_reaches_the_hs_levels_its_code_keeps(
    spark: SparkSession, tmp_path: Path, line: str, levels: tuple[object, ...]
) -> None:
    row = only(split_bronze(bronze(spark, tmp_path, line))[0])
    assert (row["hs2"], row["hs4"], row["hs6"], row["is_x_code"]) == levels


def test_no_su_becomes_null_and_a_zero_quantity_stays(
    spark: SparkSession, tmp_path: Path
) -> None:
    detail = split_bronze(bronze(spark, tmp_path, NC8, REAL_UNIT, BELOW_ONE_EUR))[0]
    by_product = {row["product_nc"]: row for row in detail.collect()}
    no_unit, real_unit = by_product["02013000"], by_product["22089069"]
    assert (no_unit["suppl_unit"], no_unit["quantity_suppl_unit"]) == (None, None)
    assert (real_unit["suppl_unit"], real_unit["quantity_suppl_unit"]) == (
        "L_ALC_100PCT",
        0,
    )
    assert by_product["29212900"]["quantity_kg"] == 0


def test_values_are_exact_decimals_and_the_month_a_date(
    spark: SparkSession, tmp_path: Path
) -> None:
    row = only(split_bronze(bronze(spark, tmp_path, BELOW_ONE_EUR))[0])
    assert (row["value_eur"], row["value_nac"]) == (Decimal("0.494"), Decimal("12.000"))
    assert (row["period"], row["period_date"]) == ("202605", date(2026, 5, 1))


def test_a_fourth_decimal_is_rounded_without_a_warning(
    spark: SparkSession, tmp_path: Path
) -> None:
    # No file has one. If one came, the cast would round it silently: the
    # reason the quality checks compare each value with a cast without loss.
    row = only(split_bronze(bronze(spark, tmp_path, NC8, VALUE_EUR="1.2345"))[0])
    assert row["value_eur"] == Decimal("1.235")


def test_text_that_is_not_a_number_fails_on_the_first_action(
    spark: SparkSession, tmp_path: Path
) -> None:
    # PySpark raises its own class, which Spark Connect's derives from too.
    detail = split_bronze(bronze(spark, tmp_path, NC8, VALUE_EUR="17 172"))[0]
    with pytest.raises(NumberFormatException, match="CAST_INVALID_INPUT"):
        detail.collect()


def test_the_columns_and_types_of_each_table(
    spark: SparkSession, tmp_path: Path
) -> None:
    detail, totals = split_bronze(bronze(spark, tmp_path, *ALL_ROWS))
    value, quantity = "decimal(18,3)", "bigint"
    assert detail.dtypes == [
        ("reporter", "string"),
        ("partner", "string"),
        ("trade_type", "string"),
        ("product_nc", "string"),
        ("hs2", "string"),
        ("hs4", "string"),
        ("hs6", "string"),
        ("is_x_code", "boolean"),
        ("product_sitc", "string"),
        ("product_cpa21", "string"),
        ("product_cpa22", "string"),
        ("product_bec", "string"),
        ("product_bec5", "string"),
        ("product_section", "string"),
        ("flow", "string"),
        ("stat_procedure", "string"),
        ("period", "string"),
        ("period_date", "date"),
        ("suppl_unit", "string"),
        ("value_eur", value),
        ("value_nac", value),
        ("quantity_kg", quantity),
        ("quantity_suppl_unit", quantity),
        ("_source_file", "string"),
    ]
    assert totals.dtypes == [
        ("reporter", "string"),
        ("partner", "string"),
        ("trade_type", "string"),
        ("flow", "string"),
        ("stat_procedure", "string"),
        ("period", "string"),
        ("period_date", "date"),
        ("value_eur", value),
        ("value_nac", value),
        ("quantity_kg", quantity),
        ("_source_file", "string"),
    ]
