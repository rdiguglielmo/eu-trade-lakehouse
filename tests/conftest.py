"""Small landed files for the tests, built from the expected header, and the
local Spark session of the tests that need one.

Each case is three data rows of full_v2_202605.csv, from the 2026-09-15
publication, with one defect or none. The files are written comma separated
and unquoted, as Eurostat writes them, but with LF line endings where
Eurostat's end in CRLF: Spark reads both the same.
"""

from __future__ import annotations

import csv
import os
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from config import EXPECTED_HEADER

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

ROWS = (
    "AT,GB,E,02013000,01112,1011,1011,122,113101,01,2,1,NO_SU,202605,17172,17172,1643,0",
    "AT,GB,E,02023050,01122,1011,1011,122,113101,01,2,1,NO_SU,202605,91,91,14,0",
    "AT,GB,E,02062999,01252,1011,1011,122,113101,01,2,1,NO_SU,202605,7609,7609,1606,0",
)


def _rows(*, middle: Callable[[list[str]], list[str]] = list) -> list[list[str]]:
    """Return the three rows, with the second one passed through middle."""
    first, second, third = (row.split(",") for row in ROWS)
    return [first, middle(second), third]


def _blank_quantity_kg(row: list[str]) -> list[str]:
    row[EXPECTED_HEADER.index("QUANTITY_KG")] = ""
    return row


# Each case: the header row and the data rows.
CASES: dict[str, tuple[tuple[str, ...], list[list[str]]]] = {
    "good": (EXPECTED_HEADER, _rows()),
    "bad_header": (("DECLARANT", *EXPECTED_HEADER[1:]), _rows()),
    "lowercase_header": (tuple(name.lower() for name in EXPECTED_HEADER), _rows()),
    "extra_field": (EXPECTED_HEADER, _rows(middle=lambda row: [*row, "0"])),
    "missing_field": (EXPECTED_HEADER, _rows(middle=lambda row: row[:-1])),
    "empty_field": (EXPECTED_HEADER, _rows(middle=_blank_quantity_kg)),
}


@pytest.fixture
def write_case() -> Callable[[str, Path], Path]:
    """Return a function that writes one case to a path and returns the path."""

    def write(case: str, path: Path) -> Path:
        header, rows = CASES[case]
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(header)
            writer.writerows(rows)
        return path

    return write


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """One local Spark session with Delta per test module.

    Spark is the engine of the tests of the transformation logic, not a second
    way to run the pipeline, which runs on Databricks. Imported here rather
    than at the top, so that the tests that need no Spark start no JVM. Needs
    a JDK 17 and JAVA_HOME.
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    # Spark starts its Python workers with PYSPARK_PYTHON, or with "python3"
    # when it is unset: the interpreter running the tests is the one that has
    # pyspark.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
