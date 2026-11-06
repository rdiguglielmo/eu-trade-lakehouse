"""The gold checks on a local Spark: each one fails on a real row broken on purpose.

Every row below is a line of full_v2_202605.csv of the 2026-09-15
publication, and every code list line a line of the landed code list, with its
own line endings, unless a test changes a field to make its point. The seeds
are the ones in reference/. The rows reach the checks as in the notebooks:
written to a landed file, read with the bronze reader, split by split_bronze()
and built into the facts of gold against dimensions built from those lines.
Uses the module's Spark session from conftest.py. Run without these tests with
-m "not spark".
"""

from __future__ import annotations

import csv
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

import gold
import gold_checks as checks
from bronze import read_landed_csv
from config import EXPECTED_HEADER, REFERENCE_DIR
from silver import VALUE_TYPE, split_bronze
from silver_checks import read_code_list, read_euro_area

pytestmark = pytest.mark.spark

MAY = "full_v2_202605.csv"


def change(line: str, **fields: str) -> str:
    """A source line with fields replaced by name."""
    values = dict(zip(EXPECTED_HEADER, line.split(","), strict=True))
    values.update(fields)
    return ",".join(values.values())


# full_v2_202605.csv.
BULGARIA = (
    "BG,GB,E,01061900,00190,0149,0148,111,111100,01,2,1,NO_SU,202605,289,289,23,0"
)
GERMANY = (
    "DE,GB,E,01061900,00190,0149,0148,111,111100,01,1,1,NO_SU,202605,36905,36905,24,0"
)
NC8 = (
    "AT,GB,E,02013000,01112,1011,1011,122,113101,01,2,1,NO_SU,202605,17172,17172,1643,0"
)
CHAPTER_X = (
    "AT,GB,E,28XXXXXX,522XX,20XX,20XX,700,21XXXX,06,1,1,NO_SU,202605,2713,2713,51,0"
)
# A code of heading 2833, which the lines of CN.txt below do not hold.
HS4_X = (
    "DE,GB,E,2833XXXX,52345,2013,2013,220,211220,06,2,1,NO_SU,202605,8859,8859,3435,0"
)
TOTAL_AT = "AT,GB,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202605,109475558,109475558,6811188,0"
TOTAL_BG = "BG,GB,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202605,29957023,29957023,4742646,0"

# Rows whose codes all have a version in the dimensions below.
CLEAN_ROWS = (BULGARIA, GERMANY, NC8, CHAPTER_X, TOTAL_AT, TOTAL_BG)


def lines(ending: str, *rows: str) -> str:
    """Code list lines as landed, each followed by its line ending."""
    return "".join(row + ending for row in rows)


CODE_LISTS = {
    "PARTNERS_ISO.txt": (
        ("code", "valid_from", "valid_to", "name"),
        lines(
            "\r\n",
            "AT\t01/01/1976\t31/12/2500\tAustria",
            "BG\t01/01/1976\t31/12/2500\tBulgaria",
            "DD\t01/01/1976\t31/12/1990\tGermany, Democratic Republic of",
            "DE\t01/01/1976\t31/12/1990\tGermany",
            "DE\t01/01/1991\t31/12/2500\tGermany",
            "GB\t01/01/1976\t31/12/2500\tUnited Kingdom",
        ),
    ),
    "GEOZONES-PARTNERS_ISO.txt": (
        ("zone", "code", "valid_from", "valid_to"),
        lines(
            "\r\n",
            "1010\tAT\t01/01/1995\t31/12/2500",
            "1010\tBG\t01/01/2007\t31/12/2500",
            "1010\tDE\t01/01/1991\t31/12/2500",
            "1010\tDE\t01/01/1976\t31/12/1990",
            "1010\tGB\t01/01/1976\t31/01/2020",
            "5100\tAT\t01/01/1976\t31/12/2500",
            "5100\tBG\t01/01/1976\t31/12/2500",
            "5100\tDE\t01/01/1976\t31/12/1990",
            "5100\tDE\t01/01/1991\t31/12/2500",
            "5100\tGB\t01/01/1976\t31/12/2500",
        ),
    ),
    "GEOZONES.txt": (
        ("zone", "valid_from", "valid_to", "zone_name"),
        lines(
            "\n",
            "1010\t01/01/1976\t31/12/2500\tEuropean union (evolutionary)",
            "5100\t01/01/1976\t31/12/2500\tEurope",
        ),
    ),
    "CN.txt": (
        ("code", "valid_from", "valid_to", "description"),
        lines(
            "\n",
            "01061900\t01/01/2012\t31/12/2500\tLive mammals (excl. primates, whales, "
            "dolphins and porpoises, manatees and dugongs, seals, sea lions and walruses, "
            "camels and other camelids, rabbits and hares, horses, asses, mules, hinnies, "
            "bovines, pigs, sheep and goats)",
            "02013000\t01/01/1988\t31/12/2500\tFresh or chilled bovine meat, boneless",
            "28XXXXXX\t01/01/1988\t31/12/2500\tTrade of chapter 28, not elsewhere specified",
        ),
    ),
    "CN_SECTIONS.txt": (
        ("section_code", "section_description"),
        lines(
            "\n",
            "01\tLive animals; animal products (chapters 1 to 5)",
            "06\tProducts of the chemical or allied industries (chapters 28 to 38)",
        ),
    ),
    "CN_SECTIONS-CHAPTERS.txt": (
        ("section_code", "hs2"),
        lines("\n", "01\t01", "01\t02", "06\t28"),
    ),
    # As landed: CRLF between its two lines, and nothing after the last.
    "FLOW.txt": (("flow", "flow_name"), "1\tImport\r\n2\tExport"),
    "STATISTICAL_PROCEDURES.txt": (
        ("stat_procedure", "valid_from", "valid_to", "description"),
        lines(
            "\n",
            "1\t01/01/1988\t31/12/2500\tNormal",
            "9\t01/01/2010\t31/12/2500\tNot recorded from customs declarations",
        ),
    ),
}

DIMENSIONS = (
    "dim_country",
    "dim_product",
    "dim_flow",
    "dim_stat_procedure",
    "dim_period",
)


def silver_of(
    spark: SparkSession, folder: Path, *rows: str, source_file: str = MAY
) -> tuple[DataFrame, DataFrame]:
    """The detail and the totals of rows, landed as a file named source_file."""
    path = folder / source_file
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(EXPECTED_HEADER)
        writer.writerows(row.split(",") for row in rows)
    bronze = read_landed_csv(spark, [str(path)]).withColumn(
        "_source_file", F.col("_metadata.file_name")
    )
    return split_bronze(bronze)


def facts_of(
    silver: tuple[DataFrame, DataFrame], dims: dict[str, DataFrame]
) -> tuple[DataFrame, DataFrame]:
    """The two facts of the silver tables, keyed against dims."""
    detail, totals = silver
    return (
        gold.build_fct_trade_detail(detail, dims["dim_country"], dims["dim_product"]),
        gold.build_fct_trade_totals(totals, dims["dim_country"]),
    )


def run(check_name: str, table: str, **inputs: DataFrame) -> DataFrame:
    """The result of one check of all_checks() over inputs, through its own wiring."""
    for name, read, result in checks.all_checks(**inputs):
        if (name, read) == (check_name, table):
            return result
    raise KeyError((check_name, table))


def counted(result: DataFrame) -> dict[str | None, tuple[int, int]]:
    """rows_checked and rows_failed per source file; None for a check of a dimension."""
    return {
        row["_source_file"]: (row["rows_checked"], row["rows_failed"])
        for row in result.collect()
    }


def failed(result: DataFrame) -> dict[str | None, int]:
    """rows_failed per source file; None for a check of a dimension."""
    return {file: rows_failed for file, (_, rows_failed) in counted(result).items()}


def bulgaria_from(day: date) -> Column:
    """The version of Bulgaria that starts on day."""
    return (F.col("country_code") == "BG") & (F.col("valid_from") == day)


@pytest.fixture(scope="module")
def lists(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> dict[str, DataFrame]:
    folder = tmp_path_factory.mktemp("code_lists")
    read = {}
    for name, (columns, text) in CODE_LISTS.items():
        path = folder / name
        path.write_bytes(text.encode("utf-8"))
        read[name] = read_code_list(spark, str(path), columns)
    return read


@pytest.fixture(scope="module")
def dims(spark: SparkSession, lists: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Every dimension but dim_period, which holds the months of the rows."""
    return {
        "dim_country": gold.build_dim_country(
            lists["PARTNERS_ISO.txt"],
            lists["GEOZONES-PARTNERS_ISO.txt"],
            lists["GEOZONES.txt"],
            read_euro_area(spark, str(REFERENCE_DIR / "euro_area_members.csv")),
            gold.read_special_codes(
                spark, str(REFERENCE_DIR / "partner_special_codes.csv")
            ),
        ).cache(),
        "dim_product": gold.build_dim_product(
            lists["CN.txt"], lists["CN_SECTIONS.txt"], lists["CN_SECTIONS-CHAPTERS.txt"]
        ).cache(),
        "dim_flow": lists["FLOW.txt"],
        "dim_stat_procedure": lists["STATISTICAL_PROCEDURES.txt"],
    }


@pytest.fixture(scope="module")
def clean(
    spark: SparkSession,
    tmp_path_factory: pytest.TempPathFactory,
    dims: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    """The inputs of all_checks() over CLEAN_ROWS: the facts, silver and the dimensions."""
    silver = silver_of(spark, tmp_path_factory.mktemp("landing"), *CLEAN_ROWS)
    detail, totals = facts_of(silver, dims)
    detail, totals = detail.cache(), totals.cache()
    return {
        "detail": detail,
        "totals": totals,
        # One month: the facts of the run are every month of them.
        "every_detail": detail,
        "every_totals": totals,
        "silver_detail": silver[0],
        "silver_totals": silver[1],
        **dims,
        "dim_period": gold.build_dim_period(*silver),
    }


# Every check over clean inputs


def test_clean_inputs_fail_no_check(clean: dict[str, DataFrame]) -> None:
    found = {
        (name, table): counted(result)
        for name, table, result in checks.all_checks(**clean)
    }
    assert {name for name, _ in found} == set(checks.CHECKS)
    # Four rows by product and two TOTAL rows; 12 versions of five countries and
    # 3 of three products. Six rows are not a month, and their groups do not add
    # up to the TOTAL rows: check 07 fails four of five, all but Bulgaria's
    # exports, 289 EUR, within the tolerance. Its own tests are below.
    assert found.pop(("07_detail_matches_totals", "detail")) == {MAY: (5, 4)}
    assert found == {
        ("01_rows_match_silver", "detail"): {MAY: (4, 0)},
        ("01_rows_match_silver", "totals"): {MAY: (2, 0)},
        ("03_one_match_per_dimension", "detail"): {MAY: (4, 0)},
        ("03_one_match_per_dimension", "totals"): {MAY: (2, 0)},
        ("04_value_matches_silver", "detail"): {MAY: (1, 0)},
        ("04_value_matches_silver", "totals"): {MAY: (1, 0)},
        ("05_dimension_key_unique", "dim_country"): {None: (12, 0)},
        ("05_dimension_key_unique", "dim_product"): {None: (3, 0)},
        ("05_dimension_key_unique", "dim_flow"): {None: (2, 0)},
        ("05_dimension_key_unique", "dim_stat_procedure"): {None: (2, 0)},
        ("05_dimension_key_unique", "dim_period"): {None: (1, 0)},
        ("06_no_overlapping_versions", "dim_country"): {None: (12, 0)},
        ("06_no_overlapping_versions", "dim_product"): {None: (3, 0)},
        ("08_version_covers_month", "detail"): {MAY: (4, 0)},
        ("08_version_covers_month", "totals"): {MAY: (2, 0)},
    }


# Each check, broken


@pytest.mark.parametrize("broken", ["twice", "missing"])
def test_01_a_fact_row_twice_or_missing(
    clean: dict[str, DataFrame], broken: str
) -> None:
    bulgaria = F.col("value_eur") == 289
    detail = clean["detail"]
    detail = (
        detail.unionByName(detail.filter(bulgaria))
        if broken == "twice"
        else detail.filter(~bulgaria)
    )
    result = run("01_rows_match_silver", "detail", **{**clean, "detail": detail})
    assert counted(result) == {MAY: (4, 1)}


@pytest.mark.parametrize(
    "line",
    [
        HS4_X,
        # A partner of the list, valid only until 1990.
        change(NC8, PARTNER="DD"),
    ],
)
def test_03_a_code_without_a_version_in_the_month(
    spark: SparkSession,
    tmp_path: Path,
    dims: dict[str, DataFrame],
    clean: dict[str, DataFrame],
    line: str,
) -> None:
    # The fact leaves the key NULL, and a NULL key finds no row of its dimension.
    detail, _ = facts_of(silver_of(spark, tmp_path, line), dims)
    result = run("03_one_match_per_dimension", "detail", **{**clean, "detail": detail})
    assert failed(result) == {MAY: 1}


def test_03_a_flow_its_dimension_lacks(clean: dict[str, DataFrame]) -> None:
    # Bulgaria's row and NC8 are exports.
    dim_flow = clean["dim_flow"].filter(F.col("flow") != "2")
    result = run(
        "03_one_match_per_dimension", "detail", **{**clean, "dim_flow": dim_flow}
    )
    assert failed(result) == {MAY: 2}


def test_03_a_version_twice_in_its_dimension(clean: dict[str, DataFrame]) -> None:
    dim = clean["dim_country"]
    twice = dim.unionByName(dim.filter(bulgaria_from(date(2026, 1, 1))))
    inputs = {**clean, "dim_country": twice}
    # Bulgaria reports one row by product and one TOTAL row.
    assert failed(run("03_one_match_per_dimension", "detail", **inputs)) == {MAY: 1}
    assert failed(run("03_one_match_per_dimension", "totals", **inputs)) == {MAY: 1}


def test_03_a_version_missing_from_its_dimension(clean: dict[str, DataFrame]) -> None:
    dim_product = clean["dim_product"].filter(F.col("product_nc") != "02013000")
    result = run(
        "03_one_match_per_dimension", "detail", **{**clean, "dim_product": dim_product}
    )
    assert failed(result) == {MAY: 1}


def test_03_a_month_missing_from_dim_period(clean: dict[str, DataFrame]) -> None:
    dim_period = clean["dim_period"].filter(F.col("period") != "202605")
    result = run(
        "03_one_match_per_dimension", "detail", **{**clean, "dim_period": dim_period}
    )
    assert failed(result) == {MAY: 4}


def test_04_a_value_a_tenth_of_a_cent_off(clean: dict[str, DataFrame]) -> None:
    value = F.col("value_eur")
    detail = clean["detail"].withColumn(
        "value_eur",
        F.when(value == 289, value + Decimal("0.001"))
        .otherwise(value)
        .cast(VALUE_TYPE),
    )
    result = run("04_value_matches_silver", "detail", **{**clean, "detail": detail})
    assert counted(result) == {MAY: (1, 1)}


@pytest.mark.parametrize("extra", ["twice", "another_key", "another_code"])
def test_05_a_key_twice_in_dim_country(clean: dict[str, DataFrame], extra: str) -> None:
    dim = clean["dim_country"]
    bulgaria = dim.filter(bulgaria_from(date(2026, 1, 1)))
    row = {
        "twice": bulgaria,
        # The same version of the code and day under another surrogate key.
        "another_key": bulgaria.withColumn("country_key", F.col("country_key") + 1),
        # Another code under the same surrogate key: a collision.
        "another_code": bulgaria.withColumn("country_code", F.lit("ZZ")),
    }[extra]
    result = run(
        "05_dimension_key_unique",
        "dim_country",
        **{**clean, "dim_country": dim.unionByName(row)},
    )
    assert counted(result) == {None: (13, 2)}


def test_05_a_flow_twice(clean: dict[str, DataFrame]) -> None:
    dim = clean["dim_flow"]
    twice = dim.unionByName(dim.filter(F.col("flow") == "1"))
    result = run("05_dimension_key_unique", "dim_flow", **{**clean, "dim_flow": twice})
    assert counted(result) == {None: (3, 2)}


@pytest.mark.parametrize(
    ("valid_to", "rows_failed"),
    [
        (date(2025, 12, 31), 0),
        # One day over the version from 2026-01-01, in the euro area.
        (date(2026, 1, 1), 1),
    ],
)
def test_06_a_version_that_ends_after_the_next_starts(
    clean: dict[str, DataFrame], valid_to: date, rows_failed: int
) -> None:
    dim = clean["dim_country"].withColumn(
        "valid_to",
        F.when(bulgaria_from(date(2007, 1, 1)), F.lit(valid_to)).otherwise(
            F.col("valid_to")
        ),
    )
    result = run(
        "06_no_overlapping_versions", "dim_country", **{**clean, "dim_country": dim}
    )
    assert counted(result) == {None: (12, rows_failed)}


@pytest.mark.parametrize(
    ("rows", "groups"),
    [
        # Austria's imports from GB under procedure 1, the TOTAL row set to the
        # one row by product.
        ((CHAPTER_X, change(TOTAL_AT, VALUE_EUR="2713")), (1, 0)),
        # 0.01 M EUR apart, exactly: within.
        ((CHAPTER_X, change(TOTAL_AT, VALUE_EUR="12713")), (1, 0)),
        # A tenth of a cent beyond.
        ((CHAPTER_X, change(TOTAL_AT, VALUE_EUR="12713.001")), (1, 1)),
        # A group without its TOTAL row counts in full: 17,172 EUR.
        ((NC8,), (1, 1)),
    ],
)
def test_07_the_detail_against_the_totals(
    spark: SparkSession,
    tmp_path: Path,
    dims: dict[str, DataFrame],
    rows: tuple[str, ...],
    groups: tuple[int, int],
) -> None:
    detail, totals = facts_of(silver_of(spark, tmp_path, *rows, source_file=MAY), dims)
    assert counted(checks.detail_matches_totals(detail, totals)) == {MAY: groups}


@pytest.mark.parametrize(
    "moved",
    [
        # Bulgaria's version of the facts, from 2026-01-01, as if a later MERGE
        # had started it in June, or closed it in April.
        {"valid_from": date(2026, 6, 1)},
        {"valid_to": date(2026, 4, 30)},
    ],
)
def test_08_a_version_moved_off_the_month_after_the_facts(
    clean: dict[str, DataFrame], moved: dict[str, date]
) -> None:
    # The key stays: the facts, built before, still point to this version.
    bulgaria = bulgaria_from(date(2026, 1, 1))
    dim_country = clean["dim_country"].withColumns(
        {
            column: F.when(bulgaria, F.lit(day)).otherwise(F.col(column))
            for column, day in moved.items()
        }
    )
    inputs = {**clean, "dim_country": dim_country}
    # Bulgaria reports one row by product and one TOTAL row.
    assert counted(run("08_version_covers_month", "detail", **inputs)) == {MAY: (4, 1)}
    assert counted(run("08_version_covers_month", "totals", **inputs)) == {MAY: (2, 1)}


def test_08_a_product_closed_before_the_month(clean: dict[str, DataFrame]) -> None:
    dim_product = clean["dim_product"].withColumn(
        "valid_to",
        F.when(F.col("product_nc") == "02013000", F.lit(date(2025, 12, 31))).otherwise(
            F.col("valid_to")
        ),
    )
    result = run(
        "08_version_covers_month", "detail", **{**clean, "dim_product": dim_product}
    )
    assert counted(result) == {MAY: (4, 1)}


def test_08_reads_every_month_of_the_facts(clean: dict[str, DataFrame]) -> None:
    # Facts of the run without Bulgaria; every month with it, its version moved.
    bulgaria = bulgaria_from(date(2026, 1, 1))
    dim_country = clean["dim_country"].withColumn(
        "valid_to",
        F.when(bulgaria, F.lit(date(2026, 4, 30))).otherwise(F.col("valid_to")),
    )
    inputs = {
        **clean,
        "detail": clean["detail"].filter(F.col("value_eur") != 289),
        "dim_country": dim_country,
    }
    assert failed(run("08_version_covers_month", "detail", **inputs)) == {MAY: 1}
