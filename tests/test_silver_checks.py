"""The silver checks on a local Spark: each one fails on a real row broken on purpose.

Every row below is a line of a file of Eurostat's 2026-09-15 publication, and
every code list line a line of the published code list, unless a test changes a
field to make its point. The rows reach the checks as in the notebooks:
written to a landed file, read with the bronze reader and, for the checks of
the silver tables, split by split_bronze(). Uses the module's Spark session
from conftest.py. Run without these tests with -m "not spark".
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import silver_checks as checks
from bronze import read_landed_csv
from config import EXPECTED_HEADER
from silver import split_bronze

pytestmark = pytest.mark.spark


def change(line: str, **fields: str) -> str:
    """A source line with fields replaced by name."""
    values = dict(zip(EXPECTED_HEADER, line.split(","), strict=True))
    values.update(fields)
    return ",".join(values.values())


MAY = "full_v2_202605.csv"
JULY = "full_v2_202607.csv"

# Lines of full_v2_202605.csv.
TOTAL_GB = "AT,GB,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202605,109475558,109475558,6811188,0"
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
# Less than 1 EUR, no leading zero, no weight.
BELOW_ONE_EUR = (
    "CZ,GB,E,29212900,51452,2014,2014,220,211220,06,2,1,NO_SU,202605,.494,12,0,0"
)
# A real supplementary unit, with a quantity of 0.
REAL_UNIT = "AT,GB,E,22089069,11249,1101,1101,122,113101,04,2,1,L_ALC_100PCT,202605,522,522,30,0"

PRODUCT_ROWS = (NC8, CHAPTER_X, HS4_X, HS6_X, BELOW_ONE_EUR, REAL_UNIT)

# Rows that break nothing but check 06a (BELOW_ONE_EUR), as in the data.
CLEAN_ROWS = (TOTAL_GB, *PRODUCT_ROWS)

# Lines of the landed code lists, with their own line endings.
CODE_LISTS = {
    "PARTNERS_ISO.txt": (
        ("code", "valid_from", "valid_to", "name"),
        "\r\n",
        (
            "AT\t01/01/1976\t31/12/2500\tAustria",
            "BE\t01/01/1976\t31/12/1998\tBelgium and Luxembourg",
            "BE\t01/01/1999\t31/12/2500\tBelgium",
            "BG\t01/01/1976\t31/12/2500\tBulgaria",
            "CZ\t01/01/1993\t31/12/2500\tCzechia",
            "DD\t01/01/1976\t31/12/1990\tGermany, Democratic Republic of",
            "DE\t01/01/1976\t31/12/1990\tGermany",
            "DE\t01/01/1991\t31/12/2500\tGermany",
            "GB\t01/01/1976\t31/12/2500\tUnited Kingdom",
            "QY\t01/01/1976\t31/12/1993\tSecret countries Intra",
            "QY\t01/01/1994\t31/12/2500\tCountries and territories not specified for "
            "commercial or military reasons in the framework of Intra_EU trade",
        ),
    ),
    "CN.txt": (
        ("code", "valid_from", "valid_to", "description"),
        "\n",
        (
            "02013000\t01/01/1988\t31/12/2500\tFresh or chilled bovine meat, boneless",
            "22089069\t01/01/1994\t31/12/2500\tSpirituous beverages, in containers "
            "holding <= 2 l (excl. ouzo, spirits, liqueurs and cordials)",
            "29212900\t01/01/1988\t31/12/2500\tAcyclic polyamines and their "
            "derivatives; salts thereof (excl. ethylenediamine and "
            "hexamethylenediamine, and their salts)",
            '01022110\t01/01/2012\t31/12/2500\tPure-bred breeding heifers "female '
            'bovines that have never calved"',
            "97060000\t01/01/1988\t31/12/2021\tAntiques of > 100 years old",
        ),
    ),
    "STATISTICAL_PROCEDURES.txt": (
        ("code", "valid_from", "valid_to", "description"),
        "\n",
        (
            "1\t01/01/1988\t31/12/2500\tNormal",
            "5\t01/01/1988\t31/12/2009\tInward processing (suspension)",
            "9\t01/01/2010\t31/12/2500\tNot recorded from customs declarations",
        ),
    ),
    "GEOZONES-PARTNERS_ISO.txt": (
        ("zone", "code", "valid_from", "valid_to"),
        "\r\n",
        (
            "1010\tAT\t01/01/1995\t31/12/2500",
            "1010\tBE\t01/01/1999\t31/12/2500",
            "1010\tBE\t01/01/1976\t31/12/1998",
            "1010\tBG\t01/01/2007\t31/12/2500",
            "1010\tCZ\t01/05/2004\t31/12/2500",
            "1010\tDE\t01/01/1991\t31/12/2500",
            "1010\tGB\t01/01/1976\t31/01/2020",
            "1010\tQY\t01/01/1988\t31/12/1993",
            "1010\tQY\t01/01/1994\t31/12/2500",
            "1013\tAT\t01/01/1999\t31/12/2500",
        ),
    ),
}


def bronze(
    spark: SparkSession, folder: Path, *lines: str, source_file: str = MAY
) -> DataFrame:
    """Bronze rows from source lines, landed as a file named source_file.

    Read from a file rather than built from a Python list: on this machine
    every query over a list waits about ten seconds for its Python worker,
    and the CSV reader needs none.
    """
    path = folder / source_file
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(EXPECTED_HEADER)
        writer.writerows(line.split(",") for line in lines)
    return read_landed_csv(spark, [str(path)]).withColumn(
        "_source_file", F.col("_metadata.file_name")
    )


def detail(spark: SparkSession, folder: Path, *lines: str, **kwargs: str) -> DataFrame:
    return split_bronze(bronze(spark, folder, *lines, **kwargs))[0]


def totals(spark: SparkSession, folder: Path, *lines: str, **kwargs: str) -> DataFrame:
    return split_bronze(bronze(spark, folder, *lines, **kwargs))[1]


def failed(result: DataFrame) -> dict[str, int]:
    """rows_failed per source file."""
    return {row["_source_file"]: row["rows_failed"] for row in result.collect()}


@pytest.fixture(scope="module")
def code_lists(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> dict[str, DataFrame]:
    folder = tmp_path_factory.mktemp("code_lists")
    lists = {}
    for name, (columns, ending, lines) in CODE_LISTS.items():
        path = folder / name
        path.write_bytes("".join(line + ending for line in lines).encode("utf-8"))
        lists[name] = checks.read_code_list(spark, str(path), columns)
    return lists


def write_published(
    folder: Path, source_file: str, *figures: tuple[str, str, str]
) -> str:
    """A copy of the published figures for one landed file: (partner, flow, M EUR)."""
    path = folder / source_file
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(
            ("period", "partner", "stk_flow", "value_meur", "table_updated")
        )
        for partner, stk_flow, value in figures:
            writer.writerow(
                ("202605", partner, stk_flow, value, "2026-09-15T11:00:00+0200")
            )
    return str(path)


# Readers


def test_a_code_list_is_read_trimmed_and_with_its_dates(
    code_lists: dict[str, DataFrame],
) -> None:
    # PARTNERS_ISO.txt ends its lines with CRLF, as the landed file does.
    rows = code_lists["PARTNERS_ISO.txt"].filter(F.col("code") == "AT").collect()
    assert [tuple(row) for row in rows] == [
        ("AT", date(1976, 1, 1), date(2500, 12, 31), "Austria")
    ]


def test_a_quote_in_a_description_is_text(code_lists: dict[str, DataFrame]) -> None:
    row = code_lists["CN.txt"].filter(F.col("code") == "01022110").collect()[0]
    assert row["description"] == (
        'Pure-bred breeding heifers "female bovines that have never calved"'
    )


def test_a_code_list_row_with_a_field_missing_stops_the_read(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = tmp_path / "PARTNERS_ISO.txt"
    path.write_text("AT\t01/01/1976\t31/12/2500\tAustria\nBE\t01/01/1999\tBelgium\n")
    with pytest.raises(ValueError, match=r"rows of \[3, 4\] fields, expected 4"):
        checks.read_code_list(
            spark, str(path), ("code", "valid_from", "valid_to", "name")
        )


def test_no_copy_of_the_published_figures_reads_as_no_rows(spark: SparkSession) -> None:
    published = checks.read_published(spark, [])
    assert published.count() == 0
    assert "_source_file" in published.columns


# Every check over clean rows


def test_clean_rows_fail_only_the_checks_the_data_fails(
    spark: SparkSession,
    tmp_path: Path,
    code_lists: dict[str, DataFrame],
) -> None:
    df_bronze = bronze(spark, tmp_path, *CLEAN_ROWS)
    df_detail, df_totals = split_bronze(df_bronze)
    results = checks.all_checks(
        bronze=df_bronze,
        detail=df_detail,
        totals=df_totals,
        partners_iso=code_lists["PARTNERS_ISO.txt"],
        cn=code_lists["CN.txt"],
        procedures=code_lists["STATISTICAL_PROCEDURES.txt"],
        geozones=code_lists["GEOZONES-PARTNERS_ISO.txt"],
        published=checks.read_published(spark, []),
    )
    found = {(name, table): failed(result) for name, table, result in results}
    assert {name for name, _ in found} == set(checks.CHECKS)
    # No copy of the published figures, so check 10 evaluates nothing.
    assert found.pop(("10_totals_match_published", "totals")) == {}
    expected = {key: {MAY: 0} for key in found}
    expected[("06a_kg_zero_with_value", "detail")] = {MAY: 1}
    assert found == expected


# Each check, broken


@pytest.mark.parametrize(
    "line",
    [
        change(NC8, PARTNER="ZZ"),
        change(NC8, REPORTER="ZZ"),
        # A code of the list, valid only until 1990.
        change(NC8, PARTNER="DD"),
    ],
)
def test_01_a_country_without_a_valid_row(
    spark: SparkSession, tmp_path: Path, code_lists: dict[str, DataFrame], line: str
) -> None:
    rows = detail(spark, tmp_path, line)
    assert failed(checks.country_valid(rows, code_lists["PARTNERS_ISO.txt"])) == {
        MAY: 1
    }


@pytest.mark.parametrize(
    "product_nc",
    [
        "99999999",
        # Valid until 2021.
        "97060000",
        # Seven characters.
        "28XXXXX",
        "2XXXXXXX",
    ],
)
def test_02_a_product_neither_valid_nor_an_x_code(
    spark: SparkSession,
    tmp_path: Path,
    code_lists: dict[str, DataFrame],
    product_nc: str,
) -> None:
    rows = detail(spark, tmp_path, change(NC8, PRODUCT_NC=product_nc))
    assert failed(checks.product_valid(rows, code_lists["CN.txt"])) == {MAY: 1}


@pytest.mark.parametrize(
    ("fields", "rows_failed"),
    [
        ({"FLOW": "3"}, 1),
        ({"TRADE_TYPE": "X"}, 1),
        # The trade types of Northern Ireland and of the rest of the United
        # Kingdom, in the subset Eurostat publishes next to each full month.
        ({"TRADE_TYPE": "L"}, 1),
        ({"TRADE_TYPE": "M"}, 1),
        # Valid until 2009.
        ({"STAT_PROCEDURE": "5"}, 1),
        # Not in the list.
        ({"STAT_PROCEDURE": "4"}, 1),
        ({"STAT_PROCEDURE": "9"}, 0),
    ],
)
def test_03_a_code_outside_its_domain(
    spark: SparkSession,
    tmp_path: Path,
    code_lists: dict[str, DataFrame],
    fields: dict[str, str],
    rows_failed: int,
) -> None:
    rows = totals(spark, tmp_path, change(TOTAL_GB, **fields))
    result = checks.codes_in_domain(rows, code_lists["STATISTICAL_PROCEDURES.txt"])
    assert failed(result) == {MAY: rows_failed}


def test_04_a_period_other_than_the_month_of_the_file(
    spark: SparkSession, tmp_path: Path
) -> None:
    rows = detail(spark, tmp_path, NC8, change(NC8, PERIOD="202606"))
    assert failed(checks.period_matches_file(rows)) == {MAY: 1}


@pytest.mark.parametrize("column", ["VALUE_EUR", "VALUE_NAC"])
def test_05_a_negative_value(spark: SparkSession, tmp_path: Path, column: str) -> None:
    rows = detail(spark, tmp_path, change(NC8, **{column: "-17172"}))
    assert failed(checks.values_not_negative(rows)) == {MAY: 1}


def test_06a_a_weight_of_0_counts_only_with_a_positive_value(
    spark: SparkSession, tmp_path: Path
) -> None:
    rows = detail(
        spark,
        tmp_path,
        change(NC8, QUANTITY_KG="0"),
        change(
            NC8, PRODUCT_SITC="01113", QUANTITY_KG="0", VALUE_EUR="0", VALUE_NAC="0"
        ),
    )
    assert failed(checks.kg_zero_with_value(rows)) == {MAY: 1}


@pytest.mark.parametrize(
    "fields",
    [
        {"PRODUCT_SECTION": "01"},
        {"PRODUCT_BEC5": "113101"},
        {"SUPPL_UNIT": "L_ALC_100PCT"},
        {"QUANTITY_SUPPL_UNIT": "5"},
    ],
)
def test_08b_a_total_row_of_another_shape(
    spark: SparkSession, tmp_path: Path, fields: dict[str, str]
) -> None:
    rows = bronze(spark, tmp_path, change(TOTAL_GB, **fields), NC8)
    assert failed(checks.total_row_shape(rows)) == {MAY: 1}


def test_09_a_repeated_key_counts_its_rows(spark: SparkSession, tmp_path: Path) -> None:
    rows = detail(spark, tmp_path, NC8, NC8, HS6_X)
    assert failed(checks.no_duplicate_on_grain(rows, checks.DETAIL_GRAIN)) == {MAY: 2}
    rows = totals(spark, tmp_path, TOTAL_GB, TOTAL_GB)
    assert failed(checks.no_duplicate_on_grain(rows, checks.TOTALS_GRAIN)) == {MAY: 2}


def test_09_an_x_code_split_by_sitc_is_not_a_duplicate(
    spark: SparkSession, tmp_path: Path
) -> None:
    rows = detail(spark, tmp_path, CHAPTER_X, change(CHAPTER_X, PRODUCT_SITC="524XX"))
    assert failed(checks.no_duplicate_on_grain(rows, checks.DETAIL_GRAIN)) == {MAY: 0}


@pytest.mark.parametrize(
    ("full_figures", "full_failed"),
    [
        # 109.475558 M EUR each, 0.024 below the published 109.5.
        (
            (
                ("EU27_2020", "IMP", "109.5"),
                ("EXT_EU27_2020", "IMP", "109.5"),
                ("UK", "IMP", "109.5"),
            ),
            0,
        ),
        # 0.076 above the published 109.4.
        (
            (
                ("EU27_2020", "IMP", "109.5"),
                ("EXT_EU27_2020", "IMP", "109.5"),
                ("UK", "IMP", "109.4"),
            ),
            1,
        ),
        # No exports in the totals.
        ((("EU27_2020", "IMP", "109.5"), ("EU27_2020", "EXP", "1.0")), 1),
    ],
)
def test_10_the_totals_against_the_published_figures(
    spark: SparkSession,
    tmp_path: Path,
    full_figures: tuple[tuple[str, str, str], ...],
    full_failed: int,
) -> None:
    rows = totals(
        spark, tmp_path, TOTAL_GB, change(TOTAL_GB, PARTNER="DE", TRADE_TYPE="I")
    ).unionByName(totals(spark, tmp_path, TOTAL_GB, source_file=JULY))
    copies = tmp_path / "published"
    copies.mkdir()
    paths = [write_published(copies, MAY, *full_figures)]
    result = {
        row["_source_file"]: (row["rows_checked"], row["rows_failed"])
        for row in checks.totals_match_published(
            rows, checks.read_published(spark, paths)
        ).collect()
    }
    # July has no copy: not evaluated.
    assert result == {MAY: (len(full_figures), full_failed)}


@pytest.mark.parametrize(
    ("fields", "rows_failed"),
    [
        ({"PARTNER": "AT"}, 1),
        ({"PARTNER": "AT", "TRADE_TYPE": "I"}, 0),
        ({"PARTNER": "QY", "TRADE_TYPE": "I"}, 0),
        ({"PARTNER": "QY"}, 1),
        # In the EU until 2020-01-31.
        ({"TRADE_TYPE": "I"}, 1),
    ],
)
def test_11_the_trade_type_against_zone_1010(
    spark: SparkSession,
    tmp_path: Path,
    code_lists: dict[str, DataFrame],
    fields: dict[str, str],
    rows_failed: int,
) -> None:
    rows = detail(spark, tmp_path, change(NC8, **fields))
    result = checks.trade_type_matches_eu(rows, code_lists["GEOZONES-PARTNERS_ISO.txt"])
    assert failed(result) == {MAY: rows_failed}


@pytest.mark.parametrize(
    ("fields", "rows_failed"),
    [
        ({}, 0),
        ({"VALUE_EUR": ".494"}, 0),
        # Rounded to 1.235 by the cast.
        ({"VALUE_EUR": "1.2345"}, 1),
        ({"QUANTITY_KG": "1.5"}, 1),
        # 17 integer digits: more than DECIMAL(18,3) holds.
        ({"VALUE_NAC": "12345678901234567"}, 1),
        ({"VALUE_EUR": "17 172"}, 1),
    ],
)
def test_12a_a_value_the_cast_changes(
    spark: SparkSession, tmp_path: Path, fields: dict[str, str], rows_failed: int
) -> None:
    rows = bronze(spark, tmp_path, change(NC8, **fields))
    assert failed(checks.cast_lossless(rows)) == {MAY: rows_failed}


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("reporter", None),
        ("value_eur", None),
        # NO_SU resolved in the unit but not in its quantity.
        ("quantity_suppl_unit", 0),
        ("hs4", None),
    ],
)
def test_12b_a_null_no_sentinel_explains(
    spark: SparkSession, tmp_path: Path, column: str, value: int | None
) -> None:
    rows = detail(spark, tmp_path, NC8, CHAPTER_X)
    broken = rows.withColumn(
        column,
        F.when(F.col("product_nc") == "02013000", F.lit(value))
        .otherwise(F.col(column))
        .cast(dict(rows.dtypes)[column]),
    )
    assert failed(checks.null_only_from_sentinel(broken)) == {MAY: 1}


def test_12b_a_null_in_the_totals(spark: SparkSession, tmp_path: Path) -> None:
    rows = totals(spark, tmp_path, TOTAL_GB).withColumn(
        "flow", F.lit(None).cast("string")
    )
    assert failed(checks.null_only_from_sentinel(rows)) == {MAY: 1}


def test_12c_no_su_with_a_quantity(spark: SparkSession, tmp_path: Path) -> None:
    rows = bronze(spark, tmp_path, NC8, change(HS6_X, QUANTITY_SUPPL_UNIT="5"))
    assert failed(checks.no_su_quantity_zero(rows)) == {MAY: 1}
