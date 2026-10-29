"""The gold model on a local Spark: versions, the MERGE of 2026, and the keys of the facts.

Every row below is a line of full_v2_202605.csv of the 2026-09-15
publication, and every code list line a line of the landed code list, with its
own line ending. The seeds are the ones in reference/. The rows reach gold as
in the notebooks: written to a landed file, read with the bronze reader and
split by split_bronze(). The Delta tables of the MERGE are written by path
below the test's temporary folder, never to the local metastore. Uses the
module's Spark session from conftest.py. Run without these tests with
-m "not spark".
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import gold
from bronze import read_landed_csv
from config import EXPECTED_HEADER, REFERENCE_DIR
from silver import split_bronze
from silver_checks import read_code_list, read_euro_area

pytestmark = pytest.mark.spark

OPEN_END = date(2500, 12, 31)
SNAPSHOT_DAY = date(2025, 12, 31)

# Lines of the landed code lists, with their own line endings.
CODE_LISTS = {
    "PARTNERS_ISO.txt": (
        ("code", "valid_from", "valid_to", "name"),
        "\r\n",
        (
            "AT\t01/01/1976\t31/12/2500\tAustria",
            "BG\t01/01/1976\t31/12/2500\tBulgaria",
            "DE\t01/01/1976\t31/12/1990\tGermany",
            "DE\t01/01/1991\t31/12/2500\tGermany",
            "GB\t01/01/1976\t31/12/2500\tUnited Kingdom",
            "QY\t01/01/1976\t31/12/1993\tSecret countries Intra",
            "QY\t01/01/1994\t31/12/2500\tCountries and territories not specified for "
            "commercial or military reasons in the framework of Intra_EU trade",
        ),
    ),
    "GEOZONES-PARTNERS_ISO.txt": (
        ("zone", "code", "valid_from", "valid_to"),
        "\r\n",
        (
            "1010\tAT\t01/01/1995\t31/12/2500",
            "1010\tBG\t01/01/2007\t31/12/2500",
            "1010\tDE\t01/01/1991\t31/12/2500",
            "1010\tDE\t01/01/1976\t31/12/1990",
            "1010\tGB\t01/01/1976\t31/01/2020",
            "1010\tQY\t01/01/1988\t31/12/1993",
            "1010\tQY\t01/01/1994\t31/12/2500",
            "5100\tAT\t01/01/1976\t31/12/2500",
            "5100\tBG\t01/01/1976\t31/12/2500",
            "5100\tDE\t01/01/1976\t31/12/1990",
            "5100\tDE\t01/01/1991\t31/12/2500",
            "5100\tGB\t01/01/1976\t31/12/2500",
            "5100\tQY\t01/01/1994\t31/12/2500",
            "5100\tQY\t01/01/1988\t31/12/1993",
            "5900\tQY\t01/01/1994\t31/12/2500",
        ),
    ),
    "GEOZONES.txt": (
        ("zone", "valid_from", "valid_to", "zone_name"),
        "\n",
        (
            "1010\t01/01/1976\t31/12/2500\tEuropean union (evolutionary)",
            "5100\t01/01/1976\t31/12/2500\tEurope",
            "5900\t01/01/1976\t31/12/2500\tMiscellaneous countries not specified and "
            "countries not determined",
        ),
    ),
    "CN.txt": (
        ("code", "valid_from", "valid_to", "description"),
        "\n",
        (
            "01\t01/01/1988\t31/12/2500\tLIVE ANIMALS",
            "0106\t01/01/1988\t31/12/2001\tLive animals (excl. horses, asses, mules, "
            "hinnies, bovine animals, swine, sheep, goats, poultry, fish, crustaceans, "
            "molluscs and other aquatic invertebrates, and microorganic cultures etc.)",
            "0106\t01/01/2002\t31/12/2500\tLive animals (excl. horses, asses, mules, "
            "hinnies, bovine animals, swine, sheep, goats, poultry, fish, crustaceans, "
            "molluscs and other aquatic invertebrates, and microorganic cultures etc.)",
            "010619\t01/01/2002\t31/12/2500\tLive mammals (excl. primates, whales, "
            "dolphins and porpoises, manatees and dugongs, seals, sea lions and walruses, "
            "camels and other camelids, rabbits and hares, horses, asses, mules, hinnies, "
            "bovines, pigs, sheep and goats)",
            "01061900\t01/01/2012\t31/12/2500\tLive mammals (excl. primates, whales, "
            "dolphins and porpoises, manatees and dugongs, seals, sea lions and walruses, "
            "camels and other camelids, rabbits and hares, horses, asses, mules, hinnies, "
            "bovines, pigs, sheep and goats)",
            "02\t01/01/1988\t31/12/2500\tMEAT AND EDIBLE MEAT OFFAL",
            "0201\t01/01/1988\t31/12/2500\tMeat of bovine animals, fresh or chilled",
            "020130\t01/01/1988\t31/12/2500\tFresh or chilled bovine meat, boneless",
            "02013000\t01/01/1988\t31/12/2500\tFresh or chilled bovine meat, boneless",
            "28\t01/01/1988\t31/12/2500\tINORGANIC CHEMICALS; ORGANIC OR INORGANIC "
            "COMPOUNDS OF PRECIOUS METALS, OF RARE-EARTH METALS, OF RADIOACTIVE ELEMENTS "
            "OR OF ISOTOPES",
            "2841\t01/01/1988\t31/12/2500\tSalts of oxometallic or peroxometallic acids",
            "284190\t01/01/1988\t31/12/2500\tSalts of oxometallic or peroxometallic acids "
            "(excl. chromates, dichromates, peroxochromates, manganites, manganates, "
            'permanganates, molybdates and tungstates "wolframamtes")',
            "28419040\t01/01/2026\t31/12/2500\tLithium nickel manganese cobalt oxides",
            "28419085\t01/01/2007\t31/12/2025\tSalts of oxometallic or peroxometallic "
            "acids (excl. chromates, dichromates, peroxochromates, manganites, manganates, "
            'permanganates, molybdates, tungstates "wolframamtes", zincates and vanadates)',
            "28XXXXXX\t01/01/1988\t31/12/2500\tTrade of chapter 28, not elsewhere specified",
        ),
    ),
    "CN_SECTIONS.txt": (
        ("section_code", "section_description"),
        "\n",
        (
            "01\tLive animals; animal products (chapters 1 to 5)",
            "06\tProducts of the chemical or allied industries (chapters 28 to 38)",
        ),
    ),
    "CN_SECTIONS-CHAPTERS.txt": (
        ("section_code", "hs2"),
        "\n",
        ("01\t01", "01\t02", "06\t28"),
    ),
}

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
# A code of heading 2833, which the lines above do not hold.
HS4_X = (
    "DE,GB,E,2833XXXX,52345,2013,2013,220,211220,06,2,1,NO_SU,202605,8859,8859,3435,0"
)
TOTAL_AT = "AT,GB,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202605,109475558,109475558,6811188,0"
TOTAL_BG = "BG,GB,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202605,29957023,29957023,4742646,0"

ROWS = (BULGARIA, GERMANY, NC8, CHAPTER_X, HS4_X, TOTAL_AT, TOTAL_BG)


@pytest.fixture(scope="module")
def lists(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> dict[str, DataFrame]:
    folder = tmp_path_factory.mktemp("code_lists")
    read = {}
    for name, (columns, ending, lines) in CODE_LISTS.items():
        path = folder / name
        path.write_bytes("".join(line + ending for line in lines).encode("utf-8"))
        read[name] = read_code_list(spark, str(path), columns)
    return read


@pytest.fixture(scope="module")
def dim_country(spark: SparkSession, lists: dict[str, DataFrame]) -> DataFrame:
    return gold.build_dim_country(
        lists["PARTNERS_ISO.txt"],
        lists["GEOZONES-PARTNERS_ISO.txt"],
        lists["GEOZONES.txt"],
        read_euro_area(spark, str(REFERENCE_DIR / "euro_area_members.csv")),
        gold.read_special_codes(
            spark, str(REFERENCE_DIR / "partner_special_codes.csv")
        ),
    ).cache()


@pytest.fixture(scope="module")
def dim_product(lists: dict[str, DataFrame]) -> DataFrame:
    return gold.build_dim_product(
        lists["CN.txt"], lists["CN_SECTIONS.txt"], lists["CN_SECTIONS-CHAPTERS.txt"]
    ).cache()


@pytest.fixture(scope="module")
def silver(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> tuple[DataFrame, DataFrame]:
    """The detail and the totals of ROWS, as silver splits them."""
    path = tmp_path_factory.mktemp("landing") / "full_v2_202605.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(EXPECTED_HEADER)
        writer.writerows(line.split(",") for line in ROWS)
    bronze = read_landed_csv(spark, [str(path)]).withColumn(
        "_source_file", F.col("_metadata.file_name")
    )
    return split_bronze(bronze)


def versions_of(dim: DataFrame, code_column: str, code: str) -> list[dict]:
    return [
        row.asDict()
        for row in dim.filter(F.col(code_column) == code)
        .orderBy("valid_from")
        .collect()
    ]


def new_table(dim: DataFrame, path: Path) -> DeltaTable:
    """An empty Delta table with the dimension's columns, as the notebook creates it."""
    spark = dim.sparkSession
    dim.limit(0).write.format("delta").save(str(path))
    return DeltaTable.forPath(spark, str(path))


# dim_country
# -----------


def test_bulgaria_has_three_versions(dim_country: DataFrame) -> None:
    bulgaria = [
        (row["valid_from"], row["valid_to"], row["is_intra_eu"], row["is_euro_area"])
        for row in versions_of(dim_country, "country_code", "BG")
    ]
    assert bulgaria == [
        (date(1976, 1, 1), date(2006, 12, 31), False, False),
        (date(2007, 1, 1), date(2025, 12, 31), True, False),
        (date(2026, 1, 1), OPEN_END, True, True),
    ]


def test_germany_keeps_both_rows_of_its_list_though_their_name_repeats(
    dim_country: DataFrame,
) -> None:
    germany = versions_of(dim_country, "country_code", "DE")
    assert [row["valid_from"] for row in germany] == [
        date(1976, 1, 1),
        date(1991, 1, 1),
        date(1999, 1, 1),
    ]
    attributes = ("country_name", "is_intra_eu", "is_euro_area", "region_zone")
    assert [germany[0][a] for a in attributes] == [germany[1][a] for a in attributes]


def test_qy_is_not_a_country_and_takes_the_zone_not_specified(
    dim_country: DataFrame,
) -> None:
    current = versions_of(dim_country, "country_code", "QY")[-1]
    assert current["valid_from"] == date(1994, 1, 1)
    assert current["is_current"]
    assert current["region_zone"] == "5900"
    assert current["region"].startswith("Miscellaneous countries not specified")
    assert current["is_country"] is False
    assert current["partner_category"] == "confidential"
    assert current["is_intra_eu"]


def test_the_united_kingdom_leaves_the_eu_on_2020_01_31(dim_country: DataFrame) -> None:
    united_kingdom = [
        (row["valid_from"], row["valid_to"], row["is_intra_eu"], row["is_country"])
        for row in versions_of(dim_country, "country_code", "GB")
    ]
    assert united_kingdom == [
        (date(1976, 1, 1), date(2020, 1, 31), True, True),
        (date(2020, 2, 1), OPEN_END, False, True),
    ]


def test_dim_country_keys_are_unique(dim_country: DataFrame) -> None:
    figures = dim_country.agg(
        F.count("*").alias("rows"),
        F.countDistinct("country_key").alias("keys"),
        F.countDistinct("country_code", "valid_from").alias("versions"),
    ).collect()[0]
    assert (figures["rows"], figures["keys"], figures["versions"]) == (14, 14, 14)


# dim_product
# -----------


def test_a_chapter_x_code_stops_at_its_chapter(dim_product: DataFrame) -> None:
    (x_code,) = versions_of(dim_product, "product_nc", "28XXXXXX")
    assert x_code["is_x_code"]
    assert (x_code["hs6"], x_code["hs4"], x_code["hs2"]) == (None, None, "28")
    assert x_code["hs2_description"].startswith("INORGANIC CHEMICALS")
    assert x_code["section_code"] == "06"


def test_an_eight_digit_code_reaches_every_level(dim_product: DataFrame) -> None:
    (meat,) = versions_of(dim_product, "product_nc", "02013000")
    assert not meat["is_x_code"]
    assert [meat[level] for level in ("hs6", "hs4", "hs2")] == ["020130", "0201", "02"]
    assert meat["hs4_description"] == "Meat of bovine animals, fresh or chilled"
    assert meat["hs2_description"] == "MEAT AND EDIBLE MEAT OFFAL"
    assert (
        meat["section_description"] == "Live animals; animal products (chapters 1 to 5)"
    )


def test_the_rows_of_a_parent_add_no_version_before_the_code_starts(
    dim_product: DataFrame,
) -> None:
    # 0106 has a row up to 2001 and another from 2002; 01061900 starts in 2012.
    (mammals,) = versions_of(dim_product, "product_nc", "01061900")
    assert (mammals["valid_from"], mammals["valid_to"]) == (date(2012, 1, 1), OPEN_END)
    assert mammals["hs4_description"].startswith("Live animals (excl. horses")


def test_one_code_closes_and_another_opens_with_2026(dim_product: DataFrame) -> None:
    (closed,) = versions_of(dim_product, "product_nc", "28419085")
    (opened,) = versions_of(dim_product, "product_nc", "28419040")
    assert (closed["valid_to"], closed["is_current"]) == (date(2025, 12, 31), False)
    assert (opened["valid_from"], opened["is_current"]) == (date(2026, 1, 1), True)
    assert dim_product.count() == 5


# as_of and the MERGE
# -------------------


def as_of(dim: DataFrame, day: date) -> DataFrame:
    """A versioned dimension as it stood on day, as the table held it then.

    The versions started by then, and any version still open that day taken
    as current.
    """
    still_open = F.col("valid_to") >= F.lit(day)
    return dim.filter(F.col("valid_from") <= F.lit(day)).withColumns(
        {
            "valid_to": F.when(still_open, F.lit(OPEN_END)).otherwise(
                F.col("valid_to")
            ),
            "is_current": still_open,
        }
    )


def test_bulgaria_as_it_stood_on_2025_12_31(dim_country: DataFrame) -> None:
    then = as_of(dim_country, SNAPSHOT_DAY)
    bulgaria = [
        (row["valid_from"], row["valid_to"], row["is_euro_area"], row["is_current"])
        for row in versions_of(then, "country_code", "BG")
    ]
    assert bulgaria == [
        (date(1976, 1, 1), date(2006, 12, 31), False, False),
        (date(2007, 1, 1), OPEN_END, False, True),
    ]


def test_a_first_load_inserts_every_version(
    dim_country: DataFrame, tmp_path: Path
) -> None:
    table = new_table(dim_country, tmp_path / "dim_country")
    counts = gold.merge_dimension(table, dim_country, "country_code")
    assert counts == {"updated": 0, "inserted": 14, "missing": 0}
    assert table.toDF().count() == 14


@pytest.mark.parametrize(
    ("name", "code_column", "before"),
    [("dim_country", "country_code", 13), ("dim_product", "product_nc", 4)],
)
def test_the_changes_of_january_2026_go_through_the_merge(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    name: str,
    code_column: str,
    before: int,
) -> None:
    dim = request.getfixturevalue(name)
    table = new_table(dim, tmp_path / name)
    gold.merge_dimension(table, as_of(dim, SNAPSHOT_DAY), code_column)
    assert table.toDF().count() == before

    counts = gold.merge_dimension(table, dim, code_column)
    assert counts == {"updated": 1, "inserted": 1, "missing": 0}
    merged = table.toDF().orderBy(code_column, "valid_from").collect()
    assert merged == dim.orderBy(code_column, "valid_from").collect()


@pytest.mark.parametrize(
    ("name", "code_column"),
    [("dim_country", "country_code"), ("dim_product", "product_nc")],
)
def test_the_same_merge_again_changes_nothing(
    request: pytest.FixtureRequest, tmp_path: Path, name: str, code_column: str
) -> None:
    dim = request.getfixturevalue(name)
    table = new_table(dim, tmp_path / name)
    gold.merge_dimension(table, dim, code_column)
    counts = gold.merge_dimension(table, dim, code_column)
    assert counts == {"updated": 0, "inserted": 0, "missing": 0}


def test_a_repeated_key_is_refused_before_the_table_is_touched(
    dim_country: DataFrame, tmp_path: Path
) -> None:
    table = new_table(dim_country, tmp_path / "dim_country")
    gold.merge_dimension(table, dim_country, "country_code")
    version = table.history(1).collect()[0]["version"]
    twice = dim_country.unionByName(dim_country.filter(F.col("country_code") == "BG"))
    with pytest.raises(ValueError, match="repeat in the source"):
        gold.merge_dimension(table, twice, "country_code")
    assert table.history(1).collect()[0]["version"] == version
    assert table.toDF().count() == 14


def test_a_version_missing_from_the_source_is_counted_not_deleted(
    dim_country: DataFrame, tmp_path: Path
) -> None:
    table = new_table(dim_country, tmp_path / "dim_country")
    gold.merge_dimension(table, dim_country, "country_code")
    without_old_qy = dim_country.filter(
        ~((F.col("country_code") == "QY") & (F.col("valid_from") < date(1988, 1, 1)))
    )
    counts = gold.merge_dimension(table, without_old_qy, "country_code")
    assert counts == {"updated": 0, "inserted": 0, "missing": 1}
    assert table.toDF().count() == 14


# Facts
# -----


@pytest.fixture(scope="module")
def facts(
    silver: tuple[DataFrame, DataFrame],
    dim_country: DataFrame,
    dim_product: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    detail, totals = silver
    return (
        gold.build_fct_trade_detail(detail, dim_country, dim_product).cache(),
        gold.build_fct_trade_totals(totals, dim_country).cache(),
    )


def key_of(dim: DataFrame, key: str, code_column: str, code: str, day: date) -> int:
    (row,) = dim.filter(
        (F.col(code_column) == code)
        & F.lit(day).between(F.col("valid_from"), F.col("valid_to"))
    ).collect()
    return row[key]


def test_every_silver_row_is_one_fact_row(
    silver: tuple[DataFrame, DataFrame], facts: tuple[DataFrame, DataFrame]
) -> None:
    assert [df.count() for df in facts] == [df.count() for df in silver] == [5, 2]


def test_bulgaria_in_may_2026_takes_its_euro_area_version(
    facts: tuple[DataFrame, DataFrame], dim_country: DataFrame
) -> None:
    bulgaria = key_of(
        dim_country, "country_key", "country_code", "BG", date(2026, 5, 1)
    )
    detail, totals = facts
    for fact in (detail, totals):
        (row,) = fact.filter(F.col("value_eur").isin(289, 29_957_023)).collect()
        assert row["reporter_key"] == bulgaria
    (version,) = dim_country.filter(F.col("country_key") == bulgaria).collect()
    assert (version["valid_from"], version["is_euro_area"]) == (date(2026, 1, 1), True)


def test_germany_and_the_united_kingdom_resolve_to_one_version_each(
    facts: tuple[DataFrame, DataFrame], dim_country: DataFrame
) -> None:
    may = date(2026, 5, 1)
    germany = key_of(dim_country, "country_key", "country_code", "DE", may)
    united_kingdom = key_of(dim_country, "country_key", "country_code", "GB", may)
    detail, _ = facts
    reporters = [
        row["reporter_key"]
        for row in detail.filter(F.col("value_eur").isin(36_905, 8_859)).collect()
    ]
    assert reporters == [germany, germany]
    assert {row["partner_key"] for row in detail.collect()} == {united_kingdom}


def test_a_code_the_dimension_does_not_hold_leaves_its_key_null(
    facts: tuple[DataFrame, DataFrame],
) -> None:
    detail, totals = facts
    without = detail.filter(F.col("product_key").isNull()).collect()
    assert [row["value_eur"] for row in without] == [8_859]
    for fact in (detail, totals):
        assert (
            fact.filter(
                F.col("reporter_key").isNull() | F.col("partner_key").isNull()
            ).count()
            == 0
        )


def test_the_columns_and_types_of_each_fact(facts: tuple[DataFrame, DataFrame]) -> None:
    detail, totals = facts
    assert tuple(detail.columns) == gold.DETAIL_FACT_COLUMNS
    assert tuple(totals.columns) == gold.TOTALS_FACT_COLUMNS
    types = dict(detail.dtypes)
    assert [types[key] for key in ("reporter_key", "partner_key", "product_key")] == [
        "bigint"
    ] * 3
    assert (types["value_eur"], types["quantity_kg"]) == ("decimal(18,3)", "bigint")
