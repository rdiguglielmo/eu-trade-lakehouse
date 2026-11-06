"""The views of the business questions on a local Spark, over real rows.

Every row below is a line of a landed file of the 2026-09-15 publication, but
the two of June and July, of 2026-10-16, and every code list line a line of
the landed code list, with its own line endings, unless a test changes a field
to make its point. The rows reach the
views as in the notebooks: written to a landed file, read with the bronze
reader, split by split_bronze() and built into the facts of gold against
dimensions built from those lines. The facts and the dimensions are registered
as temporary views under their table names, and the views of
sql/gold_views.sql are created over them as temporary views. Uses the module's
Spark session from conftest.py. Run without these tests with -m "not spark".
"""

from __future__ import annotations

import csv
import functools
import operator
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

import gold
import gold_views
from bronze import read_landed_csv
from config import EXPECTED_HEADER, REFERENCE_DIR
from silver import split_bronze
from silver_checks import read_code_list, read_euro_area

pytestmark = pytest.mark.spark

MAY = "full_v2_202605.csv"
JUNE = "full_v2_202606.csv"
JULY = "full_v2_202607.csv"


def change(line: str, **fields: str) -> str:
    """A source line with fields replaced by name."""
    values = dict(zip(EXPECTED_HEADER, line.split(","), strict=True))
    values.update(fields)
    return ",".join(values.values())


# full_v2_202605.csv. Slovenia's imports from Switzerland under three
# procedures, from Germany, and under the two confidential codes: QY within the
# EU and QZ outside it.
SI_CH_NORMAL = "SI,CH,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202605,27387351,27387351,798604,0"
SI_CH_INWARD = "SI,CH,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,2,NO_SU,202605,1878516369,1878516369,64463,0"
SI_CH_OUTWARD = "SI,CH,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,3,NO_SU,202605,893176,893176,2394,0"
SI_DE = "SI,DE,I,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202605,652110174,652110174,103432514,0"
SI_QY = "SI,QY,I,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202605,1744733,1744733,20239,0"
SI_QZ = "SI,QZ,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202605,8180028,8180028,13549,0"
# The same trade under QY and QZ, by product: all of it in chapter 99.
SI_QY_99 = "SI,QY,I,99XXXXXX,9XXXX,XXXX,XXXX,700,X1XXXX,XX,1,1,NO_SU,202605,1744733,1744733,20239,0"
SI_QZ_99 = "SI,QZ,E,99XXXXXX,9XXXX,XXXX,XXXX,700,X1XXXX,XX,1,1,NO_SU,202605,8180028,8180028,13549,0"
# Lines with GB: Austria's imports from the United Kingdom, and rows by
# product.
AT_GB_MAY = "AT,GB,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202605,109475558,109475558,6811188,0"
AT_02_EXPORT = (
    "AT,GB,E,02013000,01112,1011,1011,122,113101,01,2,1,NO_SU,202605,17172,17172,1643,0"
)
AT_28_IMPORT = (
    "AT,GB,E,28XXXXXX,522XX,20XX,20XX,700,21XXXX,06,1,1,NO_SU,202605,2713,2713,51,0"
)
AT_28_EXPORT = "AT,GB,E,28XXXXXX,522XX,20XX,20XX,700,21XXXX,06,2,1,NO_SU,202605,85466,85466,25751,0"
# A row of chapter 99 that reads section 04.
GR_99 = "GR,GB,E,99XXXXXX,9XXXX,XXXX,XXXX,700,11XXXX,04,2,1,NO_SU,202605,1,1,1,0"
# A value of 0, with a weight.
BE_11_NO_VALUE = (
    "BE,GB,E,11081100,59211,1062,1062,220,114200,02,2,1,NO_SU,202605,0,0,424160,0"
)
# Finland's imports of heading 2511, every row: from China, from Türkiye, and
# under QY an X code of the heading, larger than either.
FI_CN_2511 = "FI,CN,E,25111000,27892,0891,0891,210,311100,05,1,1,NO_SU,202605,16861,16861,20000,0"
FI_QY_2511 = "FI,QY,I,2511XXXX,278XX,0891,0891,210,311100,05,1,1,NO_SU,202605,85394,85394,125900,0"
FI_TR_2511 = "FI,TR,E,25111000,27892,0891,0891,210,311100,05,1,1,NO_SU,202605,69355,69355,143000,0"
# Croatia's imports of heading 2525, all of them under QW.
HR_QW_2525 = (
    "HR,QW,E,25252000,27852,0899,0899,210,311100,05,1,1,NO_SU,202605,172,172,2,0"
)
# France's exports of the product of AT_02_EXPORT to the United Kingdom.
FR_02_EXPORT = (
    "FR,GB,E,02013000,01112,1011,1011,122,113101,01,2,1,NO_SU,202605,22400,22400,1468,0"
)
# Malta's imports of electricity from Italy: a value and MWh, without a weight.
MT_IT_2716 = "MT,IT,I,27160000,35100,3511,3511,322,214200,05,1,1,1000_KWH,202605,9566617,9566617,0,82179"
# Austria's imports from the United Kingdom in the next two months, lines of
# full_v2_202606.csv and full_v2_202607.csv as published on 2026-10-16.
AT_GB_JUNE = "AT,GB,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202606,140214668,140214668,8510580,0"
AT_GB_JULY = "AT,GB,E,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TOTAL,TO,1,1,NO_SU,202607,123421691,123421691,7630930,0"

SI_TOTALS = (SI_CH_NORMAL, SI_CH_INWARD, SI_CH_OUTWARD, SI_DE, SI_QY, SI_QZ)
CLEAN = {
    MAY: (
        *SI_TOTALS,
        SI_QY_99,
        SI_QZ_99,
        AT_GB_MAY,
        AT_02_EXPORT,
        AT_28_IMPORT,
        AT_28_EXPORT,
        GR_99,
        BE_11_NO_VALUE,
        FI_CN_2511,
        FI_QY_2511,
        FI_TR_2511,
        HR_QW_2525,
        FR_02_EXPORT,
        MT_IT_2716,
    ),
    JUNE: (AT_GB_JUNE,),
}

# Slovenia's imports from Switzerland over its three procedures, and from
# every partner above.
SI_FROM_CH = 27387351 + 1878516369 + 893176
SI_FROM_ALL = SI_FROM_CH + 652110174 + 1744733 + 8180028


def lines(ending: str, *rows: str) -> str:
    """Code list lines as landed, each followed by its line ending."""
    return "".join(row + ending for row in rows)


CODE_LISTS = {
    "PARTNERS_ISO.txt": (
        ("code", "valid_from", "valid_to", "name"),
        lines(
            "\r\n",
            "AT\t01/01/1976\t31/12/2500\tAustria",
            "BE\t01/01/1976\t31/12/1998\tBelgium and Luxembourg",
            "BE\t01/01/1999\t31/12/2500\tBelgium",
            "CH\t01/01/1976\t31/12/1994\tSwitzerland",
            "CH\t01/01/1995\t31/12/2500\tSwitzerland",
            "CN\t01/01/1976\t31/12/2500\tChina",
            "DE\t01/01/1976\t31/12/1990\tGermany",
            "DE\t01/01/1991\t31/12/2500\tGermany",
            "FI\t01/01/1976\t31/12/2500\tFinland",
            "FR\t01/01/1976\t31/12/1996\tFrance",
            "FR\t01/01/1997\t31/12/2500\tFrance",
            "GB\t01/01/1976\t31/12/2500\tUnited Kingdom",
            "GR\t01/01/1976\t31/12/1980\tGreece",
            "GR\t01/01/1981\t31/12/2500\tGreece",
            "HR\t01/01/1992\t31/12/1992\tCroatia",
            "HR\t01/01/1993\t31/12/2500\tCroatia",
            "IT\t01/01/1976\t31/12/1993\tItaly",
            "IT\t01/01/1994\t31/12/2500\tItaly",
            "MT\t01/01/1976\t31/12/2500\tMalta",
            "QW\t01/01/1993\t31/12/2500\tCountries and territories not specified "
            "within the framework of trade with third countries",
            "QY\t01/01/1976\t31/12/1993\tSecret countries Intra",
            "QY\t01/01/1994\t31/12/2500\tCountries and territories not specified "
            "for commercial or military reasons in the framework of Intra_EU trade",
            "QZ\t01/01/1976\t31/12/1993\tSecret countries Extra",
            "QZ\t01/01/1994\t31/12/2500\tCountries and territories not specified "
            "for commercial or military reasons in the framework of trade with third "
            "countries",
            "SI\t01/01/1992\t31/12/1992\tSlovenia",
            "SI\t01/01/1993\t31/12/2500\tSlovenia",
            "TR\t01/01/1976\t31/12/2500\tTürkiye",
        ),
    ),
    "GEOZONES-PARTNERS_ISO.txt": (
        ("zone", "code", "valid_from", "valid_to"),
        lines(
            "\r\n",
            "1010\tAT\t01/01/1995\t31/12/2500",
            "1010\tDE\t01/01/1991\t31/12/2500",
            "1010\tDE\t01/01/1976\t31/12/1990",
        ),
    ),
    "GEOZONES.txt": (
        ("zone", "valid_from", "valid_to", "zone_name"),
        lines("\n", "1010\t01/01/1976\t31/12/2500\tEuropean union (evolutionary)"),
    ),
    "CN.txt": (
        ("code", "valid_from", "valid_to", "description"),
        lines(
            "\n",
            "02\t01/01/1988\t31/12/2500\tMEAT AND EDIBLE MEAT OFFAL",
            "0201\t01/01/1988\t31/12/2500\tMeat of bovine animals, fresh or chilled",
            "02013000\t01/01/1988\t31/12/2500\tFresh or chilled bovine meat, boneless",
            "11\t01/01/1988\t31/12/2500\tPRODUCTS OF THE MILLING INDUSTRY; MALT; "
            "STARCHES; INULIN; WHEAT GLUTEN",
            "11081100\t01/01/1988\t31/12/2500\tWheat starch",
            "25\t01/01/1988\t31/12/2500\tSALT; SULPHUR; EARTHS AND STONE; "
            "PLASTERING MATERIALS, LIME AND CEMENT",
            '2511\t01/01/1988\t31/12/2500\tNatural barium sulphate "barytes"; '
            'natural barium carbonate "witherite", whether or not calcined '
            "(excl. barium oxide)",
            '25111000\t01/01/1988\t31/12/2500\tNatural barium sulphate "barytes"',
            "2511XXXX\t01/01/1988\t31/12/2500\tTrade of heading 2511, not elsewhere "
            "specified",
            "2525\t01/01/1988\t31/12/2500\tMica, whether or not rifted into sheets or "
            "splittings; mica waste",
            "25252000\t01/01/1988\t31/12/2500\tMica powder",
            "27\t01/01/1988\t31/12/2500\tMINERAL FUELS, MINERAL OILS AND PRODUCTS OF "
            "THEIR DISTILLATION; BITUMINOUS SUBSTANCES; MINERAL WAXES",
            "2716\t01/01/1988\t31/12/2500\tElectrical energy",
            "27160000\t01/01/1988\t31/12/2500\tElectrical energy",
            "28\t01/01/1988\t31/12/2500\tINORGANIC CHEMICALS; ORGANIC OR INORGANIC "
            "COMPOUNDS OF PRECIOUS METALS, OF RARE-EARTH METALS, OF RADIOACTIVE "
            "ELEMENTS OR OF ISOTOPES",
            "28XXXXXX\t01/01/1988\t31/12/2500\tTrade of chapter 28, not elsewhere specified",
            "99\t01/01/1988\t31/12/2500\tOther products",
            "99XXXXXX\t01/01/1988\t31/12/2500\tTrade of chapter 99, not elsewhere specified",
        ),
    ),
    "CN_SECTIONS.txt": (
        ("section_code", "section_description"),
        lines(
            "\n",
            "01\tLive animals; animal products (chapters 1 to 5)",
            "02\tVegetable products (chapters 6 to 14)",
            "06\tProducts of the chemical or allied industries (chapters 28 to 38)",
            "XX\tNot elsewhere specified",
        ),
    ),
    "CN_SECTIONS-CHAPTERS.txt": (
        ("section_code", "hs2"),
        lines("\n", "01\t02", "02\t11", "06\t28", "XX\t99"),
    ),
    # As landed: CRLF between its two lines, and nothing after the last.
    "FLOW.txt": (("flow", "flow_name"), "1\tImport\r\n2\tExport"),
}


def land(
    spark: SparkSession, folder: Path, files: dict[str, tuple[str, ...]]
) -> tuple[DataFrame, DataFrame]:
    """The detail and the totals of the rows of each file, landed under its name."""
    paths = []
    for name, rows in files.items():
        path = folder / name
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(EXPECTED_HEADER)
            writer.writerows(row.split(",") for row in rows)
        paths.append(str(path))
    bronze = read_landed_csv(spark, paths).withColumn(
        "_source_file", F.col("_metadata.file_name")
    )
    return split_bronze(bronze)


def star(
    silver: tuple[DataFrame, DataFrame], dims: dict[str, DataFrame]
) -> dict[str, DataFrame]:
    """The facts of the silver tables, keyed against dims, and every table the views read."""
    detail, totals = silver
    return {
        "fct_trade_detail": gold.build_fct_trade_detail(
            detail, dims["dim_country"], dims["dim_product"]
        ),
        "fct_trade_totals": gold.build_fct_trade_totals(totals, dims["dim_country"]),
        **dims,
        "dim_period": gold.build_dim_period(detail, totals),
    }


def create_views_over(spark: SparkSession, tables: dict[str, DataFrame]) -> list[str]:
    """Register tables as temporary views under their names, and the views of gold over them."""
    for name, df in tables.items():
        df.createOrReplaceTempView(name)
    return gold_views.create_views(spark, temporary=True)


def imports_of(spark: SparkSession, reporter: str) -> dict[str, Row]:
    """The rows of vw_partner_ranking of reporter's imports in May, by partner."""
    rows = (
        spark.table("vw_partner_ranking")
        .filter(
            (F.col("reporter_code") == reporter)
            & (F.col("flow") == "1")
            & (F.col("period") == "202605")
        )
        .collect()
    )
    return {row["partner_code"]: row for row in rows}


def of_reporter(spark: SparkSession, view: str, reporter: str) -> list[Row]:
    return spark.table(view).filter(F.col("reporter_code") == reporter).collect()


@pytest.fixture(scope="module")
def dims(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> dict[str, DataFrame]:
    """The dimensions the views read, but dim_period, which holds the months of the rows."""
    folder = tmp_path_factory.mktemp("code_lists")
    read = {}
    for name, (columns, text) in CODE_LISTS.items():
        path = folder / name
        path.write_bytes(text.encode("utf-8"))
        read[name] = read_code_list(spark, str(path), columns)
    return {
        "dim_country": gold.build_dim_country(
            read["PARTNERS_ISO.txt"],
            read["GEOZONES-PARTNERS_ISO.txt"],
            read["GEOZONES.txt"],
            read_euro_area(spark, str(REFERENCE_DIR / "euro_area_members.csv")),
            gold.read_special_codes(
                spark, str(REFERENCE_DIR / "partner_special_codes.csv")
            ),
        ).cache(),
        "dim_product": gold.build_dim_product(
            read["CN.txt"], read["CN_SECTIONS.txt"], read["CN_SECTIONS-CHAPTERS.txt"]
        ).cache(),
        "dim_flow": read["FLOW.txt"],
    }


@pytest.fixture(scope="module")
def clean(
    spark: SparkSession,
    tmp_path_factory: pytest.TempPathFactory,
    dims: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    """Every table the views read, over the CLEAN files."""
    tables = star(land(spark, tmp_path_factory.mktemp("landing"), CLEAN), dims)
    return {name: df.cache() for name, df in tables.items()}


@pytest.fixture
def views(spark: SparkSession, clean: dict[str, DataFrame]) -> SparkSession:
    """The session, with the views created over the clean tables.

    Created again for each test, since a test with rows of its own replaces
    them.
    """
    create_views_over(spark, clean)
    return spark


def test_the_file_creates_the_six_views(
    spark: SparkSession, clean: dict[str, DataFrame]
) -> None:
    assert create_views_over(spark, clean) == list(gold_views.VIEWS)


# Question 1


def test_procedures_add_up_to_one_row_per_partner(views: SparkSession) -> None:
    found = imports_of(views, "SI")
    assert sorted(found) == ["CH", "DE", "QY", "QZ"]
    assert found["CH"]["value_eur"] == SI_FROM_CH


def test_q_codes_count_in_the_total_and_take_no_rank(views: SparkSession) -> None:
    found = imports_of(views, "SI")
    assert {code: row["partner_rank"] for code, row in found.items()} == {
        "CH": 1,
        "DE": 2,
        "QY": None,
        "QZ": None,
    }
    assert {row["flow_total_eur"] for row in found.values()} == {SI_FROM_ALL}
    share = Decimal(SI_FROM_CH) / Decimal(SI_FROM_ALL)
    assert abs(found["CH"]["share_of_flow"] - share) < Decimal("1e-9")


def test_tied_partners_share_a_rank(
    spark: SparkSession, tmp_path: Path, dims: dict[str, DataFrame]
) -> None:
    # Germany's value made equal to Switzerland's over its three procedures.
    tied = change(SI_DE, VALUE_EUR=str(SI_FROM_CH), VALUE_NAC=str(SI_FROM_CH))
    rows = (SI_CH_NORMAL, SI_CH_INWARD, SI_CH_OUTWARD, tied, SI_QY, SI_QZ)
    create_views_over(spark, star(land(spark, tmp_path, {MAY: rows}), dims))
    found = imports_of(spark, "SI")
    assert (found["CH"]["partner_rank"], found["DE"]["partner_rank"]) == (1, 1)


def test_the_previous_month_is_the_calendar_month_before(
    views: SparkSession,
) -> None:
    found = {
        row["period"]: (
            row["partner_rank"],
            row["previous_month_rank"],
            row["rank_change"],
        )
        for row in of_reporter(views, "vw_partner_ranking", "AT")
    }
    assert found == {"202605": (1, None, None), "202606": (1, 1, 0)}


def test_a_month_not_loaded_leaves_no_previous_rank(
    spark: SparkSession, tmp_path: Path, dims: dict[str, DataFrame]
) -> None:
    files = {MAY: (AT_GB_MAY,), JULY: (AT_GB_JULY,)}
    create_views_over(spark, star(land(spark, tmp_path, files), dims))
    found = {
        row["period"]: row["previous_month_rank"]
        for row in spark.table("vw_partner_ranking").collect()
    }
    assert found == {"202605": None, "202607": None}


# Question 2


def test_the_balance_is_exports_minus_imports_by_chapter(
    views: SparkSession,
) -> None:
    found = {
        (row["reporter_code"], row["chapter"]): (
            row["section_code"],
            row["exports_eur"],
            row["imports_eur"],
            row["balance_eur"],
        )
        for row in views.table("vw_trade_balance_by_chapter").collect()
    }
    assert found[("AT", "02")] == ("01", 17172, 0, 17172)
    # The X code of chapter 28, in both flows.
    assert found[("AT", "28")] == ("06", 85466, 2713, 85466 - 2713)
    assert found[("SI", "99")] == ("XX", 0, 1744733 + 8180028, -(1744733 + 8180028))


def test_the_section_is_the_chapters(views: SparkSession) -> None:
    # The row reads section 04; chapter 99 is in section XX.
    found = [
        (row["chapter"], row["section_code"], row["section_description"])
        for row in of_reporter(views, "vw_trade_balance_by_chapter", "GR")
    ]
    assert found == [("99", "XX", "Not elsewhere specified")]


# Question 3


def test_intra_and_extra_follow_the_trade_type(views: SparkSession) -> None:
    by_country = [
        (row["intra_eu_eur"], row["extra_eu_eur"])
        for row in of_reporter(views, "vw_intra_extra_by_country", "SI")
    ]
    # Germany and QY within the EU; Switzerland and QZ outside it.
    assert by_country == [(652110174 + 1744733, SI_FROM_CH + 8180028)]
    by_chapter = of_reporter(views, "vw_intra_extra_by_chapter", "SI")
    assert [
        (row["chapter"], row["intra_eu_eur"], row["extra_eu_eur"]) for row in by_chapter
    ] == [("99", 1744733, 8180028)]
    share = Decimal(1744733) / Decimal(1744733 + 8180028)
    assert abs(by_chapter[0]["intra_eu_share"] - share) < Decimal("1e-9")


def test_a_chapter_without_value_has_no_share(views: SparkSession) -> None:
    found = [
        (
            row["chapter"],
            row["intra_eu_eur"],
            row["extra_eu_eur"],
            row["intra_eu_share"],
        )
        for row in of_reporter(views, "vw_intra_extra_by_chapter", "BE")
    ]
    assert found == [("11", 0, 0, None)]


# Question 4


def dependence_of(spark: SparkSession, reporter: str) -> dict[tuple[str, str], Row]:
    """The rows of vw_partner_dependence of reporter, by heading and flow."""
    return {
        (row["heading"], row["flow"]): row
        for row in of_reporter(spark, "vw_partner_dependence", reporter)
    }


def test_the_largest_partner_is_a_country_and_its_share_counts_every_partner(
    views: SparkSession,
) -> None:
    # QY holds more than either country, in an X code of the heading.
    row = dependence_of(views, "FI")[("2511", "1")]
    flow_value = 16861 + 85394 + 69355
    assert (
        row["value_eur"],
        row["top_partner_code"],
        row["top_partner_eur"],
        row["q_codes_eur"],
        row["share_of_reporter_flow"],
    ) == (flow_value, "TR", 69355, 85394, 1)
    top_share = Decimal(69355) / Decimal(flow_value)
    assert abs(row["top_partner_share"] - top_share) < Decimal("1e-9")
    q_share = Decimal(85394) / Decimal(flow_value)
    assert abs(row["q_codes_share"] - q_share) < Decimal("1e-9")
    assert row["heading_description"].startswith('Natural barium sulphate "barytes";')


def test_a_heading_without_a_country_or_a_value_names_no_partner(
    views: SparkSession,
) -> None:
    # Croatia's imports of mica, all under QW.
    hr = dependence_of(views, "HR")[("2525", "1")]
    assert (
        hr["top_partner_code"],
        hr["top_partner_share"],
        hr["q_codes_eur"],
        hr["q_codes_share"],
    ) == (None, None, 172, 1)
    # Belgium's exports of wheat starch, a weight without a value.
    be = dependence_of(views, "BE")[("1108", "2")]
    assert (
        be["value_eur"],
        be["top_partner_code"],
        be["top_partner_share"],
        be["q_codes_share"],
        be["share_of_reporter_flow"],
    ) == (0, None, None, None, None)


def test_x_codes_of_a_chapter_reach_no_heading_and_count_in_the_reporters_flow(
    views: SparkSession,
) -> None:
    # Austria's rows of 28XXXXXX: its imports have no row, and its exports of
    # heading 0201 are divided by a flow that holds them.
    found = dependence_of(views, "AT")
    assert list(found) == [("0201", "2")]
    share = Decimal(17172) / Decimal(17172 + 85466)
    assert abs(found[("0201", "2")]["share_of_reporter_flow"] - share) < Decimal("1e-9")


def test_tied_partners_name_the_lower_code(
    spark: SparkSession, tmp_path: Path, dims: dict[str, DataFrame]
) -> None:
    # Türkiye's value made equal to China's.
    tied = change(FI_TR_2511, VALUE_EUR="16861", VALUE_NAC="16861")
    rows = (FI_CN_2511, FI_QY_2511, tied)
    create_views_over(spark, star(land(spark, tmp_path, {MAY: rows}), dims))
    row = dependence_of(spark, "FI")[("2511", "1")]
    assert row["top_partner_code"] == "CN"
    share = Decimal(16861) / Decimal(16861 + 85394 + 16861)
    assert abs(row["top_partner_share"] - share) < Decimal("1e-9")


# Question 5


def value_per_kg(spark: SparkSession) -> dict[tuple[str, str, str], Row]:
    """The rows of vw_value_per_kg_by_partner, by product, flow and partner."""
    return {
        (row["product_nc"], row["flow"], row["partner_code"]): row
        for row in spark.table("vw_value_per_kg_by_partner").collect()
    }


def test_reporters_add_up_and_the_value_per_kg_divides_the_sums(
    views: SparkSession,
) -> None:
    # Austria's and France's exports of boneless beef to the United Kingdom.
    row = value_per_kg(views)[("02013000", "2", "GB")]
    assert (row["value_eur"], row["quantity_kg"]) == (17172 + 22400, 1643 + 1468)
    ratio = Decimal(17172 + 22400) / Decimal(1643 + 1468)
    assert abs(row["value_per_kg"] - ratio) < Decimal("1e-9")


def test_only_eight_digit_codes_with_a_country_a_value_and_a_weight(
    views: SparkSession,
) -> None:
    # Left out: Malta's electricity, without a weight; Croatia's mica, under
    # QW; the X codes of Finland, Austria, Slovenia and Greece; Belgium's
    # starch, without a value.
    assert sorted(value_per_kg(views)) == [
        ("02013000", "2", "GB"),
        ("25111000", "1", "CN"),
        ("25111000", "1", "TR"),
    ]


# Every view


def test_every_view_keeps_the_value_of_the_rows_it_reads_once_per_grain(
    views: SparkSession,
) -> None:
    for name, (grain, rows_read, value_columns) in gold_views.VIEWS.items():
        view = views.table(name)
        repeated = view.groupBy(*grain).count().filter(F.col("count") > 1).count()
        assert repeated == 0, name
        value = functools.reduce(operator.add, [F.col(c) for c in value_columns])
        kept = {
            row["period"]: row["value_eur"]
            for row in view.groupBy("period")
            .agg(F.sum(value).alias("value_eur"))
            .collect()
        }
        recorded = {
            row["period"]: row["value_eur"]
            for row in views.sql(rows_read)
            .groupBy("period")
            .agg(F.sum("value_eur").alias("value_eur"))
            .collect()
        }
        assert kept == recorded, name
