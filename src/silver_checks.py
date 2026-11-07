"""The data quality checks of the silver layer, and the failures each one expects.

Each check counts, per source file, what it read and what breaks its rule, and
returns a DataFrame of (_source_file, rows_checked, rows_failed) with one row
per file it could evaluate. A configured file missing from that result was not
evaluated: its rows_failed is NULL, neither a pass nor a failure. What a row
of the count is depends on the check: a row of the table it reads, or a
published figure for check 10.

The rules, and the measurements behind each expected failure, are in
docs/engineering-notes.md, section Quality checks. No check starts a Python
worker, which on the development machine costs about ten seconds per
query: the code lists and the copies of the published figures are read with
Spark's own readers, and every rule is a Column. The numbers of the checks
retired are not reused, so a name in ops.dq_check_results keeps its meaning.
"""

from __future__ import annotations

import functools
import math
import operator
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from silver import (
    NO_SUPPLEMENTARY_UNIT,
    QUANTITY_TYPE,
    TOTAL_PRODUCT,
    VALUE_TYPE,
)

# The checks, in the order of docs/engineering-notes.md. The number keeps them
# in that order wherever their results are sorted by name.
CHECKS = (
    "01_country_valid",
    "02_product_valid",
    "03_codes_in_domain",
    "04_period_matches_file",
    "05_values_not_negative",
    "06a_kg_zero_with_value",
    "08b_total_row_shape",
    "09_no_duplicate_on_grain",
    "10_totals_match_published",
    "11_trade_type_matches_eu",
    "12a_cast_lossless",
    "12b_null_only_from_sentinel",
    "12c_no_su_quantity_zero",
)

# Expected failures
# -----------------
# Every month expects the same rule, whatever the month and its publication,
# so that a month new to the pipeline, or republished, needs nothing declared:
# what EXPECTED names, and 0 for any other check. A count that differs from its
# expectation, up or down, or falls outside its range, fails the run.


@dataclass(frozen=True)
class Rate:
    """An expected share of the rows a check reads: from low to high, both included."""

    low: Decimal
    high: Decimal


Expectation = int | Rate | None

# The rate holds the six months of 202601 to 202606 with a margin: 06a found
# 5.312% to 5.586% of the detail rows.
EXPECTED: dict[str, Expectation] = {
    # A weight of 0 is not resolved: nothing tells "not reported" from "less
    # than half a kilogram". Counted, not dropped.
    "06a_kg_zero_with_value": Rate(Decimal("0.050"), Decimal("0.060")),
}

# The grain of each silver table: no two rows share these columns.
DETAIL_GRAIN = (
    "reporter",
    "partner",
    "product_nc",
    "product_sitc",
    "product_cpa21",
    "product_cpa22",
    "product_bec",
    "product_bec5",
    "flow",
    "stat_procedure",
    "period",
)
TOTALS_GRAIN = ("reporter", "partner", "flow", "stat_procedure", "period")

# The five parallel classifications of a bronze row, each TOTAL in a TOTAL row.
CLASSIFICATIONS = (
    "PRODUCT_SITC",
    "PRODUCT_CPA21",
    "PRODUCT_CPA22",
    "PRODUCT_BEC",
    "PRODUCT_BEC5",
)

# An X code keeps the chapter, the heading or the subheading.
X_CODE = r"^(\d{2}X{6}|\d{4}X{4}|\d{6}X{2})$"

# The zone of GEOZONES-PARTNERS_ISO.txt that holds the EU members and the
# intra-EU Q codes.
EU_ZONE = "1010"

# Check 10: a figure of the totals within this many million EUR of the
# published one matches it.
PUBLISHED_TOLERANCE_MEUR = Decimal("0.05")

# A cast that loses nothing on any value measured: the silver types hold at
# most 3 decimals and 13 integer digits.
EXACT_TYPE = "decimal(38,10)"

# The bronze columns silver casts, and the type each gets.
CAST_COLUMNS = {
    "VALUE_EUR": VALUE_TYPE,
    "VALUE_NAC": VALUE_TYPE,
    "QUANTITY_KG": QUANTITY_TYPE,
    "QUANTITY_SUPPL_UNIT": QUANTITY_TYPE,
}


# Readers
# -------


def read_code_list(spark: SparkSession, path: str, columns: Sequence[str]) -> DataFrame:
    """A COMEXT code list: tab separated, no header, every field trimmed.

    Read as text and split rather than with the CSV reader, which would take a
    quote inside a description for the start of a quoted field. valid_from and
    valid_to become dates; 31/12/2500 means still valid. A row with another
    number of fields stops the read.
    """
    lines = spark.read.text(path).filter(F.trim("value") != "")
    fields = F.split("value", "\t")
    widths = sorted(row[0] for row in lines.select(F.size(fields)).distinct().collect())
    if widths != [len(columns)]:
        raise ValueError(f"{path}: rows of {widths} fields, expected {len(columns)}")
    table = lines.select(
        *(F.trim(fields[i]).alias(name) for i, name in enumerate(columns))
    )
    dates = {
        name: F.to_date(name, "dd/MM/yyyy")
        for name in ("valid_from", "valid_to")
        if name in columns
    }
    # Spark Connect, as on serverless, refuses a withColumns of no column.
    return table.withColumns(dates) if dates else table


def read_euro_area(spark: SparkSession, path: str) -> DataFrame:
    """The euro area members of the seed, each with the day it joined.

    Read by the gold model for dim_country, next to the code lists.
    """
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(
            "country_code string, country_name string, euro_from date, source string"
        )
        .csv(path)
        .select("country_code", "euro_from")
    )


def read_published(spark: SparkSession, paths: Sequence[str]) -> DataFrame:
    """The copies of the published figures, each named after its landed file.

    With no path, an empty DataFrame of the same columns.
    """
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(
            "period string, partner string, stk_flow string, "
            "value_meur decimal(18,3), table_updated string"
        )
        .csv(list(paths))
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )


# Shared steps
# ------------


def _by_file(rows: DataFrame, failing: Column) -> DataFrame:
    """Count per source file the rows read, and those for which failing holds."""
    return rows.groupBy("_source_file").agg(
        F.count("*").alias("rows_checked"),
        F.count(F.when(failing, True)).alias("rows_failed"),
    )


def _valid_codes(code_list: DataFrame, rows: DataFrame) -> DataFrame:
    """The codes valid on each period_date of rows, one row per code and date.

    A code has a row per period of validity, so a join on the code alone
    would pair a row with each of them. These pairs match a row at most once.
    """
    dates = rows.select("period_date").distinct()
    valid = code_list.join(
        dates, F.col("period_date").between(F.col("valid_from"), F.col("valid_to"))
    )
    return valid.select("code", "period_date").distinct()


def _flag_valid(rows: DataFrame, column: str, valid: DataFrame, flag: str) -> DataFrame:
    """Add flag: whether column holds a code valid on the row's period_date.

    valid comes from _valid_codes(), so the join adds no row.
    """
    pairs = valid.withColumnRenamed("code", column).withColumn(flag, F.lit(True))
    return rows.join(F.broadcast(pairs), [column, "period_date"], "left").withColumn(
        flag, F.coalesce(flag, F.lit(False))
    )


def _exact(column: str) -> Column:
    """A bronze value cast without loss, or NULL if it is not a number."""
    return F.col(column).try_cast(EXACT_TYPE)


# The checks
# ----------


def country_valid(rows: DataFrame, partners_iso: DataFrame) -> DataFrame:
    """01: reporter and partner have a row of PARTNERS_ISO.txt valid on period_date."""
    valid = _valid_codes(partners_iso, rows)
    flagged = _flag_valid(rows, "reporter", valid, "reporter_valid")
    flagged = _flag_valid(flagged, "partner", valid, "partner_valid")
    return _by_file(flagged, ~(F.col("reporter_valid") & F.col("partner_valid")))


def product_valid(detail: DataFrame, cn: DataFrame) -> DataFrame:
    """02: eight digits valid in CN.txt on period_date, or an X code."""
    nc8 = cn.filter(F.col("code").rlike(r"^\d{8}$"))
    flagged = _flag_valid(detail, "product_nc", _valid_codes(nc8, detail), "nc8_valid")
    return _by_file(flagged, ~(F.col("nc8_valid") | F.col("product_nc").rlike(X_CODE)))


def codes_in_domain(rows: DataFrame, procedures: DataFrame) -> DataFrame:
    """03: flow 1 or 2, trade type I or E, a procedure valid on period_date."""
    flagged = _flag_valid(
        rows, "stat_procedure", _valid_codes(procedures, rows), "procedure_valid"
    )
    return _by_file(
        flagged,
        ~F.col("flow").isin("1", "2")
        | ~F.col("trade_type").isin("I", "E")
        | ~F.col("procedure_valid"),
    )


def period_matches_file(rows: DataFrame) -> DataFrame:
    """04: period is the month in the name of the source file."""
    file_period = F.regexp_extract("_source_file", r"_(\d{6})\.csv$", 1)
    return _by_file(rows, F.col("period") != file_period)


def values_not_negative(rows: DataFrame) -> DataFrame:
    """05: neither value is negative."""
    return _by_file(rows, (F.col("value_eur") < 0) | (F.col("value_nac") < 0))


def kg_zero_with_value(detail: DataFrame) -> DataFrame:
    """06a: a weight of 0 with a positive value."""
    return _by_file(detail, (F.col("quantity_kg") == 0) & (F.col("value_eur") > 0))


def total_row_shape(bronze: DataFrame) -> DataFrame:
    """08b: every TOTAL row reads TOTAL in the classifications, TO, NO_SU and 0.

    Read from bronze: the totals table leaves those columns out.
    """
    totals = bronze.filter(F.col("PRODUCT_NC") == TOTAL_PRODUCT)
    shape = functools.reduce(
        operator.and_,
        [F.col(name) == TOTAL_PRODUCT for name in CLASSIFICATIONS],
        (F.col("PRODUCT_SECTION") == "TO")
        & (F.col("SUPPL_UNIT") == NO_SUPPLEMENTARY_UNIT)
        & _exact("QUANTITY_SUPPL_UNIT").eqNullSafe(0),
    )
    return _by_file(totals, ~F.coalesce(shape, F.lit(False)))


def no_duplicate_on_grain(rows: DataFrame, grain: Sequence[str]) -> DataFrame:
    """09: no two rows share the grain. Counts the rows of the repeated keys."""
    keys = rows.groupBy("_source_file", *grain).count()
    return keys.groupBy("_source_file").agg(
        F.sum("count").alias("rows_checked"),
        F.sum(F.when(F.col("count") > 1, F.col("count")).otherwise(0)).alias(
            "rows_failed"
        ),
    )


def totals_match_published(totals: DataFrame, published: DataFrame) -> DataFrame:
    """10: the totals against the copy of the published figures saved with the file.

    Compared by trade type and flow against EU27_2020 and EXT_EU27_2020, and
    GB against UK. Counts the figures of the copy; each needs a figure of the
    totals within the tolerance. A file without a copy is not evaluated.
    """
    stk_flow = F.when(F.col("flow") == "1", "IMP").when(F.col("flow") == "2", "EXP")
    meur = (F.sum("value_eur") / 1_000_000).alias("landed_meur")
    by_trade_type = totals.groupBy(
        "_source_file",
        "period",
        F.when(F.col("trade_type") == "I", "EU27_2020")
        .when(F.col("trade_type") == "E", "EXT_EU27_2020")
        .alias("partner"),
        stk_flow.alias("stk_flow"),
    ).agg(meur)
    uk = (
        totals.filter(F.col("partner") == "GB")
        .groupBy(
            "_source_file",
            "period",
            F.lit("UK").alias("partner"),
            stk_flow.alias("stk_flow"),
        )
        .agg(meur)
    )
    compared = published.join(
        by_trade_type.unionByName(uk),
        ["_source_file", "period", "partner", "stk_flow"],
        "left",
    )
    gap = F.abs(F.col("landed_meur") - F.col("value_meur"))
    return _by_file(compared, F.coalesce(gap > PUBLISHED_TOLERANCE_MEUR, F.lit(True)))


def trade_type_matches_eu(rows: DataFrame, geozones: DataFrame) -> DataFrame:
    """11: trade type I exactly when the partner is in zone 1010 on period_date."""
    eu = geozones.filter(F.col("zone") == EU_ZONE)
    flagged = _flag_valid(rows, "partner", _valid_codes(eu, rows), "partner_in_eu")
    return _by_file(flagged, (F.col("trade_type") == "I") != F.col("partner_in_eu"))


def cast_lossless(bronze: DataFrame) -> DataFrame:
    """12a: each value cast to its silver type equals the same value cast without loss.

    Read from bronze, where the value is still the text of the file. A value
    that is not a number fails too.
    """
    lost = [
        _exact(name).isNull()
        | ~F.col(name).try_cast(silver_type).eqNullSafe(_exact(name))
        for name, silver_type in CAST_COLUMNS.items()
    ]
    return _by_file(bronze, functools.reduce(operator.or_, lost))


def null_only_from_sentinel(rows: DataFrame) -> DataFrame:
    """12b: a NULL only where silver resolved NO_SU, or below the level of an X code.

    suppl_unit and quantity_suppl_unit are NULL together, and hs4 and hs6
    only in an X code. Every other column of either table is never NULL.
    """
    allowed = ("suppl_unit", "quantity_suppl_unit", "hs4", "hs6")
    failing = functools.reduce(
        operator.or_,
        [F.col(name).isNull() for name in rows.columns if name not in allowed],
    )
    if "suppl_unit" in rows.columns:
        failing = failing | (
            F.col("suppl_unit").isNull() != F.col("quantity_suppl_unit").isNull()
        )
    for level in ("hs4", "hs6"):
        if level in rows.columns:
            failing = failing | (F.col(level).isNull() & ~F.col("is_x_code"))
    return _by_file(rows, failing)


def no_su_quantity_zero(bronze: DataFrame) -> DataFrame:
    """12c: NO_SU always comes with a quantity of 0, which silver makes NULL.

    Read from bronze, the only place the quantity is still there.
    """
    return _by_file(
        bronze,
        (F.col("SUPPL_UNIT") == NO_SUPPLEMENTARY_UNIT)
        & ~_exact("QUANTITY_SUPPL_UNIT").eqNullSafe(0),
    )


def all_checks(
    *,
    bronze: DataFrame,
    detail: DataFrame,
    totals: DataFrame,
    partners_iso: DataFrame,
    cn: DataFrame,
    procedures: DataFrame,
    geozones: DataFrame,
    published: DataFrame,
) -> list[tuple[str, str, DataFrame]]:
    """Every check, with the table it reads: bronze, detail or totals.

    bronze holds the rows of the files checked.
    """
    return [
        ("01_country_valid", "detail", country_valid(detail, partners_iso)),
        ("01_country_valid", "totals", country_valid(totals, partners_iso)),
        ("02_product_valid", "detail", product_valid(detail, cn)),
        ("03_codes_in_domain", "detail", codes_in_domain(detail, procedures)),
        ("03_codes_in_domain", "totals", codes_in_domain(totals, procedures)),
        ("04_period_matches_file", "detail", period_matches_file(detail)),
        ("04_period_matches_file", "totals", period_matches_file(totals)),
        ("05_values_not_negative", "detail", values_not_negative(detail)),
        ("05_values_not_negative", "totals", values_not_negative(totals)),
        ("06a_kg_zero_with_value", "detail", kg_zero_with_value(detail)),
        ("08b_total_row_shape", "bronze", total_row_shape(bronze)),
        (
            "09_no_duplicate_on_grain",
            "detail",
            no_duplicate_on_grain(detail, DETAIL_GRAIN),
        ),
        (
            "09_no_duplicate_on_grain",
            "totals",
            no_duplicate_on_grain(totals, TOTALS_GRAIN),
        ),
        (
            "10_totals_match_published",
            "totals",
            totals_match_published(totals, published),
        ),
        ("11_trade_type_matches_eu", "detail", trade_type_matches_eu(detail, geozones)),
        ("11_trade_type_matches_eu", "totals", trade_type_matches_eu(totals, geozones)),
        ("12a_cast_lossless", "bronze", cast_lossless(bronze)),
        ("12b_null_only_from_sentinel", "detail", null_only_from_sentinel(detail)),
        ("12b_null_only_from_sentinel", "totals", null_only_from_sentinel(totals)),
        ("12c_no_su_quantity_zero", "bronze", no_su_quantity_zero(bronze)),
    ]


# Comparison
# ----------


def expected_failures(check_name: str, *, has_copy: bool) -> Expectation:
    """What a silver check is expected to find in a file: a count, a Rate, or None.

    has_copy says whether the file landed with a copy of the published figures.
    Without one, check 10 has nothing to compare and is not evaluated: the
    table had no figure for the month yet when it was landed.
    """
    if check_name == "10_totals_match_published" and not has_copy:
        return None
    return EXPECTED.get(check_name, 0)


def expected_bounds(
    expectation: Expectation, rows_checked: int | None
) -> tuple[int | None, int | None, int | None]:
    """The expected_failures, expected_min and expected_max that a result records.

    A count, or None, is expected_failures. A Rate is the range of rows it
    allows out of rows_checked, rounded inward; with nothing checked, 0 to 0,
    so that a check that could not evaluate the file does not pass.
    """
    if isinstance(expectation, Rate):
        checked = rows_checked or 0
        return (
            None,
            math.ceil(checked * expectation.low),
            math.floor(checked * expectation.high),
        )
    return expectation, None, None


def mismatches(
    results: Iterable[Mapping[str, Any]], checks: Sequence[str] = CHECKS
) -> list[str]:
    """Each result off its expectation, either way.

    A result with expected_min and expected_max is off outside that range; any
    other, where rows_failed is not expected_failures. NULL against a number is
    a mismatch too, and so is a check of checks that reported nothing or a name
    that is not in it. checks are the silver checks unless given: the gold
    checks are compared by the same rule.
    """
    found = []
    reported = set()
    for result in results:
        reported.add(result["check_name"])
        failed = result["rows_failed"]
        low, high = result.get("expected_min"), result.get("expected_max")
        if low is not None and high is not None:
            off = failed is None or not low <= failed <= high
            expected = f"{low} to {high}"
        else:
            off = failed != result["expected_failures"]
            expected = result["expected_failures"]
        if off:
            # A check of a gold dimension reads every version, and no file.
            source_file = result["source_file"]
            where = result["table_name"] + (f", {source_file}" if source_file else "")
            found.append(
                f"{result['check_name']} on {where}: {failed} failed, {expected} expected"
            )
    found += [f"{name}: no result" for name in checks if name not in reported]
    found += [f"{name}: not a check" for name in sorted(reported - set(checks))]
    return found
