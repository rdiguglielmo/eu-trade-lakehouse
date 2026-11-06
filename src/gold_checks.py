"""The data quality checks of the gold layer, and the failures each one expects.

A check of a fact counts, per source file, what it read and what breaks its
rule, and returns a DataFrame of (_source_file, rows_checked, rows_failed) with
one row per file it could evaluate, as the silver checks do: a configured file
missing from that result was not evaluated, and its rows_failed is NULL. A
check of a dimension reads every version of the table, whatever file the facts
came from, and returns a single row whose _source_file is NULL. What a row of
the count is depends on the check: a row of the table it reads, the sum of a
file for check 04, a group of rows for check 07.

The rules, and the measurements behind each expected failure, are in
docs/engineering-notes.md, section Gold, The checks of gold. The results are
compared with their expectations by silver_checks.mismatches(), the rule of the
silver checks. No check starts a Python worker: every rule is a Column. As in
silver, the number of a check retired is not reused.
"""

from __future__ import annotations

import functools
import operator
from collections.abc import Mapping, Sequence
from decimal import Decimal

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from config import (
    COUNTRY_DIM_TABLE,
    FLOW_DIM_TABLE,
    PERIOD_DIM_TABLE,
    PRODUCT_DIM_TABLE,
    STAT_PROCEDURE_DIM_TABLE,
)
from silver_checks import Expectation, _by_file

# The checks, in the order of docs/engineering-notes.md. The number keeps them
# in that order wherever their results are sorted by name.
CHECKS = (
    "01_rows_match_silver",
    "03_one_match_per_dimension",
    "04_value_matches_silver",
    "05_dimension_key_unique",
    "06_no_overlapping_versions",
    "07_detail_matches_totals",
    "08_version_covers_month",
)

# The checks of a fact that read every month of it, not only the files of the
# run: a MERGE of this run can move a version that older months point to.
EVERY_MONTH_CHECKS = frozenset({"08_version_covers_month"})

# Expected failures
# -----------------
# By the rule of the silver checks: every month expects what EXPECTED names,
# and 0 for any other check. A check of a dimension writes its result without
# a file, and is expected at 0.
EXPECTED: dict[str, Expectation] = {
    # France's intra-EU exports and imports, in each of the six months of
    # 202601 to 202606, 24,144 to 26,751 and 12,381 to 15,452 EUR above the
    # totals, which no documented cause explains.
    "07_detail_matches_totals": 2,
}

# Check 07: the detail of a group within this many million EUR of its totals
# matches them.
TOTALS_TOLERANCE_MEUR = Decimal("0.01")


# Shared steps
# ------------


def _whole(dim: DataFrame, failing: Column) -> DataFrame:
    """Count the rows of a dimension, and those for which failing holds, without a file.

    An empty table returns no row: its check is not evaluated rather than passed.
    """
    return _by_file(dim.withColumn("_source_file", F.lit(None).cast("string")), failing)


# The checks of a fact
# --------------------


def rows_match_silver(fact: DataFrame, silver: DataFrame) -> DataFrame:
    """01: per file, as many fact rows as silver rows. Counts the rows apart."""
    silver_rows = silver.groupBy("_source_file").agg(F.count("*").alias("silver_rows"))
    fact_rows = fact.groupBy("_source_file").agg(F.count("*").alias("fact_rows"))
    compared = silver_rows.join(fact_rows, "_source_file", "full_outer").fillna(
        0, subset=["silver_rows", "fact_rows"]
    )
    return compared.select(
        "_source_file",
        F.col("silver_rows").alias("rows_checked"),
        F.abs(F.col("fact_rows") - F.col("silver_rows")).alias("rows_failed"),
    )


def one_match_per_dimension(
    fact: DataFrame, dimensions: Mapping[str, tuple[DataFrame, str]]
) -> DataFrame:
    """03: each fact row finds exactly one row in each of its dimensions.

    dimensions maps each column of the fact to the dimension it points to and
    the column of the dimension it matches. A row that finds none is lost to
    an inner join, and one that finds two is counted twice: the star keeps the
    rows of the fact only if every row finds one. A key the fact could not
    resolve to a version is NULL, and finds none.
    """
    flagged = fact
    off = []
    for column, (dim, dim_column) in dimensions.items():
        matches = f"{column}_matches"
        counts = dim.groupBy(F.col(dim_column).alias(column)).agg(
            F.count("*").alias(matches)
        )
        flagged = flagged.join(F.broadcast(counts), column, "left")
        off.append(F.coalesce(F.col(matches), F.lit(0)) != 1)
    return _by_file(flagged, functools.reduce(operator.or_, off))


def value_matches_silver(fact: DataFrame, silver: DataFrame) -> DataFrame:
    """04: per file, the value_eur of the fact adds up to silver's. Counts the one sum."""

    def total(rows: DataFrame, name: str) -> DataFrame:
        return rows.groupBy("_source_file").agg(F.sum("value_eur").alias(name))

    compared = total(silver, "silver_eur").join(
        total(fact, "fact_eur"), "_source_file", "full_outer"
    )
    return compared.select(
        "_source_file",
        F.lit(1).alias("rows_checked"),
        F.when(F.col("fact_eur").eqNullSafe(F.col("silver_eur")), 0)
        .otherwise(1)
        .alias("rows_failed"),
    )


def detail_matches_totals(detail: DataFrame, totals: DataFrame) -> DataFrame:
    """07: per reporter, trade type, flow and procedure, the detail adds up to the totals.

    Summed over every partner and product, within the tolerance. Counts
    groups; a group on one side only counts in full.
    """
    group = [
        "_source_file",
        "reporter_key",
        "trade_type",
        "flow",
        "stat_procedure",
        "period",
    ]
    detail_sums, totals_sums = (
        rows.groupBy(*group).agg(F.sum("value_eur").alias(name))
        for rows, name in ((detail, "detail_eur"), (totals, "totals_eur"))
    )
    gaps = detail_sums.join(totals_sums, group, "full_outer")
    gap = F.coalesce("detail_eur", F.lit(0)) - F.coalesce("totals_eur", F.lit(0))
    return _by_file(gaps, F.abs(gap) / 1_000_000 > TOTALS_TOLERANCE_MEUR)


def version_covers_month(
    fact: DataFrame,
    dim_period: DataFrame,
    versions: Mapping[str, tuple[DataFrame, str]],
) -> DataFrame:
    """08: the version each key of a fact row points to covers the row's month.

    versions maps each key column of the fact to the dimension it points to and
    the dimension's key column. A row counts when one of its versions starts
    after its period_date or ends before it, which a MERGE after the row was
    built can bring about. A key without a version counts too, as in check 03.
    """
    dated = fact.join(
        F.broadcast(dim_period.select("period", "period_date")), "period", "left"
    )
    covered = []
    for column, (dim, dim_key) in versions.items():
        spans = dim.select(
            F.col(dim_key).alias(column),
            F.col("valid_from").alias(f"{column}_from"),
            F.col("valid_to").alias(f"{column}_to"),
        )
        dated = dated.join(F.broadcast(spans), column, "left")
        covered.append(
            F.col("period_date").between(F.col(f"{column}_from"), F.col(f"{column}_to"))
        )
    every = functools.reduce(operator.and_, covered)
    return _by_file(dated, ~F.coalesce(every, F.lit(False)))


# The checks of a dimension
# -------------------------


def dimension_key_unique(dim: DataFrame, *keys: Sequence[str]) -> DataFrame:
    """05: no two rows share a key. Counts the rows of the repeated ones.

    Each key is a set of columns: the surrogate key, and the code with
    valid_from, of a versioned dimension; the code alone of the others.
    """
    repeated = {
        f"repeated_{i}": F.count("*").over(Window.partitionBy(*key)) > 1
        for i, key in enumerate(keys)
    }
    flagged = dim.withColumns(repeated)
    return _whole(
        flagged, functools.reduce(operator.or_, [F.col(name) for name in repeated])
    )


def no_overlapping_versions(dim: DataFrame, code_column: str) -> DataFrame:
    """06: no version of a code starts before the one before it ends.

    Sorted by valid_from, any two versions of a code that overlap make two
    consecutive ones overlap, so each version is compared with the next.
    Counts the versions that overlap the next.
    """
    by_code = Window.partitionBy(code_column).orderBy("valid_from")
    flagged = dim.withColumn("next_from", F.lead("valid_from").over(by_code))
    return _whole(flagged, F.col("next_from") <= F.col("valid_to"))


def all_checks(
    *,
    detail: DataFrame,
    totals: DataFrame,
    every_detail: DataFrame,
    every_totals: DataFrame,
    silver_detail: DataFrame,
    silver_totals: DataFrame,
    dim_country: DataFrame,
    dim_product: DataFrame,
    dim_flow: DataFrame,
    dim_stat_procedure: DataFrame,
    dim_period: DataFrame,
) -> list[tuple[str, str, DataFrame]]:
    """Every check, with the table it reads: a fact, detail or totals, or a dimension.

    detail and totals are the facts of the files of the run; every_detail and
    every_totals, every month of them, which the EVERY_MONTH_CHECKS read;
    silver_detail and silver_totals, the silver tables the facts were built from.
    """
    countries = {
        "reporter_key": (dim_country, "country_key"),
        "partner_key": (dim_country, "country_key"),
    }
    codes = {
        "flow": (dim_flow, "flow"),
        "stat_procedure": (dim_stat_procedure, "stat_procedure"),
        "period": (dim_period, "period"),
    }
    detail_dimensions = {
        **countries,
        "product_key": (dim_product, "product_key"),
        **codes,
    }
    return [
        ("01_rows_match_silver", "detail", rows_match_silver(detail, silver_detail)),
        ("01_rows_match_silver", "totals", rows_match_silver(totals, silver_totals)),
        (
            "03_one_match_per_dimension",
            "detail",
            one_match_per_dimension(detail, detail_dimensions),
        ),
        (
            "03_one_match_per_dimension",
            "totals",
            one_match_per_dimension(totals, {**countries, **codes}),
        ),
        (
            "04_value_matches_silver",
            "detail",
            value_matches_silver(detail, silver_detail),
        ),
        (
            "04_value_matches_silver",
            "totals",
            value_matches_silver(totals, silver_totals),
        ),
        (
            "05_dimension_key_unique",
            COUNTRY_DIM_TABLE,
            dimension_key_unique(
                dim_country, ("country_key",), ("country_code", "valid_from")
            ),
        ),
        (
            "05_dimension_key_unique",
            PRODUCT_DIM_TABLE,
            dimension_key_unique(
                dim_product, ("product_key",), ("product_nc", "valid_from")
            ),
        ),
        (
            "05_dimension_key_unique",
            FLOW_DIM_TABLE,
            dimension_key_unique(dim_flow, ("flow",)),
        ),
        (
            "05_dimension_key_unique",
            STAT_PROCEDURE_DIM_TABLE,
            dimension_key_unique(dim_stat_procedure, ("stat_procedure",)),
        ),
        (
            "05_dimension_key_unique",
            PERIOD_DIM_TABLE,
            dimension_key_unique(dim_period, ("period",)),
        ),
        (
            "06_no_overlapping_versions",
            COUNTRY_DIM_TABLE,
            no_overlapping_versions(dim_country, "country_code"),
        ),
        (
            "06_no_overlapping_versions",
            PRODUCT_DIM_TABLE,
            no_overlapping_versions(dim_product, "product_nc"),
        ),
        ("07_detail_matches_totals", "detail", detail_matches_totals(detail, totals)),
        (
            "08_version_covers_month",
            "detail",
            version_covers_month(
                every_detail,
                dim_period,
                {**countries, "product_key": (dim_product, "product_key")},
            ),
        ),
        (
            "08_version_covers_month",
            "totals",
            version_covers_month(every_totals, dim_period, countries),
        ),
    ]


# Comparison
# ----------


def expected_failures(check_name: str) -> Expectation:
    """What a gold check is expected to find: a count, or None if not evaluated."""
    return EXPECTED.get(check_name, 0)
