"""How the gold layer builds the dimensional model over silver.

Two facts, one per silver table, point to five dimensions. dim_country and
dim_product hold one row per version of a code, and each fact row takes the
version that covers its period_date, through a surrogate key. The rules, and
the measurements behind each one, are in docs/engineering-notes.md, section
Gold.

Two things to keep in mind when changing this module:

- A version is a span between the dates of any source of the dimension, kept
  only inside a row of the dimension's own list, and it takes the attributes
  valid on its first day. A code has several versions, so a fact row joined to
  a dimension by its code alone, without the date, is multiplied.
- The versioned dimensions load with a MERGE on (code, valid_from). A key that
  repeats in the source stops Delta's MERGE if the table holds it, and is
  inserted twice, without an error, if it does not. merge_dimension() refuses
  such a source before touching the table.
"""

from __future__ import annotations

import functools
import operator
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from silver import _hs_level
from silver_checks import EU_ZONE

# The valid_to of a row still valid.
OPEN_END = date(2500, 12, 31)

# The continent zones of GEOZONES-PARTNERS_ISO.txt. 5900 holds the codes of
# no continent.
CONTINENT_ZONES = ("5100", "5200", "5300", "5400", "5500", "5900")
NOT_SPECIFIED_ZONE = "5900"

# Listed both in Europe and in 5900: they are not countries, and take 5900.
INTRA_EU_Q_CODES = ("QR", "QV", "QY")

# As silver's is_x_code.
X_CODE = r"^\d+X+$"

# The parallel classifications stay on the fact row, as codes: they describe
# the row, not the product.
CLASSIFICATIONS = (
    "product_sitc",
    "product_cpa21",
    "product_cpa22",
    "product_bec",
    "product_bec5",
)

DETAIL_FACT_COLUMNS = (
    "reporter_key",
    "partner_key",
    "product_key",
    *CLASSIFICATIONS,
    "flow",
    "stat_procedure",
    "period",
    "trade_type",
    "product_section",
    "suppl_unit",
    "value_eur",
    "value_nac",
    "quantity_kg",
    "quantity_suppl_unit",
    "_source_file",
)
TOTALS_FACT_COLUMNS = (
    "reporter_key",
    "partner_key",
    "flow",
    "stat_procedure",
    "period",
    "trade_type",
    "value_eur",
    "value_nac",
    "quantity_kg",
    "_source_file",
)

# The levels of the HS hierarchy a product reaches, and their CN.txt rows.
HS_LEVELS = ("hs6", "hs4", "hs2")


def read_special_codes(spark: SparkSession, path: str) -> DataFrame:
    """The partner codes of the seed that are not a single country, with their category."""
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(
            "partner_code string, partner_name string, category string, "
            "counts_in_totals boolean, reason string"
        )
        .csv(path)
        .select(F.col("partner_code").alias("code"), "category")
    )


# Versions
# --------


def _between_rows(rows: DataFrame) -> DataFrame:
    """rows with valid_from and valid_to renamed, to be joined on a date between them."""
    return rows.withColumnsRenamed({"valid_from": "row_from", "valid_to": "row_to"})


def _covers(day: str) -> Column:
    """Whether the row of _between_rows() covers the column day."""
    return F.col(day).between(F.col("row_from"), F.col("row_to"))


def _starts_of(source: DataFrame) -> DataFrame:
    """The days on which a row of source starts, and the days after one ends, per code."""
    return source.select("code", F.col("valid_from").alias("start")).unionByName(
        source.select("code", F.date_add("valid_to", 1).alias("start"))
    )


def versions(base: DataFrame, *others: DataFrame) -> DataFrame:
    """One version per span between consecutive starts of any source, inside a row of base.

    Every source holds code, valid_from and valid_to. A version runs from one
    start to the day before the next, and keeps the columns of the row of base
    that holds it. The other sources only add dates.
    """
    starts = functools.reduce(
        DataFrame.unionByName, [_starts_of(source) for source in (base, *others)]
    )
    by_code = Window.partitionBy("code").orderBy("start")
    spans = (
        starts.distinct()
        .withColumn("next_start", F.lead("start").over(by_code))
        .filter(F.col("next_start").isNotNull())
        .select(
            "code",
            F.col("start").alias("valid_from"),
            F.date_sub("next_start", 1).alias("valid_to"),
        )
    )
    return (
        spans.join(_between_rows(base), "code")
        .filter(_covers("valid_from"))
        .drop("row_from", "row_to")
    )


def _holds_on_start(spans: DataFrame, rows: DataFrame, flag: str) -> DataFrame:
    """Add flag: whether a row of rows covers the version's valid_from."""
    hits = (
        spans.select("code", "valid_from")
        .join(_between_rows(rows), "code")
        .filter(_covers("valid_from"))
        .select("code", "valid_from")
        .distinct()
        .withColumn(flag, F.lit(True))
    )
    return spans.join(hits, ["code", "valid_from"], "left").fillna(False, subset=[flag])


def _is_current() -> Column:
    return (F.col("valid_to") == F.lit(OPEN_END)).alias("is_current")


# Dimensions
# ----------


def build_dim_country(
    partners_iso: DataFrame,
    relation: DataFrame,
    zone_names: DataFrame,
    euro_area: DataFrame,
    special: DataFrame,
) -> DataFrame:
    """One row per version of a partner code.

    The versions come from PARTNERS_ISO.txt, the code's own rows and name;
    zone 1010 of GEOZONES-PARTNERS_ISO.txt, is_intra_eu; its continent zones,
    region; and the euro area seed, is_euro_area. The seed of special codes,
    which has no dates, gives is_country and partner_category.
    """
    eu = relation.filter(F.col("zone") == EU_ZONE).select(
        "code", "valid_from", "valid_to"
    )
    continent = relation.filter(F.col("zone").isin(*CONTINENT_ZONES))
    euro = euro_area.select(
        F.col("country_code").alias("code"),
        F.col("euro_from").alias("valid_from"),
        F.lit(OPEN_END).alias("valid_to"),
    )

    spans = versions(partners_iso, eu, continent.drop("zone"), euro)
    spans = _holds_on_start(spans, eu, "is_intra_eu")
    spans = _holds_on_start(spans, euro, "is_euro_area")
    continents_on_start = (
        spans.select("code", "valid_from")
        .join(_between_rows(continent), "code")
        .filter(_covers("valid_from"))
        .groupBy("code", "valid_from")
        .agg(F.array_sort(F.collect_set("zone")).alias("continents"))
    )
    region_zone = F.when(F.size("continents") == 1, F.col("continents")[0]).when(
        F.col("code").isin(*INTRA_EU_Q_CODES)
        & F.array_contains("continents", NOT_SPECIFIED_ZONE),
        F.lit(NOT_SPECIFIED_ZONE),
    )
    continent_names = zone_names.filter(F.col("zone").isin(*CONTINENT_ZONES)).select(
        F.col("zone").alias("region_zone"), F.col("zone_name").alias("region")
    )
    return (
        spans.join(continents_on_start, ["code", "valid_from"], "left")
        .withColumn("region_zone", region_zone)
        .join(continent_names, "region_zone", "left")
        .join(special, "code", "left")
        .select(
            F.xxhash64("code", "valid_from").alias("country_key"),
            F.col("code").alias("country_code"),
            F.col("name").alias("country_name"),
            F.col("category").isNull().alias("is_country"),
            F.coalesce("category", F.lit("country")).alias("partner_category"),
            "is_intra_eu",
            "is_euro_area",
            "region_zone",
            "region",
            "valid_from",
            "valid_to",
            _is_current(),
        )
    )


def build_dim_product(
    cn: DataFrame, sections: DataFrame, chapters: DataFrame
) -> DataFrame:
    """One row per version of an eight-character code of CN.txt, X codes included.

    The hierarchy is silver's: an X code stops at its own level. The rows of
    the code's HS6, HS4 and chapter only add dates, and give their descriptions
    on the version's valid_from. The section is the chapter's.
    """
    # _hs_level() reads PRODUCT_NC, the bronze name of the code.
    named = cn.filter(F.length("code") == 8).select(
        F.col("code").alias("PRODUCT_NC"), "valid_from", "valid_to", "description"
    )
    products = named.select(
        F.col("PRODUCT_NC").alias("code"),
        "valid_from",
        "valid_to",
        "description",
        F.col("PRODUCT_NC").rlike(X_CODE).alias("is_x_code"),
        *(_hs_level(int(level[2:])).alias(level) for level in HS_LEVELS),
    )

    def parent_rows(level: str) -> DataFrame:
        """The rows of CN.txt of each product's code at level, under the product's own code."""
        parents = cn.select(
            F.col("code").alias(level),
            F.col("valid_from").alias("row_from"),
            F.col("valid_to").alias("row_to"),
            F.col("description").alias(f"{level}_description"),
        )
        return products.select("code", level).distinct().join(parents, level)

    spans = versions(
        products,
        *(
            parent_rows(level).select(
                "code",
                F.col("row_from").alias("valid_from"),
                F.col("row_to").alias("valid_to"),
            )
            for level in HS_LEVELS
        ),
    )
    dim = spans
    for level in HS_LEVELS:
        on_start = (
            spans.select("code", "valid_from")
            .join(parent_rows(level).drop(level), "code")
            .filter(_covers("valid_from"))
            .select("code", "valid_from", f"{level}_description")
        )
        dim = dim.join(on_start, ["code", "valid_from"], "left")
    return (
        dim.join(chapters, "hs2", "left")
        .join(sections, "section_code", "left")
        .select(
            F.xxhash64("code", "valid_from").alias("product_key"),
            F.col("code").alias("product_nc"),
            "description",
            "is_x_code",
            "hs6",
            "hs6_description",
            "hs4",
            "hs4_description",
            "hs2",
            "hs2_description",
            "section_code",
            "section_description",
            "valid_from",
            "valid_to",
            _is_current(),
        )
    )


def build_dim_period(*rows: DataFrame) -> DataFrame:
    """One row per month of the silver rows given."""
    months = functools.reduce(
        DataFrame.unionByName, [df.select("period", "period_date") for df in rows]
    ).distinct()
    return months.select(
        "period",
        "period_date",
        F.year("period_date").alias("year"),
        F.quarter("period_date").alias("quarter"),
        F.month("period_date").alias("month"),
    )


def merge_dimension(
    target: DeltaTable, source: DataFrame, code_column: str
) -> dict[str, int]:
    """Merge a versioned dimension into its table on (code, valid_from).

    A version whose end or attributes changed is updated, and a new one is
    inserted. A version of the table missing from the source is counted, not
    deleted. Returns the updated, inserted and missing versions.

    Raises ValueError, before the table is touched, if (code, valid_from)
    repeats in the source.
    """
    on = [code_column, "valid_from"]
    repeated = source.groupBy(*on).count().filter(F.col("count") > 1).count()
    if repeated:
        raise ValueError(
            f"{repeated} ({code_column}, valid_from) pairs repeat in the source: "
            "merged, a new one would be inserted twice"
        )
    missing = target.toDF().join(source, on, "left_anti").count()
    compared = [name for name in source.columns if name not in on]
    changed = functools.reduce(
        operator.or_,
        [~F.col(f"t.{name}").eqNullSafe(F.col(f"s.{name}")) for name in compared],
    )
    same_key = functools.reduce(
        operator.and_, [F.col(f"t.{name}") == F.col(f"s.{name}") for name in on]
    )
    metrics = (
        target.alias("t")
        .merge(source.alias("s"), same_key)
        .whenMatchedUpdateAll(condition=changed)
        .whenNotMatchedInsertAll()
        .execute()
        .collect()[0]
    )
    return {
        "updated": metrics["num_updated_rows"],
        "inserted": metrics["num_inserted_rows"],
        "missing": missing,
    }


# Facts
# -----


def _with_key(
    rows: DataFrame,
    dim: DataFrame,
    *,
    row_column: str,
    code_column: str,
    key_column: str,
    key_name: str,
) -> DataFrame:
    """rows with key_name: the key of the version of row_column that covers period_date.

    The key is looked up once per pair of a code and a period_date, and NULL
    where no version covers it. A pair that two versions covered would be
    joined twice, so the fact is counted against silver after it is built.
    """
    pairs = rows.select(F.col(row_column).alias("code"), "period_date").distinct()
    candidates = dim.select(
        F.col(code_column).alias("dim_code"),
        F.col("valid_from").alias("row_from"),
        F.col("valid_to").alias("row_to"),
        F.col(key_column).alias(key_name),
    )
    lookup = pairs.join(
        candidates, (F.col("code") == F.col("dim_code")) & _covers("period_date")
    ).select(F.col("code").alias(row_column), "period_date", key_name)
    return rows.join(F.broadcast(lookup), [row_column, "period_date"], "left")


def _with_country_keys(rows: DataFrame, dim_country: DataFrame) -> DataFrame:
    for role in ("reporter", "partner"):
        rows = _with_key(
            rows,
            dim_country,
            row_column=role,
            code_column="country_code",
            key_column="country_key",
            key_name=f"{role}_key",
        )
    return rows


def build_fct_trade_detail(
    detail: DataFrame, dim_country: DataFrame, dim_product: DataFrame
) -> DataFrame:
    """The rows by product of silver, with the keys of their country and product versions."""
    keyed = _with_key(
        _with_country_keys(detail, dim_country),
        dim_product,
        row_column="product_nc",
        code_column="product_nc",
        key_column="product_key",
        key_name="product_key",
    )
    return keyed.select(*DETAIL_FACT_COLUMNS)


def build_fct_trade_totals(totals: DataFrame, dim_country: DataFrame) -> DataFrame:
    """The TOTAL rows of silver, with the keys of their country versions."""
    return _with_country_keys(totals, dim_country).select(*TOTALS_FACT_COLUMNS)
