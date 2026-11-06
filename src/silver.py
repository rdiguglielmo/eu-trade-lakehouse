"""How the silver layer turns the bronze rows into two typed tables.

trade_detail holds the rows by product, and trade_totals the TOTAL rows, which
sum the others and have a grain of their own. The rules, and the measurements
behind each one, are in docs/engineering-notes.md, section Silver.

Two things to keep in mind when changing this module:

- Spark resolves column names without regard to case, so a column value_eur
  added to a frame that holds VALUE_EUR replaces it. The typed columns are
  built in a single select from the bronze columns, never added next to them.
- Spark 4 casts in ANSI mode: text that is not a number fails the cast, and
  with it the run. A fourth decimal is rounded to the scale without a warning;
  the quality checks compare each value with a cast that loses nothing.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# PRODUCT_NC of the rows that sum the others.
TOTAL_PRODUCT = "TOTAL"

# The supplementary unit of a product that has none. Its quantity is always 0.
NO_SUPPLEMENTARY_UNIT = "NO_SU"

# At most 3 decimals and 13 integer digits are measured in the values, and
# whole numbers of at most 11 digits in the quantities.
VALUE_TYPE = "decimal(18,3)"
QUANTITY_TYPE = "bigint"

# The TOTAL rows read TOTAL in the five parallel classifications, TO in the
# section, NO_SU and a quantity of 0: their table leaves those columns out.
TOTALS_COLUMNS = (
    "reporter",
    "partner",
    "trade_type",
    "flow",
    "stat_procedure",
    "period",
    "period_date",
    "value_eur",
    "value_nac",
    "quantity_kg",
    "_source_file",
)


def _hs_level(digits: int) -> Column:
    """The first digits of PRODUCT_NC, or NULL where an X code stops above them.

    28XXXXXX reaches the chapter only, 2833XXXX the heading, and 283321XX the
    subheading.
    """
    prefix = F.substring("PRODUCT_NC", 1, digits)
    return F.when(prefix.rlike(rf"^\d{{{digits}}}$"), prefix)


def typed(bronze: DataFrame) -> DataFrame:
    """Type and name every bronze row as silver does, in one select.

    Codes stay text, flow and procedure included. NO_SU and its quantity
    become NULL; a quantity of 0 with a real unit, or in kilograms, stays 0.
    """
    no_unit = F.col("SUPPL_UNIT") == NO_SUPPLEMENTARY_UNIT
    return bronze.select(
        F.col("REPORTER").alias("reporter"),
        F.col("PARTNER").alias("partner"),
        F.col("TRADE_TYPE").alias("trade_type"),
        F.col("PRODUCT_NC").alias("product_nc"),
        _hs_level(2).alias("hs2"),
        _hs_level(4).alias("hs4"),
        _hs_level(6).alias("hs6"),
        F.col("PRODUCT_NC").rlike(r"^\d+X+$").alias("is_x_code"),
        F.col("PRODUCT_SITC").alias("product_sitc"),
        F.col("PRODUCT_CPA21").alias("product_cpa21"),
        F.col("PRODUCT_CPA22").alias("product_cpa22"),
        F.col("PRODUCT_BEC").alias("product_bec"),
        F.col("PRODUCT_BEC5").alias("product_bec5"),
        F.col("PRODUCT_SECTION").alias("product_section"),
        F.col("FLOW").alias("flow"),
        F.col("STAT_PROCEDURE").alias("stat_procedure"),
        F.col("PERIOD").alias("period"),
        # The first day of the month picks the valid row of each code list.
        F.to_date("PERIOD", "yyyyMM").alias("period_date"),
        F.when(~no_unit, F.col("SUPPL_UNIT")).alias("suppl_unit"),
        F.col("VALUE_EUR").cast(VALUE_TYPE).alias("value_eur"),
        F.col("VALUE_NAC").cast(VALUE_TYPE).alias("value_nac"),
        F.col("QUANTITY_KG").cast(QUANTITY_TYPE).alias("quantity_kg"),
        F.when(~no_unit, F.col("QUANTITY_SUPPL_UNIT").cast(QUANTITY_TYPE)).alias(
            "quantity_suppl_unit"
        ),
        F.col("_source_file"),
    )


def split_bronze(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return the detail and the totals of the bronze rows."""
    rows = typed(bronze)
    is_total = F.col("product_nc") == TOTAL_PRODUCT
    detail = rows.filter(~is_total)
    totals = rows.filter(is_total).select(*TOTALS_COLUMNS)
    return detail, totals
