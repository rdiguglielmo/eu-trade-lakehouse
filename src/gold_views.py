"""The views that answer the business questions, created from their SQL.

sql/gold_views.sql holds one view per question over the gold star schema, and
sql/analysis_queries.sql the queries that state each answer from them. The
rules, and what each view returned on the landed files, are in
docs/engineering-notes.md, section Gold.

The SQL names the facts and the dimensions without a schema. The notebook sets
the gold schema before it creates the views, and a view resolves its tables in
the schema it was created in. The tests register their facts and dimensions as
temporary views of the same names, and create the views as temporary views
over them.
"""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import SparkSession

from config import PROJECT_ROOT

SQL_DIR = PROJECT_ROOT / "sql"
VIEWS_FILE = SQL_DIR / "gold_views.sql"
QUESTIONS_FILE = SQL_DIR / "analysis_queries.sql"

CREATE_VIEW = "CREATE OR REPLACE VIEW "
CREATE_TEMPORARY_VIEW = "CREATE OR REPLACE TEMPORARY VIEW "

# The fact rows a view reads, as a query of their period and value_eur. The
# rule of each question is written here again, apart from its view, so that a
# view that loses, adds or splits rows does not match it. Questions 1 to 3
# read every row of their fact.
TOTALS_ROWS = "SELECT period, value_eur FROM fct_trade_totals"
DETAIL_ROWS = "SELECT period, value_eur FROM fct_trade_detail"
# Question 4 reads the rows of a product with a heading: an X code of a
# chapter has none.
HEADING_ROWS = """
SELECT f.period, f.value_eur
FROM fct_trade_detail AS f
JOIN dim_product AS pr ON f.product_key = pr.product_key
WHERE pr.hs4 IS NOT NULL
"""
# Question 5 reads the rows of an eight-digit code with a partner that is a
# country, a value and a weight.
VALUE_PER_KG_ROWS = """
SELECT f.period, f.value_eur
FROM fct_trade_detail AS f
JOIN dim_product AS pr ON f.product_key = pr.product_key
JOIN dim_country AS p ON f.partner_key = p.country_key
WHERE NOT pr.is_x_code AND p.is_country AND f.value_eur > 0 AND f.quantity_kg > 0
"""

# Each view: its grain, the fact rows it reads, and the columns that add up to
# their value_eur. Every view is unique on its grain and keeps the value of the
# rows it reads in every month; the notebook stops otherwise.
VIEWS = {
    "vw_partner_ranking": (
        ("period", "reporter_code", "flow", "partner_code"),
        TOTALS_ROWS,
        ("value_eur",),
    ),
    "vw_trade_balance_by_chapter": (
        ("period", "reporter_code", "chapter"),
        DETAIL_ROWS,
        ("exports_eur", "imports_eur"),
    ),
    "vw_intra_extra_by_country": (
        ("period", "reporter_code", "flow"),
        TOTALS_ROWS,
        ("intra_eu_eur", "extra_eu_eur"),
    ),
    "vw_intra_extra_by_chapter": (
        ("period", "reporter_code", "chapter", "flow"),
        DETAIL_ROWS,
        ("intra_eu_eur", "extra_eu_eur"),
    ),
    "vw_partner_dependence": (
        ("period", "reporter_code", "heading", "flow"),
        HEADING_ROWS,
        ("value_eur",),
    ),
    "vw_value_per_kg_by_partner": (
        ("period", "product_nc", "flow", "partner_code"),
        VALUE_PER_KG_ROWS,
        ("value_eur",),
    ),
}


def _is_comment(line: str) -> bool:
    return line.lstrip().startswith("--")


def statements(path: Path) -> list[str]:
    """The statements of a SQL file, each with the comment lines above it.

    A statement ends at a line that is not a comment and ends with a
    semicolon, so a comment may hold one. Comment lines after the last
    statement are dropped; anything else there is a statement without its
    semicolon, and raises ValueError.
    """
    found: list[str] = []
    pending: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        pending.append(line)
        if not _is_comment(line) and line.rstrip().endswith(";"):
            found.append("\n".join(pending).strip().removesuffix(";"))
            pending = []
    if any(line.strip() and not _is_comment(line) for line in pending):
        raise ValueError(f"{path.name}: a statement without its semicolon at the end")
    return found


def view_name(statement: str) -> str:
    """The name of the view a statement creates."""
    found = re.search(rf"{CREATE_VIEW}(\w+) AS", statement)
    if found is None:
        raise ValueError(f"Not a {CREATE_VIEW.strip()} statement: {statement[:80]!r}")
    return found.group(1)


def create_views(spark: SparkSession, *, temporary: bool = False) -> list[str]:
    """Create every view of VIEWS_FILE in the current schema, and return their names.

    With temporary, each is created as a temporary view, which resolves the
    facts and the dimensions among the session's temporary views.
    """
    names = []
    for statement in statements(VIEWS_FILE):
        names.append(view_name(statement))
        spark.sql(
            statement.replace(CREATE_VIEW, CREATE_TEMPORARY_VIEW, 1)
            if temporary
            else statement
        )
    return names
