"""Shared configuration for the landing code and the Databricks notebooks.

Constants only: the source URLs and the access policy of the landing, the
Unity Catalog names every layer writes to, and the expected header of a monthly
file. The months of a run are not here: the Job passes them to each notebook
as the parameters start_period and end_period, and months_in_window() turns
them into the months a listing or a manifest holds.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path

# Paths are derived from this file's location rather than the working directory,
# so the code behaves the same wherever a notebook imports it from.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Seeds written by hand and versioned with the code, such as the euro area
# members the silver checks read.
REFERENCE_DIR = PROJECT_ROOT / "reference"

# Source
# ------
# Eurostat COMEXT detailed trade data, published through the bulk download
# facility rather than the SDMX API, which does not expose this dataset. The
# slash before ?file= is part of the canonical URL: without it the server
# answers 302 and redirects here.
EUROSTAT_FILE_URL_TEMPLATE = (
    "https://ec.europa.eu/eurostat/api/dissemination/files/"
    "?file=comext%2FCOMEXT_DATA%2FPRODUCTS%2F{file_name}"
)

# The listing of the same folder: an HTML page with one row per file, giving its
# name, its size in bytes and the time it was published. A republished month
# keeps its name and gets a new time, so the listing is what tells it apart.
EUROSTAT_LISTING_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/files/"
    "?dir=comext%2FCOMEXT_DATA%2FPRODUCTS"
)

# One archive per month, tens of MB compressed. The folder also holds a yearly
# archive per year, numbered as month 52, and a subset of each month with the
# United Kingdom's partners, under another prefix; the pipeline reads neither.
FULL_FILE_PREFIX = "full_v2_"
_MONTHLY_ARCHIVE = re.compile(rf"{FULL_FILE_PREFIX}(\d{{6}})\.7z")

# Code lists, published next to the data, resolve the codes each row carries:
# partner names, product codes, geographic zones and the partners in each,
# trade types, statistical procedures. Tab separated, without a header row.
# Only the lists the pipeline reads are listed, as paths below
# COMEXT_METADATA/CLASSIFICATIONS_AND_RELATIONS/. They land in a subfolder of
# the Volume, under their own names.
CODE_LIST_URL_TEMPLATE = (
    "https://ec.europa.eu/eurostat/api/dissemination/files/"
    "?file=comext%2FCOMEXT_METADATA%2FCLASSIFICATIONS_AND_RELATIONS%2F{path}"
)
CODE_LISTS = (
    "CLASSIFICATIONS/ENGLISH/PARTNERS_ISO.txt",
    "CLASSIFICATIONS/ENGLISH/GEOZONES.txt",
    "CLASSIFICATIONS/ENGLISH/TRADE_TYPE.txt",
    "CLASSIFICATIONS/ENGLISH/CN.txt",
    "CLASSIFICATIONS/ENGLISH/STATISTICAL_PROCEDURES.txt",
    "CLASSIFICATIONS/ENGLISH/FLOW.txt",
    "CLASSIFICATIONS/ENGLISH/CN_SECTIONS.txt",
    "RELATIONS/GEOZONES-PARTNERS_ISO.txt",
    "RELATIONS/CN_SECTIONS-CHAPTERS.txt",
)
CODE_LISTS_SUBDIR = "code_lists"

# Published figures
# -----------------
# Eurostat's monthly EU27 totals, in million EUR, compiled from the same data:
# trade with the EU27, outside it, and with the United Kingdom, by flow. The
# silver checks reconcile the TOTAL rows against them. The table is revised
# together with the files, so the figures are saved when an archive is landed,
# one copy per landed file in this subfolder, and never read live. The time
# is YYYY-MM.
PUBLISHED_TABLE_URL_TEMPLATE = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    "ext_st_eu27_2020sitc?format=JSON&lang=EN&sitc06=TOTAL&indic_et=TRD_VAL"
    "&time={month}&partner=EU27_2020&partner=EXT_EU27_2020&partner=UK"
    "&stk_flow=IMP&stk_flow=EXP"
)
PUBLISHED_SUBDIR = "published"

# Manifest
# --------
# One row per landed archive, in the Volume: its size and the time the listing
# gave it when it was landed. Against the listing, it tells a month republished
# since from one that was not; against the watermark, a month the pipeline has
# not processed yet.
MANIFEST_NAME = "manifest.csv"

# Unity Catalog
# -------------
# Where the landed files go, and where each layer writes. The Volume holds the
# CSV at its root, the code lists in CODE_LISTS_SUBDIR and the copies of the
# published figures in PUBLISHED_SUBDIR.
CATALOG = "eu_trade"
LANDING_SCHEMA = "landing"
LANDING_VOLUME = "comext"
BRONZE_SCHEMA = "bronze"
BRONZE_TRADE_TABLE = "comext_trade"
# Silver: the rows by product, and the TOTAL rows, in tables of their own.
SILVER_SCHEMA = "silver"
SILVER_DETAIL_TABLE = "trade_detail"
SILVER_TOTALS_TABLE = "trade_totals"
# Gold: one fact per silver table, and the dimensions they point to.
GOLD_SCHEMA = "gold"
DETAIL_FACT_TABLE = "fct_trade_detail"
TOTALS_FACT_TABLE = "fct_trade_totals"
COUNTRY_DIM_TABLE = "dim_country"
PRODUCT_DIM_TABLE = "dim_product"
FLOW_DIM_TABLE = "dim_flow"
STAT_PROCEDURE_DIM_TABLE = "dim_stat_procedure"
PERIOD_DIM_TABLE = "dim_period"
# The results of the data quality checks, in a schema apart from the layers,
# and the watermark: the publication of each file the pipeline processed.
OPS_SCHEMA = "ops"
DQ_RESULTS_TABLE = "dq_check_results"
WATERMARK_TABLE = "etl_watermark"

VOLUME_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}"

# Every monthly file carries this header, in this order. The extracted member is
# named .dat but is a comma separated file with a header row.
EXPECTED_HEADER = (
    "REPORTER",
    "PARTNER",
    "TRADE_TYPE",
    "PRODUCT_NC",
    "PRODUCT_SITC",
    "PRODUCT_CPA21",
    "PRODUCT_CPA22",
    "PRODUCT_BEC",
    "PRODUCT_BEC5",
    "PRODUCT_SECTION",
    "FLOW",
    "STAT_PROCEDURE",
    "SUPPL_UNIT",
    "PERIOD",
    "VALUE_EUR",
    "VALUE_NAC",
    "QUANTITY_KG",
    "QUANTITY_SUPPL_UNIT",
)

# Access policy
# -------------
# Eurostat publishes no request rate for the bulk download facility. One request
# per second is a conservative choice rather than a measured limit, and costs
# nothing at a handful of monthly files.
MIN_REQUEST_INTERVAL_SECONDS = 1.0

DOWNLOAD_CHUNK_BYTES = 1024 * 1024
REQUEST_TIMEOUT_SECONDS = 60

# The largest member an archive may unpack to. The largest full month landed is
# 403 MB; the ceiling only stops a corrupt or hostile archive from filling the
# disk.
MAX_EXTRACTED_BYTES = 2 * 1024**3


def qualified(*names: str) -> str:
    """Name a schema or a table in the catalog: qualified("bronze") is eu_trade.bronze."""
    return ".".join((CATALOG, *names))


def archive_name(period: str) -> str:
    """Return the published file name of one monthly archive."""
    return f"{FULL_FILE_PREFIX}{period}.7z"


def csv_name(period: str) -> str:
    """The CSV of a month, as the Volume, bronze and the watermark name it."""
    return f"{FULL_FILE_PREFIX}{period}.csv"


def period_from(value: str, name: str) -> str:
    """Read a month as YYYYMM, from 01 to 12. name says which setting it came from."""
    period = value.strip()
    if not re.fullmatch(r"\d{4}(0[1-9]|1[0-2])", period):
        raise ValueError(f"{name}={value!r}: expected a month as YYYYMM")
    return period


def window_from(start_value: str, end_value: str) -> tuple[str, str | None]:
    """Read the parameters start_period and end_period of a run.

    An empty end_period leaves the window open: it reaches the last month
    there is.
    """
    start = period_from(start_value, "start_period")
    end = period_from(end_value, "end_period") if end_value.strip() else None
    return start, end


def months_in_window(
    archives: Iterable[str], start: str, end: str | None
) -> tuple[str, ...]:
    """The months of the monthly archives named in archives, from start to end.

    archives are file names, as the listing or a manifest holds them; every
    other name is ignored, the yearly archives and the subsets of each month included.
    Without an end, the window reaches the last month there is. In order, from
    the oldest.
    """
    if end is not None and end < start:
        raise ValueError(f"The window starts at {start} and ends before, at {end}")
    months = set()
    for name in archives:
        match = _MONTHLY_ARCHIVE.fullmatch(name)
        if match and not match.group(1).endswith("52"):
            months.add(match.group(1))
    return tuple(sorted(m for m in months if m >= start and (end is None or m <= end)))
