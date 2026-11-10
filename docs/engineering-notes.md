# Engineering Notes

The rules in this repository that are easy to break and expensive to break.
[`README.md`](../README.md) says what the project is, and
[`development.md`](development.md) how it is installed, deployed and tested.

## Contents

- [The source](#the-source)
- [Partners and products](#partners-and-products)
- [Landing and the manifest](#landing-and-the-manifest)
- [Bronze](#bronze)
- [Silver](#silver)
- [Gold](#gold)
- [Dimensions with history (SCD type 2)](#dimensions-with-history-scd-type-2)
- [The business questions](#the-business-questions)
- [Quality checks](#quality-checks)
- [The incremental run](#the-incremental-run)
- [The Job](#the-job)

## The source

**One 7z archive per month, holding one CSV of 18 columns, which
Eurostat republishes in place: a file is told by its publication time, not by
its month.** July 2026 is 46,802,030 bytes and 5,050,155 rows, as published
on 2026-10-16.

| | |
|---|---|
| Access | Eurostat's bulk download facility, with no credentials and no published request rate: the landing holds itself to one request per second. The SDMX API does not serve this dataset, and its dataflow answers 404 |
| File | `full_v2_YYYYMM.7z`, 45 to 48 MB for the months of 2026, at `https://ec.europa.eu/eurostat/api/dissemination/files/?file=comext%2FCOMEXT_DATA%2FPRODUCTS%2F<file>`. Without the slash before `?` the server redirects |
| Listing | The same address with `?dir=comext%2FCOMEXT_DATA%2FPRODUCTS`: an HTML page with the name, size and publication time of each file. `parse_listing()` stops on a page where it finds none, since read as empty it would make every month look unpublished. The folder also holds yearly archives and per-month subsets, which the landing does not read |
| Archive | 7z, which `py7zr` opens and `zipfile` does not, holding exactly one member, `full_YYYYMM.dat`: comma separated with one header row, despite the extension, and the 18 columns of `EXPECTED_HEADER` in `src/config.py`. An archive that holds anything else stops the landing |
| Failures | 429, 500, 502, 503, 504, a dropped connection or a timeout: five attempts, backing off 1, 2, 4 and 8 seconds, with `Retry-After` honoured up to 30. Any other status is raised at once; a 404 means a wrong name or a month not published yet |

**Eurostat republishes a month in place, under the same name, with other
rows:** 202605 went from 4,988,013 rows on 2026-09-15 to 4,994,361 on
2026-10-16. The landing compares each month's size and publication time in the
listing with the Volume's manifest, and lands a republished month again
(Landing and the manifest); the watermark makes it pending (The incremental
run).

**The newest month of the listing lacks most of its intra-EU trade**, which
Eurostat publishes with the next monthly release (manual, pp. 56-57): 202608,
as listed on 2026-10-20, held that of 9 of the 27 reporters. An open window
stops at the month before the last one listed (The incremental run).

**Every file carries its own totals as rows, `PRODUCT_NC =
TOTAL`, so a sum over the file counts every euro twice.**

One row per reporter, partner, flow and statistical procedure sums the other
rows of that combination: in July 2026, 12,783 `TOTAL` rows add up to
1,224,894.873 M EUR, and the other rows to 1,224,894.940. They read `TOTAL` in
every classification column but `PRODUCT_SECTION`, which reads `TO`, so a
filter on the section lets them through: the filter is `PRODUCT_NC = 'TOTAL'`,
and silver gives them a table of their own (Silver).

**The `TOTAL` rows reproduce the figures Eurostat publishes,
within their rounding of 0.05 M EUR, against the same publication
only.**

Eurostat's monthly table `ext_st_eu27_2020sitc` (`sitc06=TOTAL`,
`indic_et=TRD_VAL`, `geo=EU27_2020`, million EUR) is compiled from the same
data: `TRADE_TYPE` `I` is its partner `EU27_2020`, `E` is `EXT_EU27_2020`, and
GB is `UK`. July 2026, both from the publication of 2026-10-16:

| 2026-07, M EUR | `TOTAL` rows | Published | Difference |
|---|---:|---:|---:|
| Intra-EU imports | 358,993.494 | 358,993.5 | -0.006 |
| Intra-EU exports | 375,317.664 | 375,317.7 | -0.036 |
| Extra-EU imports | 243,299.599 | 243,299.6 | -0.001 |
| Extra-EU exports | 247,284.116 | 247,284.1 | +0.016 |
| Partner UK, imports | 14,409.093 | 14,409.1 | -0.007 |
| Partner UK, exports | 31,883.928 | 31,883.9 | +0.028 |

**The table is revised together with the files**: against the table of
2026-10-16, the file of 202605 published on 2026-09-15 misses every figure, by
19.8 to 248.5 M EUR. So check 10 compares each file with a copy of the figures
saved when its month lands (Landing and the manifest), never with the live
table.

<p align="right"><a href="#engineering-notes">Back to top</a></p>

## Partners and products

**Four properties of the codes change a ranking or a sum without
changing a row count.** In July 2026, 9,738.2 M EUR, 0.80% of the month,
changes partner with the rows read, and codes ending in X hold 2.93% of the
value of the rows by product.

- **Ten partner codes are not countries.** The Q codes, for high seas, stores
  and provisions, a partner not specified and a partner confidential, count in
  totals but not in a ranking of countries; the seed
  [`partner_special_codes.csv`](../reference/partner_special_codes.csv) names
  them. No partner code is a zone: the 245 of July 2026, 7 of them Q codes,
  are all valid in `PARTNERS_ISO.txt` on 2026-07-01.
- **The partner depends on the flow** (manual, pp. 30 and 60): the country of
  destination for exports, the member state the goods were shipped from for
  intra-EU imports, and the country of origin for extra-EU imports. Goods
  cleared in Rotterdam reach Germany as Dutch intra-EU exports (pp. 36-37), so
  the Netherlands, 14.5% of Germany's imports, need not be where they were
  made. Ships and aircraft follow their owner (pp. 87-90), and processing
  trade counts at the full value of the goods (pp. 25-26).
- **A partner confidential at product level moves to QY (intra-EU) or QZ
  (extra-EU) in the rows by product, while its `TOTAL` row still names it**
  (pp. 41-43): 3,422.5 and 6,315.7 M EUR in July 2026. Questions by partner
  read the totals, and questions by product the rows by product (The business
  questions).
- **Product codes ending in X hold confidential or estimated trade, or trade
  that simplification leaves without an eight-digit code** (pp. 28 and 40), at
  the level of a chapter (`28XXXXXX`), a heading (`2833XXXX`) or a subheading
  (`283321XX`): 47,801, 2,828 and 27,869 rows in July 2026, 1.56% of the rows
  by product. They add to the eight-digit codes rather than repeat them, so
  the HS hierarchy stops at the level they keep, and one of them can take a
  row per code of the parallel classifications (Silver).

<p align="right"><a href="#engineering-notes">Back to top</a></p>

## Landing and the manifest

**The landing task compares each month of the window with the
Volume's manifest and lands only the new and republished ones, through the
driver's disk.** Its last run found the seven months of 2026 current, landed
none, and downloaded the nine code lists again.

`land()` in `src/land.py`, the first task of the Job, reads the listing once,
before any download, and takes the months of the window from it (The
incremental run). `manifest.csv`, at the root of the Volume, records each
landed archive with its size and publication time, and `plan_months()`
compares the two: a `new` month is landed, a `replace` one landed over what
is there, a `current` one left alone, and a month whose CSV is missing is
`replace`. With 202605 as it first landed, against today's listing:

| Archive | In the manifest | In the listing | Status |
|---|---|---|---|
| `full_v2_202605.7z` | 46,127,843 bytes, `2026-09-15T09:38:30` | 46,148,464 bytes, `2026-10-16T10:00:08` | `replace` |
| `full_v2_202606.7z` | 47,304,745 bytes, `2026-10-16T10:00:09` | The same | `current` |
| `full_v2_202607.7z` | No row | 46,802,030 bytes, `2026-10-16T10:00:09` | `new` |

A month enters the manifest only after its files, so a run that stops in
between leaves it as it was, and the next run lands it again. In a run of the
Job with `end_period` 202607 and every month current, the task landed nothing
and downloaded the nine code lists, each the size the Volume held, in 236
seconds; a run that lands a month is in The Job.

**A republished month is replaced without asking.** Its CSV and its copy of
the published figures, fetched with it, are written over the old ones; the old
copy is deleted if the table holds no figure for the month yet, since it
belongs to another publication. The Delta tables built from it then hold only
the new publication, and the one before can be read by time travel for about
7 days, until the `VACUUM` that predictive optimization runs on managed tables
removes its files.

**The status reads the time next to the size, because a republication can
keep the size of a file**: the copy of the published figures of 202605 is 371
bytes in both of its publications, with other figures.

The nine code lists, about 6.5 MB, are written over the Volume's in every run.
Each holds every version of its codes, and the MERGE of the versioned
dimensions keeps a version its list no longer holds (Dimensions with history).
Their layout is in the [data dictionary](data_dictionary.md#code-lists).

**Each month goes through the driver's disk, since a Volume does not
take the random writes of an extraction.**

`land_month()` downloads the archive to a temporary folder on the driver,
extracts it there, and only then copies the CSV into the Volume with
`shutil.copyfile`, a sequential write (Databricks documentation); the manifest
is written the same way, so nothing is renamed in the Volume. The archive is
not kept, and the CSV lands as `full_v2_YYYYMM.csv`, the name bronze records
on every row.

On the driver both steps are atomic. The download writes to `.part`, renamed
only when its size equals the `Content-Length` announced, asked with
`Accept-Encoding: identity` so that the bytes on the wire are the bytes
written; the extraction goes to a temporary folder. An archive of another size
than the listing shows stops the run before anything reaches the Volume, since
the listing changed in between. One that would unpack to more than 2 GiB is
refused; the months of 2026 unpack to 400 to 423 MB.

The landing creates the schema `landing` and the Volume `comext` if they are
missing, and never the catalog, which on Free Edition is a Standard catalog
with default storage, created in Catalog Explorer.

<p align="right"><a href="#engineering-notes">Back to top</a></p>

## Bronze

**Bronze is a faithful copy of the landed files, every column as
text and one publication per month: 35,253,450 rows for the seven months of
2026.** The rows of each file equal its lines, counted without the CSV
parser.

`notebooks/01_bronze_ingest.py` loads the CSV files of the Volume into
`eu_trade.bronze.comext_trade`, a managed Delta table partitioned by `PERIOD`:
the 18 source columns as text, plus `_ingested_at`, `_source_file` and
`_batch_id`. It reads them through `read_landed_csv()` in `src/bronze.py`,
which the tests run on a local Spark ([Tests and
lint](development.md#tests-and-lint)).

| Rule | How |
|---|---|
| One file per month | The months of the window that the Volume's manifest names, through `landed_csv_paths()` in `src/watermark.py`. Never every CSV in the Volume |
| Every column as text | An explicit schema of 18 strings. A typed reader drops leading zeros, which 277,951 values of `PRODUCT_NC` carry in July 2026 |
| Header checked | `enforceSchema=false` compares each file's header with `EXPECTED_HEADER`, position by position, and fails if a name differs. Not in case: Spark compares column names without it, so a lowercase header loads, and every later step resolves its columns the same way |
| A malformed row fails the load | `mode=FAILFAST`, instead of a row padded with nulls |
| One publication per month | Only the pending months are loaded (The incremental run), each replaced whole with `replaceWhere` on its `PERIOD`, as silver and gold are. With none pending, the notebook ends without writing. All the files of a run go in one write, so a failure leaves the table as it was |

**A month republished since it was processed is pending again, and bronze
keeps only its new publication.** The month is replaced whole, so the rows of
the publication before leave the table. They can be read by time travel until
the `VACUUM` of predictive optimization removes their files, about 7 days
later.

Before it ends, the notebook checks the table against the files it loaded,
and fails if a file's rows differ from its lines read as plain text minus the
header, a row carries another `PERIOD` than its file, a field is null, or a
file took more than one batch: with one month in the table twice, it stops
naming that file. The seven full months of 2026-10-16, each with a valid
header, 0 nulls and 0 rows of another period:

| File | Archive bytes | Rows, equal to lines |
|---|---:|---:|
| `full_v2_202601.csv` | 44,955,883 | 4,877,993 |
| `full_v2_202602.csv` | 46,165,433 | 5,000,810 |
| `full_v2_202603.csv` | 47,757,078 | 5,149,512 |
| `full_v2_202604.csv` | 46,952,767 | 5,076,790 |
| `full_v2_202605.csv` | 46,148,464 | 4,994,361 |
| `full_v2_202606.csv` | 47,304,745 | 5,103,829 |
| `full_v2_202607.csv` | 46,802,030 | 5,050,155 |
| Total | | 35,253,450 |

**An empty field arrives as NULL**, since Spark's CSV reader takes the empty
string as its null marker by default (`nullValue` is `""`): the null check is
what would surface one, and no field of the seven full months is null.

<p align="right"><a href="#engineering-notes">Back to top</a></p>

## Silver

**Silver splits bronze into two typed tables, the rows by product
and the `TOTAL` rows, so no sum over the detail counts a euro
twice:** 5,037,372 and 12,783 rows in July 2026.

The rules live in `src/silver.py`, and `notebooks/02_silver_transform.py`
applies them. The checks live in `src/silver_checks.py`, with the failures
each one expects, and `notebooks/03_silver_quality_checks.py` runs them
(Quality checks). The counts are of July 2026, and the rules hold over the
seven full months.

| Table | Holds | Grain | July 2026 | Seven months |
|---|---|---|---:|---:|
| `trade_detail` | The rows by product: eight-digit codes and X codes | reporter, partner, product_nc, the five parallel classifications (SITC, CPA21, CPA22, BEC, BEC5), flow, stat_procedure, period | 5,037,372 | 35,164,358 |
| `trade_totals` | The `TOTAL` rows | reporter, partner, flow, stat_procedure, period | 12,783 | 89,092 |

- The `TOTAL` rows have a grain of their own, so they get a table of their
  own. They read `TOTAL` in the five classifications, `TO` in the section,
  `NO_SU` and a quantity of 0 in that unit, in every full month, so their
  table leaves those columns out.
- The grain of the detail includes the five parallel classifications because
  an X code can come in several rows, one per SITC code: Sweden's exports of
  `29XXXXXX` to QY are 20. Without them, July 2026 has 2,934 detail rows in
  1,175 repeated keys, every one of them an X code.
- The Q codes stay, since they count in totals. That they are not countries
  belongs to `dim_country`, from the seed.

The notebook reads the pending months from bronze, stops if a file is
missing, and writes each table with an overwrite of those months only
(`replaceWhere` on `period`), so a second run replaces its rows instead of
adding them. Then it checks that the bronze rows of each file are its detail
and its totals, each in one batch, and stops otherwise; no test covers that
check.

**`split_bronze()` types and names every column in one
`select`, then splits the rows in two: three rows of Austria's
exports to Switzerland in July 2026 become two rows of detail and one of
totals.**

The three rows in bronze, all of `REPORTER` AT, `PARTNER` CH, `FLOW` 2 and
`STAT_PROCEDURE` 1: the `TOTAL` row, and the eight-digit code and the X code
of the largest value, medicaments and a chapter of inorganic chemicals.

| `PRODUCT_NC` | `PRODUCT_SITC` | `PRODUCT_SECTION` | `SUPPL_UNIT` | `VALUE_EUR` | `QUANTITY_KG` | `QUANTITY_SUPPL_UNIT` |
|---|---|---|---|---:|---:|---:|
| `TOTAL` | `TOTAL` | `TO` | `NO_SU` | `948009982` | `199299553` | `0` |
| `30049000` | `54293` | `06` | `NO_SU` | `201432993` | `72347` | `0` |
| `28XXXXXX` | `524XX` | `06` | `NO_SU` | `2790723` | `18307` | `0` |

In `trade_detail`, the two rows by product, with 24 columns:

| `product_nc` | `hs2` | `hs4` | `hs6` | `is_x_code` | `suppl_unit` | `value_eur` | `quantity_kg` | `quantity_suppl_unit` |
|---|---|---|---|---|---|---:|---:|---|
| `30049000` | `30` | `3004` | `300490` | false | NULL | 201432993.000 | 72347 | NULL |
| `28XXXXXX` | `28` | NULL | NULL | true | NULL | 2790723.000 | 18307 | NULL |

In `trade_totals`, the `TOTAL` row keeps 11 columns, with `value_eur`
948009982.000 and `quantity_kg` 199299553. Both tables keep `_source_file`,
which the checks count by, and the notebook adds the `_batch_id` of its run;
their columns are in the [data dictionary](data_dictionary.md#silver). Over
July 2026, `is_x_code` marks 78,498 rows; `hs6` is NULL in 50,629, `hs4` in
47,801 and `hs2` in none; `suppl_unit` and its quantity in 3,656,762, exactly
the rows that read `NO_SU`. The totals add up by trade type and flow to the
sums that reconcile with the published figures (The source).

**Codes stay text, values are `DECIMAL(18,3)` and
`NO_SU` becomes NULL; Spark itself has two ways to break this
without an error.**

| Column | Rule |
|---|---|
| Codes | Text, every one of them, `flow` and `stat_procedure` included: a code is not a number. `period` is `YYYYMM`, and `period_date`, its first day, picks the valid row of each code list |
| `value_eur`, `value_nac` | `DECIMAL(18,3)`: at most 3 decimals and 13 integer digits are measured ([data dictionary](data_dictionary.md#bronze)). A float cannot hold a cent-level identity. `value_nac` stays as published: no question reads it |
| `quantity_kg`, `quantity_suppl_unit` | `BIGINT`: whole numbers of at most 11 digits |
| `suppl_unit` | `NO_SU` becomes NULL, and so does its `quantity_suppl_unit`, which is 0 in every `NO_SU` row: check 12c finds no other in the seven full months. With a real unit a 0 stays |
| `quantity_kg` | A 0 stays 0: nothing in the file tells "not reported" from "less than half a kilogram". Check 6a counts it, and a value per kilogram exists only with a positive weight |
| `hs2`, `hs4`, `hs6` | Truncations of `product_nc`, NULL below the level an X code keeps. Chapter 99 follows the same rule, though it belongs to the CN and not to the HS: 2,146 rows and 6,112.9 M EUR in July 2026 |

- **Spark resolves column names without regard to case.** A column named
  `value_eur` added to a frame that holds `VALUE_EUR` replaces it, so code that
  still reads `VALUE_EUR` gets the typed copy. Measured on that column, the
  number of decimals is the scale of the type, 10, not the 3 of the file.
- **Spark 4 casts in ANSI mode.** Text that is not a number fails the cast,
  which is wanted, but a fourth decimal would be rounded to the scale without
  a warning: `1.2345` becomes `1.235`. Check 12 compares each value cast to the
  silver type with the same value cast without loss.

<p align="right"><a href="#engineering-notes">Back to top</a></p>

## Gold

**Two facts, the two silver tables with each code replaced by the
key of the version valid in its month, and five dimensions read from the code
lists and the seeds.** The totals name the partner the rows by product
hide: read from the detail, Norway's July 2026 is 11.2% smaller.

`src/gold.py` builds the star, and `notebooks/04_gold_dimensional_model.py`
writes it to the `gold` schema. `fct_trade_detail` and `fct_trade_totals` keep
the grain of the two silver tables (Silver), one fact row per silver row, with
the reporter, the partner and the product replaced by the key of their
version; their columns are in the [data dictionary](data_dictionary.md#gold).

**Two facts, because the two kinds of rows name different partners.** The
rows by product book a partner confidential at product level under QY or QZ
(Partners and products): over every reporter and both flows, Norway's July
is 16,331.6 M EUR in the totals and 14,500.0 in the detail. Questions by
partner read the totals, and questions by product the detail. Over the whole
month the two facts are 66,547.619 EUR apart (Quality checks).

Each fact replaces its pending months, as silver does, and looks its keys up
in the dimension tables as written, so a fact joins what was recorded. The
notebook stops unless, per file, every silver row is one fact row of the
run's batch with every key resolved: 5,037,372 and 12,783 rows in July 2026.

| Table | One row per | Sources |
|---|---|---|
| `dim_country` | Version of a partner code | `PARTNERS_ISO.txt`; zone 1010 and the continent zones of `GEOZONES-PARTNERS_ISO.txt`, named from `GEOZONES.txt`; the seeds `euro_area_members.csv` and `partner_special_codes.csv` |
| `dim_product` | Version of an eight-character code of `CN.txt` | `CN.txt`, `CN_SECTIONS-CHAPTERS.txt`, `CN_SECTIONS.txt` |
| `dim_flow` | Flow | `FLOW.txt` |
| `dim_stat_procedure` | Procedure | `STATISTICAL_PROCEDURES.txt`: 7 codes, one row each |
| `dim_period` | Month | The months of silver |

**The dimensions read the code lists and the seeds as they are, with no
bronze table of their own**: reference data of about 6.5 MB, which the source
already versions and the landing replaces whole on every run. `dim_country`
and `dim_product` keep a row per version and load by MERGE (Dimensions with
history, below); the other three are rebuilt with an overwrite.

**Five places where a column of gold reads as something
else.**

- **`is_intra_eu`, not `is_eu_member`.** Zone 1010 holds the member states and
  QR, QV and QY, which are not states. Check 11 compares `trade_type` with it.
- **`region` is the continent zone**, 5100 Europe to 5900 not specified. On
  2026-07-01 every code of the month has one, except QR, QV and QY, listed in
  both 5100 and 5900, which take 5900.
- **`is_country` is false for the 10 codes of the seed
  `partner_special_codes.csv`**, and true for every other code.
- **The X codes come from `CN.txt`**, which lists them with a description and
  dates: `dim_product` holds no row of its own making. Their hierarchy is
  silver's, so `28XXXXXX` has no `hs4`.
- **The section on the fact row is the one the row reads**, and that of
  `dim_product` is its chapter's. They differ only in 175 rows of July 2026,
  codes `99XXXXXX` that read `04`; the views take the chapter's.

The five parallel classifications stay on the fact row, as codes, since an X
code can take several (Silver). No value per kilogram is stored: a ratio per
row does not add up, and question 5 divides sums.

<p align="right"><a href="#engineering-notes">Back to top</a></p>

## Dimensions with history (SCD type 2)

**`dim_country` and `dim_product` keep one row
per version of a code, 390 and 30,985, and a fact row joins the version valid
in its month.** Bulgaria's trade of July 2026 is euro-area trade, and its
trade of 2025 is not.

A slowly changing dimension of type 2 keeps a row for every version of a
member, with the dates it was valid, instead of overwriting the old values.
Usually the load makes the versions by comparing the source with the table.
Here the source already holds the history: Eurostat's code lists give each row
with its validity dates, so a dimension rebuilt from them gives the same rows.

Bulgaria has one row in `PARTNERS_ISO.txt`, from 1976. It enters the EU zone
of `GEOZONES-PARTNERS_ISO.txt` on 2007-01-01, and the euro area on 2026-01-01,
by the seed `reference/euro_area_members.csv`. Each date starts a version:

| `country_key` | `valid_from` | `valid_to` | `is_intra_eu` | `is_euro_area` | `is_current` |
|---|---|---|---|---|---|
| 866757845668845418 | 1976-01-01 | 2006-12-31 | false | false | false |
| 6262974360465178804 | 2007-01-01 | 2025-12-31 | true | false | false |
| 6769583107433018351 | 2026-01-01 | 2500-12-31 | true | true | true |

Bulgaria's exports of silver waste and scrap (`71129900`) to Germany in July
2026, 88,579,115 EUR, take the third version. Joined on the code alone they
would take all three: the month's 103,180 rows of Bulgaria would be 309,540.

**A version is a span between the dates of any source, inside a row of
the code's own list, and its key, `xxhash64(code, valid_from)`,
never moves.**

A version starts where a source opens a row, or the day after one closes it,
and ends the day before the next such date. Only spans inside a row of
`PARTNERS_ISO.txt` or `CN.txt` count, each with the attributes of its first
day. Germany's two rows there, up to 1990 and from 1991, stay two versions.

| Dimension | Rows of its list | Versions | What the other sources add |
|---|---:|---:|---|
| `dim_country` | 342 | 390 | The EU zone and the euro area, 37 versions in 26 codes; the continent zones, 11 in 10 codes, all before 1999 |
| `dim_product` | 30,984 | 30,985 | `9301S891`, split on 2002-01-01, when its heading 9301 starts a new row |

- **A product code is never changed**: the Combined Nomenclature closes it and
  opens another, as `28419085` on 2025-12-31 and `28419040`, lithium nickel
  manganese cobalt oxides, on 2026-01-01.
- **A version keeps its key when a later load closes it**, as Bulgaria's from
  2007 keeps 6262974360465178804. A sequential number would not: inserting
  Bulgaria's version of 2026 moves 358 of the 389 keys of 2025-12-31, and the
  facts already written would point to other countries. No key collides.
- Gold check 03 finds every fact row of the seven months joined to exactly one
  version of each dimension (Quality checks).

**The load is a MERGE written by hand that updates and inserts, and
never deletes: 2026 brought 1 and 1 in `dim_country`, 14 and 27 in
`dim_product`.**

`merge_dimension()` of `src/gold.py` matches the source to the table on
`(code, valid_from)`: a version whose end or attributes changed is updated, a
new one inserted, and one missing from the source counted, not deleted, so a
month already loaded keeps its version. The dimension as it stood on
2025-12-31, and today's merged into it:

| Dimension | On 2025-12-31 | Updated | Inserted | Missing | After |
|---|---:|---:|---:|---:|---:|
| `dim_country` | 389 | 1 | 1 | 0 | 390 |
| `dim_product` | 30,958 | 14 | 27 | 0 | 30,985 |

- Bulgaria's version from 2007 is updated, its `valid_to` from 2500-12-31 to
  2025-12-31, and its version of 2026 inserted; so are the 14 product codes
  closed and the 27 opened with 2026.
- **A repeated `(code, valid_from)` in the source is refused before the table
  is touched.** Delta 4.4 stops a MERGE over a repeated key the table holds,
  and inserts one it does not hold twice, without an error, which would join
  every fact row of that code twice.
- **Run again, the same MERGE changes nothing.** Open-source Delta still
  rewrites each file that holds a matched row; Databricks serverless writes
  nothing.

<p align="right"><a href="#engineering-notes">Back to top</a></p>

## The business questions

**Six views over the star answer the five questions, and a notebook
task creates them, since a SQL task would need the SQL warehouse.** The
notebook stops unless each view keeps the value of every fact row it
reads.

`sql/gold_views.sql` holds the views, and `sql/analysis_queries.sql` one query
per question that states the answer for the latest month loaded.
`notebooks/06_gold_views.py` runs both on the serverless compute of the Job,
where a SQL task would need the SQL warehouse, the most expensive compute
measured on Free Edition; the tests create the same views over test data. The
answers of July 2026 are in the README's [Results](../README.md#results), and
the counts behind them in `sql/results_queries.sql`, which no notebook runs.

| # | Question | View | Fact | Rule |
|---|---|---|---|---|
| 1 | Main partners, and how the ranking changed | `vw_partner_ranking` | Totals | The partners that are a country, ranked each month next to their rank the month before. The Q codes count in the flow's total |
| 2 | Products behind a surplus or a deficit | `vw_trade_balance_by_chapter` | Detail | Exports minus imports by reporter and chapter, with the chapter's section |
| 3 | Intra-EU against extra-EU | `vw_intra_extra_by_country`, `vw_intra_extra_by_chapter` | Totals by country, detail by chapter | By `trade_type`, which survives confidentiality: QY is intra-EU and QZ extra-EU |
| 4 | Dependence on a single partner | `vw_partner_dependence` | Detail | By reporter, heading and flow, the share of the largest partner that is a country, over the whole flow with the Q codes, and the share under Q next to it. The answer reads the headings of at least 1% of the reporter's flow |
| 5 | Value per kilogram across partners | `vw_value_per_kg_by_partner` | Detail | Eight-digit codes, a partner that is a country, a value and a weight, over every reporter. The answer compares the partners of at least 10 M EUR in the month |

- **Question 2 is asked by chapter**, with its section next to it: the
  balance of a section can hide chapters that offset each other.
- **Question 4 is asked by product, not by reporter.** The Netherlands hold
  14.5% of Germany's imports, and China 44.0% of its batteries (8507). An X
  code of a chapter reaches no heading and stays out: 32,182.3 M EUR, 2.63% of
  the detail of July 2026. Without the floor of 1%, 3,195 of the 59,883
  headings have a single partner; a floor in euros would follow the size of
  the reporter.
- **Question 5 compares the partners of the EU as a whole**, so cheese, wine
  and cars compare across European partners too. It keeps 4,670,839 of the
  4,958,874 rows of eight-digit codes of July 2026, 94.19%, with 97.90% of
  their value. Below 10 M EUR a few kilograms put noise at the top.

**Six rules of the SQL, each of which would change an answer if it
were written otherwise.**

- **`RANK()`, so tied values share a rank**: 29 rows of July 2026. The 156
  rows of Q codes take no rank.
- **The rank of the month before is the calendar month's**, joined on
  `ADD_MONTHS(period_date, -1)`, and NULL where that month is not loaded or
  the partner had no trade in it. `LAG` would take the latest month the
  partner traded in, however far back.
- **The largest partner of a heading is a country with a value**, and tied
  values name the lower code, in 21 headings of July 2026. None is named
  where every partner is a Q code, in 49, or where the heading has no value,
  in 12.
- **A sum of sums is `DECIMAL(38,3)`, which Spark divides to 6 decimals**, so
  a heading at 0.99996% of its flow would read 0.010000 and pass the floor.
  The view casts its sums back to `DECIMAL(28,3)`, whose quotients keep 10;
  no heading of July 2026 falls in that band. The test of Austria's share of
  its flow found it: 0.167306 at 6 decimals, 4.6e-7 off the exact share.
- **The value per kilogram divides the sums**, and never averages the ratios
  of the rows. Every ratio goes through `try_divide`: in ANSI mode a division
  by zero stops the query.
- **A count over a threshold is made on the quotient**, which the answer
  orders on and shows to 2 decimals: in July 2026, 1,260 products and flows
  have a dearest partner at twice the cheapest or more, and 1,266 on the gap
  shown, where six quotients of 1.997 to 1.999 read 2.00.

The views name their tables without a schema, and the notebook sets the gold
schema with `USE` first. The floors of questions 4 and 5 live in their
queries, so the views keep every heading and every partner.

**The notebook writes each view's fact rows again, apart from the view,
and compares the two in every month: in July 2026 the six views keep every
euro of the rows they read.**

`src/gold_views.py` holds the rows each view reads, so a view that loses,
adds or splits rows, or repeats a value of its grain, stops the notebook even
where it reads part of its fact. The views read every month and write
nothing. In July 2026 the views over the totals keep 1,224,894,873,220.229
EUR, those over the whole detail 1,224,894,939,767.848, question 4
1,192,712,668,368.800, the detail without the X codes of a chapter, and
question 5 1,164,149,183,748.356. The six views hold 9,566, 2,618, 54, 5,194,
59,883 and 923,569 rows.

<p align="right"><a href="#engineering-notes">Back to top</a></p>

## Quality checks

**20 checks, 13 on silver and 7 on gold, expect the same rule in
every month, and a result off its expectation in either direction stops the
run.** In each of the seven months, silver 06a found 5.3% to 5.6% of the
rows by product and gold 07 two groups, and every other check 0.

The checks live in `src/silver_checks.py` and `src/gold_checks.py`, and
notebooks 03 and 05 run them. Each writes one row per table it reads and per
source file into `ops.dq_check_results`, in a schema apart from the data
([data dictionary](data_dictionary.md#opsdq_check_results)); a check of a gold
dimension writes one, without a file. With one month pending, a run writes 20
results of silver and 28 of gold, 14 of them from gold 08, which reads every
month.

**Every month expects the same rule**, whatever its publication: `EXPECTED`
in each module names the checks expected to find something, as a count, a
share of the rows read or NULL for not evaluated, and every other check
expects 0. A share becomes a range of rows, rounded inward, so a month new to
the pipeline, or republished with other counts, needs nothing declared. The
notebook appends its results, reads back those of its batch, and stops on a
count off its expectation, up or down, outside its range, or NULL against a
number. A file a check returns nothing for fails an expectation of 0.

| Silver check | Rule | Reads | Expected |
|---|---|---|---:|
| `01_country_valid` | `reporter` and `partner` have a row of `PARTNERS_ISO.txt` valid on `period_date` | both tables | 0 |
| `02_product_valid` | `product_nc` is eight digits valid in `CN.txt` on `period_date`, or an X code of chapter, HS4 or HS6 | detail | 0 |
| `03_codes_in_domain` | `flow` in (1, 2); `trade_type` in (I, E); `stat_procedure` valid in `STATISTICAL_PROCEDURES.txt` | both tables | 0 |
| `04_period_matches_file` | `period` is the month in the file name | both tables | 0 |
| `05_values_not_negative` | `value_eur` and `value_nac` are not negative | both tables | 0 |
| `06a_kg_zero_with_value` | `quantity_kg` is 0 with a positive `value_eur`: counted, not dropped | detail | 5.0% to 6.0% of the rows |
| `08b_total_row_shape` | Every `TOTAL` row reads `TOTAL`, `TO`, `NO_SU` and 0 | bronze | 0 |
| `09_no_duplicate_on_grain` | No duplicate on the grain | both tables | 0 |
| `10_totals_match_published` | Totals by `trade_type` and `flow` against EU27_2020 and EXT_EU27_2020, and GB against UK, within 0.05 M EUR of the figures of the same publication | totals | 0 of 6, or not evaluated without a copy |
| `11_trade_type_matches_eu` | `trade_type` is I exactly when the partner belongs to zone 1010 of `GEOZONES-PARTNERS_ISO.txt` on `period_date` | both tables | 0 |
| `12a_cast_lossless` | Every value cast to its silver type equals the same value cast without loss, and is a number | bronze | 0 |
| `12b_null_only_from_sentinel` | No NULL except in `suppl_unit` and its quantity together, from `NO_SU`, and in `hs4` and `hs6` below the level of an X code | both tables | 0 |
| `12c_no_su_quantity_zero` | `NO_SU` always with a quantity of 0, which silver turns to NULL | bronze | 0 |

| Gold check | Rule | Reads | Expected |
|---|---|---|---:|
| `01_rows_match_silver` | Per file, as many fact rows as silver rows | each fact and its silver table | 0 |
| `03_one_match_per_dimension` | Each fact row finds exactly one row in each of its dimensions | each fact and its dimensions | 0 |
| `04_value_matches_silver` | Per file, the `value_eur` of the fact adds up to silver's | each fact and its silver table | 0 |
| `05_dimension_key_unique` | No two rows share the surrogate key or `(code, valid_from)`; in the three small dimensions, the code | the five dimensions | 0 |
| `06_no_overlapping_versions` | No version of a code starts before the one before it ends | `dim_country`, `dim_product` | 0 |
| `07_detail_matches_totals` | Per reporter, trade type, flow, procedure and file, the detail within 0.01 M EUR of the totals | both facts | 2 |
| `08_version_covers_month` | The version each key of a fact row points to covers the row's `period_date`, in every month of the facts | each fact, `dim_period` and its versioned dimensions | 0 |

The seven full months, all from the publication of 2026-10-16:

| Month | Rows by product | `06a` | Gold `07` | Detail less totals, EUR |
|---|---:|---:|---:|---:|
| 202601 | 4,865,450 | 271,808 | 2 of 239 | 63,142.363 |
| 202602 | 4,988,110 | 271,767 | 2 of 240 | 61,919.063 |
| 202603 | 5,136,739 | 272,844 | 2 of 242 | 66,710.387 |
| 202604 | 5,064,050 | 272,068 | 2 of 240 | 65,323.908 |
| 202605 | 4,981,646 | 274,225 | 2 of 239 | 64,163.133 |
| 202606 | 5,090,991 | 281,201 | 2 of 241 | 68,008.669 |
| 202607 | 5,037,372 | 283,190 | 2 of 240 | 66,547.619 |

**A check off by one row stops the run.** With the expectation of 06a moved
one row off, up and then down, each run wrote its results and stopped, naming
the check, the table, the file and both counts.

**Why each check reads what it reads, and what its expectation
allows.**

- **06a expects 5.0% to 6.0%**, a margin around the 5.312% to 5.586% of the
  six months it was set on; 202607 then found 5.622%.
- **10 reads a copy of the published figures saved with its file** (Landing
  and the manifest), never the live table, which is revised with the files
  (The source). Without a copy the file is not evaluated. Over the seven
  months the largest gap is 0.048 M EUR.
- **08b, 12a and 12c read bronze**, where the columns are still as landed:
  the `TOTAL` rows before the split, each value as text, so that one that is
  not a number counts instead of stopping the run, and the 0 of `NO_SU`.
- **Gold 03 is the strict form of "the star keeps the rows"**: counted row by
  row, a row lost and another counted twice cannot cancel out. An unresolved
  key finds no row, and a repeated key two.
- **Gold 05 and 06 read the dimensions as merged**, with every version an
  earlier MERGE left (Dimensions with history): a code list row that changed
  its start would be inserted next to the old version, which 06 counts.
- **Gold 08 reads every month of the facts**: a later MERGE that moves a
  version's dates leaves older rows pointing to a version that no longer
  covers their month, with the key still found (The incremental run).
- **The numbers of the checks retired are not reused**: silver 06b, 07 and
  08a and gold 02, which described columns no question reads or repeated
  another check. A `check_name` keeps one meaning across runs.
- **No check starts a Python worker.** The code lists are read as text and
  split on the tab, since 5,078 lines of `CN.txt` hold a double quote, and
  every rule is a column expression.

**The rows by product exceed the `TOTAL` rows by 61,919 to
68,009 EUR a month, and only France's intra-EU trade passes the tolerance of
gold 07, with no documented cause.**

Per reporter, trade type, flow and procedure, summed over every partner and
product, the detail is above the totals in France's intra-EU imports by
12,381 to 15,452 EUR, and in its exports by 24,144 to 26,751, in each of the
seven months; no other group passes 0.01 M EUR. The manual never mentions
rounding, and says of `TOTAL` only that it is the one aggregate the bulk
files precalculate (p. 83). The one exception it describes, confidential
trade kept in a country's total but allocated to no chapter (p. 42), would
put the totals above the detail. The sums come from the gold facts through
`sql/results_queries.sql`.

<p align="right"><a href="#engineering-notes">Back to top</a></p>

## The incremental run

**A month is pending when the Volume's manifest dates its archive
otherwise than the watermark, and a run writes only the pending
months.** 202605, republished, and 202607, new, were each pending in their
run, and the other five months were not written again.

`ops.etl_watermark` records, per file, the publication the pipeline processed
([data dictionary](data_dictionary.md#opsetl_watermark)). `src/watermark.py`
compares it with the Volume's manifest, which the landing writes (Landing and
the manifest), for the months of the window. The rows behind two runs, one
with 202605 republished and one with 202607 new:

| Month | Manifest | Watermark at first | Run over 202601 to 202606 | Run over 202601 to 202607 |
|---|---|---|---|---|
| 202605 | 46,148,464 bytes, `2026-10-16T10:00:08` | 46,127,843 bytes, `2026-09-15T09:38:30` | Pending: republished. The watermark takes the manifest's row | Not pending |
| 202606 | 47,304,745 bytes, `2026-10-16T10:00:09` | The same | Not pending | Not pending |
| 202607 | 46,802,030 bytes, `2026-10-16T10:00:09` | No row | Outside the window | Pending: new. The watermark gains its row |

Each notebook from 01 to 05 asks for the pending months before it writes
anything: bronze, silver and both facts replace those months whole, and the
checks read their files. A month of the window the manifest does not name
stops the run, since its publication is unknown. **The gold checks notebook
writes the watermark last**, and only when every result of both layers
matched its expectation, so a run that stops leaves its months pending, and
the next one writes them again in place. With nothing pending, each notebook
says so and ends its task with success through `dbutils.notebook.exit()`.
The views notebook reads every month and no watermark. Gold check 08 belongs
here: the facts of the months not processed were built against an earlier
MERGE (Quality checks).

**The window runs from `start_period` to
`end_period`, and with `end_period` empty the landing
stops at the month before the last one listed.**

A run takes its months from two job parameters, `start_period`, `202601` by
default, and `end_period`, empty, as `YYYYMM` (The Job). They reach notebooks
00 to 05 as widgets of the same names, created in a cell of their own,
`Run settings`, right after the imports, because Databricks never shows a
widget created by a command that fails. `config.window_from()` reads them,
and stops the run on a value that is not a month, naming its parameter, or on
a window that ends before it starts.

| Notebooks | Window | With `end_period` empty |
|---|---|---|
| `00_land` | The months of the listing, from `start_period` to `end_period` | To the month before the last one listed (The source) |
| `01` to `05` | The months of the Volume's manifest, from `start_period` to `end_period` | To the last month the manifest names |

`config.months_in_window()` reads the months from the names of the monthly
archives, `full_v2_YYYYMM.7z`, in the listing and the manifest alike. It
lives in `config` because importing `land` imports `py7zr`, which only the
landing task installs. An open window lets a month enter on its own: the next
release completes the newest month and lists a newer one, and the landing
lands the first.

**Three runs on serverless over the full months, and one of the Job,
each wrote what was pending and left the rest as it was.**

| Run | What it wrote |
|---|---|
| Republished, 202601 to 202606 | 202605 alone. Bronze replaced its 4,988,013 rows of 2026-09-15 with the 4,994,361 of 2026-10-16 in one write; silver and the facts, 4,981,646 and 12,715. Every check as expected, check 10 against the copy of 2026-10-16, and both MERGE at 0 updated and 0 inserted. Of the 36 lines the views notebook prints, the 30 of the other months stayed identical, byte for byte; those of 202605 moved, the total of its trade by 294.2 M EUR |
| New month, 202601 to 202607 | 202607 alone: 5,050,155 rows in bronze, 35,253,450 in all, and 5,037,372 and 12,783 in silver and the facts. The views printed 42 lines, the 36 of the run before unchanged |
| Nothing pending, the same | Nothing: notebooks 01 to 05 ended without writing, and the last commit of each of the twelve tables they write is that of the run before |

The Job of this repository then landed 202607 again from Eurostat and took it
through every layer, with the same numbers and the same 42 lines (The Job).

<p align="right"><a href="#engineering-notes">Back to top</a></p>

## The Job

**One Job runs the seven notebooks as a chain of tasks on
serverless, from the branch `main`, one task at a time.** With
`end_period` 202607 it landed 202607 again from Eurostat and took
it to gold in 13 minutes, with the counts of the incremental run and the same
42 lines of the views, byte for byte.

`jobs/trade_pipeline_job.json` defines `eu_trade_pipeline` as the Jobs API
takes it, and `src/deploy_job.py` creates it from the file, or replaces the
settings of the one of that name ([Deploying the
Job](development.md#deploying-the-job)). Each task starts when the one before
it succeeds, since each layer reads the one before.

| Setting | Value | Why |
|---|---|---|
| Source | `git_source`: the branch `main`, every task with `source: GIT` | Each run takes a snapshot of the branch and records its commit on every task. No Git folder has to be pulled first |
| Parameters | `start_period`, `202601`, and `end_period`, empty | Each reaches every task as the widget of its name (The incremental run). Run now with different parameters changes them for one run |
| Environment | `land`: environment version 6 with `py7zr==1.1.4`, the version of the lock, for the task `land` only | The other tasks run on the environment of their notebook's header, and none of them needs `py7zr` |
| Compute | None declared, and `performance_target: STANDARD` | Serverless, on the SKU of performance optimized: fewer DBU, a longer start |
| Concurrent runs | 1 | Two runs at once would find the same pending months, since the gold checks write the watermark last |
| Schedule | Monthly, on the 20th at 06:00 Europe/Brussels, paused | How it would run in production, after each monthly release. Paused, it spends no quota on its own |

**The landing is the first task.** It downloads from `ec.europa.eu` on the
Job's serverless compute, which reaches the internet on Free Edition, so a run
processes what it landed itself, with no step outside the workspace. Only the
first task waits for compute, and the landing also installs its environment:
it took 287 seconds in the run of 202607, 41 of them landing the month. With
the defaults, a run over the seven months left 202608 out, landed nothing and
found nothing pending, and printed the same 42 lines, in 8 minutes.

<p align="right"><a href="#engineering-notes">Back to top</a></p>
