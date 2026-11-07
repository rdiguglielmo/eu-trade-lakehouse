# Engineering Notes

The rules in this repository that are easy to break and expensive to break.
[`README.md`](../README.md) says what the project is.

## The source

<details open>
<summary><b>One 7z archive per month, holding one CSV of 18 columns, which
Eurostat republishes in place: a file is told by its publication time, not by
its month.</b> July 2026 is 46,802,030 bytes and 5,050,155 rows, as published
on 2026-10-16.</summary>

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
therefore stops at the month before the last one listed (The window of a run).

<details>
<summary>Every file carries its own totals as rows, <code>PRODUCT_NC =
TOTAL</code>, so a sum over the file counts every euro twice.</summary>

One row per reporter, partner, flow and statistical procedure sums the other
rows of that combination: in July 2026, 12,783 `TOTAL` rows add up to
1,224,894.873 M EUR, and the other rows to 1,224,894.940. They read `TOTAL` in
every classification column but `PRODUCT_SECTION`, which reads `TO`, so a
filter on the section lets them through: the filter is `PRODUCT_NC = 'TOTAL'`,
and silver gives them a table of their own (Silver).

</details>

<details>
<summary>The <code>TOTAL</code> rows reproduce the figures Eurostat publishes,
within their rounding of 0.05 M EUR, against the same publication
only.</summary>

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

</details>

</details>

## Partners and products

<details open>
<summary><b>Four properties of the codes change a ranking or a sum without
changing a row count.</b> In July 2026, 9,738.2 M EUR, 0.80% of the month,
changes partner with the rows read, and codes ending in X hold 2.93% of the
value of the rows by product.</summary>

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
  read the totals, and questions by product the rows by product (Gold, What
  each business question reads).
- **Product codes ending in X hold confidential or estimated trade, or trade
  that simplification leaves without an eight-digit code** (pp. 28 and 40), at
  the level of a chapter (`28XXXXXX`), a heading (`2833XXXX`) or a subheading
  (`283321XX`): 47,801, 2,828 and 27,869 rows in July 2026, 1.56% of the rows
  by product. They add to the eight-digit codes rather than repeat them, so
  the HS hierarchy stops at the level they keep, and one of them can take a
  row per code of the parallel classifications (Silver).

</details>

## Landing and the manifest

<details open>
<summary><b>The landing task compares each month of the window with the
Volume's manifest and lands only the new and republished ones, through the
driver's disk.</b> Its last run found the seven months of 2026 current, landed
none, and downloaded the nine code lists again.</summary>

`land()` in `src/land.py`, the first task of the Job, reads the listing once,
before any download, and takes the months of the window from it (The window of
a run). `manifest.csv`, at the root of the Volume, records each landed archive
with its size and publication time, and `plan_months()` compares the two: a
`new` month is landed, a `replace` one landed over what is there, a `current`
one left alone, and a month whose CSV is missing is `replace`. With 202605 as
it first landed, against today's listing:

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

<details>
<summary>Each month goes through the driver's disk, since a Volume does not
take the random writes of an extraction.</summary>

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

</details>

</details>

## Bronze

<details open>
<summary><b>Bronze is a faithful copy of the landed files, every column as
text and one publication per month: 35,253,450 rows for the seven months of
2026.</b> The rows of each file equal its lines, counted without the CSV
parser.</summary>

`notebooks/01_bronze_ingest.py` loads the CSV files of the Volume into
`eu_trade.bronze.comext_trade`, a managed Delta table partitioned by `PERIOD`:
the 18 source columns as text, plus `_ingested_at`, `_source_file` and
`_batch_id`. It reads them through `read_landed_csv()` in `src/bronze.py`,
which the tests run on a local Spark (Tests and lint).

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

</details>

## Silver

<details open>
<summary><b>Silver splits bronze into two typed tables, the rows by product
and the <code>TOTAL</code> rows, so no sum over the detail counts a euro
twice:</b> 5,037,372 and 12,783 rows in July 2026.</summary>

The rules live in `src/silver.py`, and `notebooks/02_silver_transform.py`
applies them. The checks live in `src/silver_checks.py`, with the failures
each one expects, and `notebooks/03_silver_quality_checks.py` runs them (The
checks, below). The counts are of July 2026, and the rules hold over the
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

<details>
<summary><code>split_bronze()</code> types and names every column in one
<code>select</code>, then splits the rows in two: three rows of Austria's
exports to Switzerland in July 2026 become two rows of detail and one of
totals.</summary>

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

</details>

<details>
<summary>Codes stay text, values are <code>DECIMAL(18,3)</code> and
<code>NO_SU</code> becomes NULL; Spark itself has two ways to break this
without an error.</summary>

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

</details>

</details>

### The checks

Each check writes one row per table it reads and per source file into
`ops.dq_check_results`, a Delta table in a schema apart from the data, which
every run appends to:

| Column | Holds |
|---|---|
| `check_name` | The number of the check and what it asks, such as `06a_kg_zero_with_value`: the names of `CHECKS` in `src/silver_checks.py`, and in `src/gold_checks.py` for gold |
| `layer` | `silver`, or `gold` for the checks of gold (Gold, The checks of gold) |
| `table_name` | The table the check reads. Checks 08b, 12a and 12c read bronze, where the columns they look at are still as landed |
| `source_file` | The landed file the rows come from, or NULL for a check of a gold dimension, which reads every version |
| `batch_id`, `run_at` | The run of the checks that wrote the row |
| `rows_checked` | What the check read: rows of the table, or published figures for 10. In gold, the one sum of a file for 04, and groups for 07 |
| `rows_failed` | How many of them break the rule, or NULL if the check could not evaluate the file |
| `expected_failures` | The count the check is expected to find there, or NULL if it is expected to find a range, or not to evaluate the file |
| `severity` | `error`, for every check |
| `expected_min`, `expected_max` | The range the check is expected to fall in, both included, or NULL if it expects a count |

**Every month expects the same rule**, whatever the month and its
publication: `EXPECTED` in `src/silver_checks.py` names the checks expected to
find something, and every other check is expected at 0. A month new to the
pipeline, or one republished with other counts under the same file name, needs
nothing declared. A rule is a count, NULL for not evaluated, or a `Rate`: a
share of the rows the check read, which the notebook turns into a range of
rows, rounded inward, from the file's `rows_checked`. Check 10 of a file that
landed without a copy of the published figures expects NULL: it has nothing to
compare.

The notebook appends the results first, then reads back those of its batch and
compares: a count that differs from its expectation, up or down, or falls
outside its range, fails the run, and so does NULL against a number, either
way. A rate over a file the check could not evaluate becomes the range 0 to 0,
which NULL does not meet. A run that fails leaves its results in the table.

A file a check returns nothing for was not evaluated, so a check that compares
nothing does not pass unless it is expected not to evaluate: a check over a
table that lacks one of the files of the run returns nothing for it, and its
expectation of 0 then fails the run.

| Check | Rule | Reads | Expected |
|---|---|---|---:|
| `01_country_valid` | `reporter` and `partner` have a row of `PARTNERS_ISO.txt` valid on `period_date` | both tables | 0 |
| `02_product_valid` | `product_nc` is eight digits valid in `CN.txt` on `period_date`, or an X code of chapter, HS4 or HS6 | detail | 0 |
| `03_codes_in_domain` | `flow` in (1, 2); `trade_type` in (I, E); `stat_procedure` valid in `STATISTICAL_PROCEDURES.txt` | both tables | 0 |
| `04_period_matches_file` | `period` is the month in the file name | both tables | 0 |
| `05_values_not_negative` | `value_eur` and `value_nac` are not negative | both tables | 0 |
| `06a_kg_zero_with_value` | `quantity_kg` is 0 with a positive `value_eur`: counted, not dropped | detail | 5.0% to 6.0% of the rows |
| `08b_total_row_shape` | Every `TOTAL` row reads `TOTAL`, `TO`, `NO_SU` and 0 | bronze | 0 |
| `09_no_duplicate_on_grain` | No duplicate on the grain | both tables | 0 |
| `10_totals_match_published` | Totals by `trade_type` and `flow` against EU27_2020 and EXT_EU27_2020, and GB against UK: within 0.05 M EUR of the figures of the same publication | totals | 0 of 6, or not evaluated without a copy |
| `11_trade_type_matches_eu` | `trade_type` is I exactly when the partner belongs to zone 1010 of `GEOZONES-PARTNERS_ISO.txt` on `period_date` | both tables | 0 |
| `12a_cast_lossless` | Every value cast to its silver type equals the same value cast without loss, and is a number | bronze | 0 |
| `12b_null_only_from_sentinel` | No NULL except in `suppl_unit` and its quantity together, from `NO_SU`, and in `hs4` and `hs6` below the level of an X code | both tables | 0 |
| `12c_no_su_quantity_zero` | `NO_SU` always with a quantity of 0, which silver turns to NULL | bronze | 0 |

- **The rate of 06a** holds the six full months of 202601 to 202606 with a
  margin: it found 5.312% to 5.586% of their detail rows. The margin is there
  for the months to come: 06a found 5.622% in 202607. A count outside the range
  stops the run, as a count off its expectation does, so a change of the rule,
  or of the data, still shows.
- **The numbers of the checks retired are not reused**: 06b, 07 and 08a, which
  described columns no question reads or repeated what a test and gold check
  07 cover. A `check_name` in `ops.dq_check_results` keeps one meaning across
  runs.
- **Check 10** reads a copy of the published figures saved with the landed
  file, holding `period`, `partner`, `stk_flow`, `value_meur` and
  `table_updated`: never the live table, which is revised together with the
  files. The landing saves the copy when it lands a month, in the same run:
  `published/<CSV name>` in the Volume, fetched from the statistics API through
  the same access policy as the files. A month that stays `current` keeps the
  copy it has, since figures fetched later could belong to a later publication;
  a month replaced takes the figures of its new publication, or loses its old
  copy if the table holds none yet (Landing and the manifest). For a month the
  table does not hold yet, the API answers 200 with no value: no copy is
  written, a warning says so, and check 10 does not evaluate that file. The
  check can fail: against the table as revised on 2026-10-16, the full month of
  202605 as published on 2026-09-15 misses all 6 figures, by 19.8 to 248.5 M
  EUR. Each of the seven full months landed with its copy, six figures with
  `updated` = `2026-10-16T11:00:00+0200`, the revision of their files.
- **Check 11** is the intra-EU logic. Zone 1010 holds the 27 member states and
  the three intra-EU Q codes, and no other partner is traded as intra-EU.
- **Check 12** comes in three parts because each reads a different table.
  12a follows from the maximums measured: 3 decimals and 13 integer digits fit
  `DECIMAL(18,3)`, and the cast reads every value, `.999` included. It compares
  in bronze, where the value is still text, so a value that is not a number
  counts there instead of stopping the run. 12c reads bronze too, since silver
  turns the quantity of `NO_SU` to NULL. 12b reads the silver tables.
- **No check starts a Python worker**: the code lists are read as text and
  split on the tab, the seed and the copies with Spark's CSV reader, and every
  rule is a column expression. A code list is not read as CSV because 5,078
  lines of `CN.txt` hold a double quote. A code has a row per period of
  validity, so the checks join on the code and the date against pairs of each
  valid code with each `period_date` present, which match a row at most once.
  The only query with a worker is the one that writes the results.

#### What the checks found

On the full month of 202605 as published on 2026-09-15, every check found
exactly its expected failures: 06a 274,613 of 4,975,304 detail rows. Every
other check found nothing, check 10 included, against the six
published figures of the same publication. 12a and 12c read the 4,988,013
bronze rows of the month.

Over the full months of 202601 to 202606, 202605 from the publication of
2026-09-15 and the other five from that of 2026-10-16, every check again found
exactly its expected failures, 06a within its range in each month. Every other
check found nothing in any month, and check 10 matched all 36 published
figures, the largest gap 0.047 M EUR.

| File | Detail rows | `06a_kg_zero_with_value` |
|---|---:|---:|
| `full_v2_202601.csv` | 4,865,450 | 271,808 |
| `full_v2_202602.csv` | 4,988,110 | 271,767 |
| `full_v2_202603.csv` | 5,136,739 | 272,844 |
| `full_v2_202604.csv` | 5,064,050 | 272,068 |
| `full_v2_202605.csv` | 4,975,304 | 274,613 |
| `full_v2_202606.csv` | 5,090,991 | 281,201 |

The two files of the incremental run (The incremental run), each within the
range of its rule, and every other check as over the six:

| File | Detail rows | `06a_kg_zero_with_value` |
|---|---:|---:|
| `full_v2_202605.csv`, of 2026-10-16 | 4,981,646 | 274,225, range 249,083 to 298,898 |
| `full_v2_202607.csv` | 5,037,372 | 283,190, range 251,869 to 302,242 |

- The expectations of the five new months were counted before the checks ran,
  without Spark: each CSV read with pandas, every value as an integer of
  thousandths of a euro, and the rules of 06a, 10 and gold 07 applied as
  `src/silver_checks.py` and `src/gold_checks.py` write them. On 202605 that
  count gave every known number first, 12,709 `TOTAL` rows, 274,613, 0 of 6
  figures and 2 of 239 groups, and only then were its counts of the other
  months declared.
- **A check off by one row stops the run.** With an expectation of 06a moved
  one row off what the check counts, up and then down, each run wrote its
  results and stopped, naming the check, the table, the file and both counts.

## Gold

Gold is the dimensional model over silver: two facts, one per silver table,
and the dimensions they point to. `src/gold.py` builds them, and
`notebooks/04_gold_dimensional_model.py` writes them to the `gold` schema. The
design was measured on the full month of 202605 as published on 2026-09-15,
with the dimensions built from the code lists and the seeds, and the notebook
writes the same counts from the same file. The checks of gold live in
`src/gold_checks.py`, with the failures each one expects, and
`notebooks/05_gold_quality_checks.py` runs them (The checks of gold, below).
The views of `sql/gold_views.sql` answer the five business questions over the
star, and `notebooks/06_gold_views.py` creates them (The views of the business
questions, below).

### Two facts, one per silver table

| Table | Grain | Other columns |
|---|---|---|
| `fct_trade_detail` | `reporter_key`, `partner_key`, `product_key`, the five parallel classifications, `flow`, `stat_procedure`, `period` | `trade_type`, `product_section`, `suppl_unit`, `value_eur`, `value_nac`, `quantity_kg`, `quantity_suppl_unit`, `_source_file`, `_batch_id` |
| `fct_trade_totals` | `reporter_key`, `partner_key`, `flow`, `stat_procedure`, `period` | `trade_type`, `value_eur`, `value_nac`, `quantity_kg`, `_source_file`, `_batch_id` |

- **The `TOTAL` rows name the real partner; the rows by product do not.** The
  rows by product book 9,071.8 M EUR of the full month, 0.78%, under QY and QZ
  (Partners and products). A ranking of partners read from them puts Norway
  at 14,832.7 M EUR instead of 16,500.4. Over the whole month the two facts
  agree within 0.064 M EUR.
- **The parallel classifications stay on the row, as codes**: they describe
  the row, not the product. In the full month of 202605 they take 3,956
  distinct combinations. Three eight-digit codes take a second one in a single
  row, with a confidential SITC code: `15042090` reads SITC `41112` in 585 rows
  and `411XX` in one. Of the 5,114 X codes, 100 take several, up to 17. As
  attributes of the product they would not fit, and no business question reads
  them.
- `product_section` is the section the row reads, and `dim_product` holds the
  section of the product's chapter. They differ only in `99XXXXXX` rows that
  read `04`: 164 rows of the full month of 202605.
- **No value per kilogram on the fact.** A ratio per row does not add up:
  question 5 divides sums.
- Each fact is written as silver is, with an overwrite of the pending months.
  Its keys are looked up in the dimension tables as written, so a fact joins
  what was recorded. The notebook stops unless, per file, every silver row is
  one fact row, of the run's batch, with every key resolved: 4,975,304 detail
  rows and 12,709 totals rows in the full month of 202605.

### Dimensions

| Table | One row per | Columns | Sources |
|---|---|---|---|
| `dim_country` | Version of a partner code | `country_key`, `country_code`, `country_name`, `is_country`, `partner_category`, `is_intra_eu`, `is_euro_area`, `region_zone`, `region`, `valid_from`, `valid_to`, `is_current` | `PARTNERS_ISO.txt`; zone 1010 and zones 5100 to 5900 of `GEOZONES-PARTNERS_ISO.txt`, named from `GEOZONES.txt`; `reference/euro_area_members.csv`; `reference/partner_special_codes.csv` |
| `dim_product` | Version of an eight-character code of `CN.txt` | `product_key`, `product_nc`, `description`, `is_x_code`, `hs6`, `hs4` and `hs2` with their descriptions, `section_code`, `section_description`, `valid_from`, `valid_to`, `is_current` | `CN.txt`, `CN_SECTIONS-CHAPTERS.txt`, `CN_SECTIONS.txt` |
| `dim_flow` | Flow | `flow`, `flow_name` | `FLOW.txt` |
| `dim_stat_procedure` | Procedure | `stat_procedure`, `description`, `valid_from`, `valid_to` | `STATISTICAL_PROCEDURES.txt`: 7 codes, one row each |
| `dim_period` | Month | `period`, `period_date`, `year`, `quarter`, `month` | The months of silver |

- **`dim_country` and `dim_product` keep their history**, one row per version
  of a code (Dimensions with history, below); the other three are rebuilt
  with an overwrite.
- **`is_intra_eu`, not `is_eu_member`.** Zone 1010 holds QR, QV and QY, which
  are not member states. It is the zone check 11 compares `trade_type` with.
- **`region` is the continent zone**: 5100 Europe, 5200 Africa, 5300 America,
  5400 Asia, 5500 Oceania and polar regions, 5900 not specified. QR, QV and QY
  are listed both in 5100 and in 5900, and take 5900: they are not countries.
  On the dates of the data every code has exactly one: 48, 59, 50, 49, 33 and 7
  codes. `ZZ` ("No data, work code") has none, and is not in the data.
- `is_country` is false, and `partner_category` the seed's, for the 10 codes of
  `reference/partner_special_codes.csv`; every other code is a country.
- **The X codes come from `CN.txt`**, which lists them (data dictionary, Code
  lists). Every X code of the seven months has a row valid in its month, so
  `dim_product` needs no row of its own making. The hierarchy is silver's: an
  X code stops at its own level, and `28XXXXXX` has no `hs4`. The section is
  the chapter's, from
  `CN_SECTIONS-CHAPTERS.txt`; every chapter of the data has one.

### What each business question reads

| # | Question | Fact | Rule |
|---|---|---|---|
| 1 | Main partners, and how the ranking changed | Totals | Only partners where `is_country` are ranked, each month, next to their rank the month before. The Q codes count in the reporter's total |
| 2 | Products behind a surplus or a deficit | Detail | Exports minus imports by reporter and chapter, over every partner, with the chapter's section |
| 3 | Intra-EU against extra-EU | Totals by country, detail by chapter | `trade_type` survives confidentiality: QY is intra-EU and QZ extra-EU |
| 4 | Dependence on a single partner | Detail | By reporter, HS4 and flow, the share of the largest partner that is a country, over the whole flow with the Q codes. The share under Q is reported next to it, so the share is a lower bound. The answer reads the headings that hold at least 1% of the reporter's flow |
| 5 | Value per kilogram across partners | Detail | Eight-digit codes, a partner that is a country, a positive value and a positive weight, summed over every reporter: the EU as a whole. The ratio divides sums. The answer compares the partners of at least 10 M EUR in the month, the dearest against the cheapest |

- **Question 2 is asked by chapter, with its section next to it.** A section
  can hold chapters whose balances offset each other, and its own balance
  hides them. A section's balance is the sum of its chapters.
- **Question 4 is asked by product, not by reporter.** At the level of
  reporter and flow the largest partner of Germany's imports in 202605, the
  Netherlands, holds 14.7%. By HS4, China holds 45.9% of its imports of
  batteries (8507). Chapter-level X codes reach no HS4 and stay out:
  29,840.1 M EUR, 2.58% of the detail value of the full month.
- **A heading counts in the answer of question 4 from 1% of its reporter's
  flow.** Without a floor, 3,222 of the 59,926 headings of the full month have
  a single partner at 100%. A floor in euros follows the size of the reporter,
  and leaves the smallest with almost no heading; a share of the reporter's own
  flow gives each one the headings that weigh in its trade. The flow is the
  reporter's whole detail in that direction, X codes of a chapter and Q codes
  included.
- **Question 5 keeps 4,600,653 of the 4,876,391 eight-digit rows of the full
  month**, 94.35%, and 97.76% of their value.
- **Question 5 compares the partners of the EU as a whole, from 10 M EUR in
  the month.** Summed over every reporter, a product's partners are the EU's
  suppliers or customers, the intra-EU ones included, so cheese, wine and cars
  compare across European partners too. Below 10 M EUR, partners of a few
  kilograms put noise at the top of the answer. With that floor, 2,585 products
  of the full month have two partners or more.
- **A value per kilogram can be wrong where the value is right.** A reporter
  may keep the quantity confidential and publish the value, to protect the
  price it implies; in intra-EU trade the net mass of a code with a
  supplementary unit need not be collected, and is then estimated; and a
  weight below 0.5 kg reads 0 (manual, pp. 33-34, 40-41 and 49). The manual
  places the most common errors at the most detailed level, ratios of value to
  volume among them (p. 48). Motor spirit to Singapore at 912.67 EUR per
  kilogram, against 0.89 to Angola, is a ratio these leave open; the manual
  does not say which applies.
- **An eight-digit code can hold goods of different quality.** `09011100`
  is every coffee neither roasted nor decaffeinated (`CN.txt`), so a gap
  between its partners need not be a gap in price for one good.

### The views of the business questions

`sql/gold_views.sql` holds one view per question over the star, and
`sql/analysis_queries.sql` one query per question that states the answer for
the latest month loaded. `notebooks/06_gold_views.py` creates the views in the
gold schema, stops unless every view keeps the value of the fact rows it reads
in each month and repeats no value of its grain, and runs the queries. Both
files hold the five questions. `sql/results_queries.sql` holds the queries
behind the README's Results and Limitations that are not an answer itself:
counts over an answer, and the rows behind a case it names. It reads the same
views and facts in the gold schema, and no notebook runs it.

| View | Question | Reads | One row per | Columns |
|---|---|---|---|---|
| `vw_partner_ranking` | 1 | `fct_trade_totals` | Month, reporter, flow and partner, over every procedure | `value_eur`, `flow_total_eur`, `share_of_flow`, `partner_rank`, `previous_month_rank`, `rank_change` |
| `vw_trade_balance_by_chapter` | 2 | `fct_trade_detail` | Month, reporter and chapter | `exports_eur`, `imports_eur`, `balance_eur`, and the chapter's section |
| `vw_intra_extra_by_country` | 3 | `fct_trade_totals` | Month, reporter and flow | `intra_eu_eur`, `extra_eu_eur`, `intra_eu_share` |
| `vw_intra_extra_by_chapter` | 3 | `fct_trade_detail` | Month, reporter, chapter and flow | The same, by chapter |
| `vw_partner_dependence` | 4 | `fct_trade_detail` | Month, reporter, heading and flow, over every procedure | `value_eur`, `top_partner_code` and `top_partner_name`, `top_partner_eur`, `top_partner_share`, `q_codes_eur`, `q_codes_share`, `share_of_reporter_flow`, and the heading's description |
| `vw_value_per_kg_by_partner` | 5 | `fct_trade_detail` | Month, product, flow and partner, over every reporter and procedure | `value_eur`, `quantity_kg`, `value_per_kg`, the product's description and the partner's `is_intra_eu` |

- **Only partners that are a country are ranked**, with `RANK()`, so tied
  values share a rank: 45 rows of the full month of 202605 share theirs. The Q
  codes count in the flow's total, so a share is of the whole flow; the 156
  rows of Q codes in the full month have no rank.
- **The rank of the month before is the calendar month's**, joined on
  `ADD_MONTHS(period_date, -1)`. Where that month is not loaded, or the partner
  had no trade in it, it is NULL. `LAG` would take the latest month the partner
  traded in, however far back.
- **The section is the chapter's**, from `dim_product`, not the section the
  fact row reads, which is 04 in some `99XXXXXX` rows of chapter 99, section
  XX.
- **Every ratio goes through `try_divide`.** Spark runs in ANSI mode, where a
  division by zero stops the query, and a chapter whose rows carry no value has
  no share.
- **The views name their tables without a schema.** The notebook sets the gold
  schema with `USE` before it creates them, and a view resolves its tables in
  the schema it was created in: read from `default`, `gold.vw_partner_ranking`
  returns its rows. The tests register facts and dimensions as temporary views
  under the same names, and create the views as temporary views over them.
- The views read every month of the facts, not only the pending ones, and
  write nothing: a month written again in the facts is in the views at the
  next read.
- **The fact rows each view reads are written again in `src/gold_views.py`**,
  as a query of their month and value, apart from the view: the whole fact for
  questions 1 to 3, the rows of a product with a heading for question 4, and
  for question 5 the rows of an eight-digit code with a partner that is a
  country, a value and a weight. The notebook compares each view with them, so
  a view that loses, adds or splits rows stops it even where it does not read
  its whole fact.
- **The largest partner of a heading is a country with a value.** A Q code is
  never named and counts in the heading's total; tied values name the lower
  code, as in 22 headings of the full month. No partner is named where every
  partner is a Q code, in 55 headings, or where the heading has no value, in 13.
- **A sum of sums is `DECIMAL(38,3)`, and Spark divides one to 6 decimals.**
  Divided by a single sum, `DECIMAL(28,3)`, a share keeps 10. At 6, a heading
  at 0.99996% of its reporter's flow would read 0.010000 and pass the floor of
  1%. No heading of the full month falls in that band, and the view of question
  4 casts its sums of sums back to `DECIMAL(28,3)`, so the answer does not
  depend on it. The test of Austria's share of its flow found it: 0.167306 at 6
  decimals, 4.6e-7 off the exact share.
- **The value per kilogram divides the sums.** Austria's and France's boneless
  beef to the United Kingdom, 39,572 EUR over 3,111 kg, is 12.72 EUR per
  kilogram, not 12.86, the mean of their 10.45 and 15.26.
- **The answer of question 5 orders on the quotient Spark computes, to 6
  decimals, and shows it to 2.** Two products of the full month, at 1.9956 and
  1.9980, show 2.00: counted on the column shown, 1,173 products are at twice
  or more, and on the quotient, 1,171.
- **The floors of questions 4 and 5 live in their queries**, 1% of the
  reporter's flow and 10 M EUR. The views keep every heading and every partner,
  so a report can apply another.

Rows of the full month of 202605 as published on 2026-09-15:

| View | Rows |
|---|---:|
| `vw_partner_ranking` | 9,522 |
| `vw_trade_balance_by_chapter` | 2,617 |
| `vw_intra_extra_by_country` | 54 |
| `vw_intra_extra_by_chapter` | 5,193 |
| `vw_partner_dependence` | 59,926 |
| `vw_value_per_kg_by_partner` | 913,169 |

- Each view keeps the value of the fact rows it reads in every month, to the
  thousandth of a euro. In the full month, 1,156,578,315,179.473 EUR over the
  totals and 1,156,578,379,285.260 over the detail: the difference is the
  64,105.787 EUR of Detail against totals. Question 4 reads
  1,126,738,271,858.565, the detail without the 29,840,107,426.695 EUR of the X
  codes of a chapter, and question 5 reads 1,097,396,230,664.621.
- The answers of the full month of 202605, the only month loaded, so its
  previous ranks are NULL:

| Question | Answer |
|---|---|
| 1 | Germany imports first from the Netherlands, 16,755.8 M EUR or 14.7% of its imports, then China, 7.6%, and Poland, 7.0%. It exports first to the United States, 9.7%, then the Netherlands, 7.6%, and France, 7.4%. Germany is the first partner of 14 reporters' imports and 16 reporters' exports |
| 2 | 10 of the 27 reporters are in surplus. Germany's +15,832.5 M EUR comes first from chapter 87, vehicles, +7,890.7, against chapter 27, mineral fuels, −6,903.0. Chapter 27 is the largest deficit of 18 reporters |
| 3 | The intra-EU share of a reporter's imports goes from Slovenia's 35.5% to Luxembourg's 88.5%, and Germany's is 64.5%. Over every reporter, by chapter, it goes from works of art, 9.1%, and ships, 16.7%, to cork, 94.7% |
| 4 | Croatia imports its crude petroleum from Guyana and Malta its electricity from Italy, 100% each, and Slovakia its gas from Russia, 97.7%. Germany depends most on refined petroleum from the Netherlands, 63.5%, and on batteries from China, 45.9%. Slovenia's medicaments, 40.1% of its exports, go 87.8% to Switzerland. The largest share under the Q codes in the answer is 6.3%, in Estonia's exports of heading 2707 |
| 5 | 2,585 products have two partners of at least 10 M EUR, and 266 of them a gap of ten times or more; the median is 1.85. The widest: precious-metal ores exported to Italy at 93,208.42 EUR per kilogram, against 1.76 to China; lactams from Ireland at 22,507.07, against 8.41 from China; motor spirit to Singapore at 912.67, against 0.89 to Angola. Unroasted coffee, over 17 partners: Viet Nam 3.73 against Guatemala 7.02, 1.88 times |

Over the full months of 202601 to 202606, 202605 from the publication of
2026-09-15 and the rest from that of 2026-10-16, rows per view and month:

| View | 202601 | 202602 | 202603 | 202604 | 202605 | 202606 |
|---|---:|---:|---:|---:|---:|---:|
| `vw_partner_ranking` | 9,411 | 9,512 | 9,526 | 9,517 | 9,522 | 9,572 |
| `vw_trade_balance_by_chapter` | 2,618 | 2,618 | 2,618 | 2,618 | 2,617 | 2,618 |
| `vw_intra_extra_by_country` | 54 | 54 | 54 | 54 | 54 | 54 |
| `vw_intra_extra_by_chapter` | 5,184 | 5,184 | 5,201 | 5,199 | 5,193 | 5,194 |
| `vw_partner_dependence` | 59,731 | 59,816 | 60,207 | 60,067 | 59,926 | 60,077 |
| `vw_value_per_kg_by_partner` | 898,603 | 918,938 | 933,619 | 925,145 | 913,169 | 930,690 |

The value the views keep, in EUR, over the totals and over the detail:

| Month | Totals | Detail |
|---|---:|---:|
| 202601 | 1,035,771,288,823.854 | 1,035,771,351,966.217 |
| 202602 | 1,084,718,636,909.279 | 1,084,718,698,828.342 |
| 202603 | 1,255,751,966,698.189 | 1,255,752,033,408.576 |
| 202604 | 1,198,669,988,829.698 | 1,198,670,054,153.606 |
| 202605 | 1,156,578,315,179.473 | 1,156,578,379,285.260 |
| 202606 | 1,259,002,999,612.932 | 1,259,003,067,621.601 |

- **202605 keeps every row and value of the run of 202605 alone.** A month
  loaded next to it changes only the rank of the month before in question 1,
  never which rows a view holds.
- The answers are those of 202606, the latest month loaded: 162, 27, 27, 97,
  162 and 2,854 rows. With 202607 loaded, they are those of July: 162, 27, 27,
  97, 162 and 2,749 rows, the ones the README reports in Results.

### Detail against totals

Per reporter, trade type, flow, procedure and file, summed over every partner
and product:

| File | Groups | Gap other than 0 | Beyond 0.01 M EUR | Sum of the gaps, EUR |
|---|---:|---:|---:|---:|
| `full_v2_202601.csv` | 239 | 109 | 2 | 63,142.363 |
| `full_v2_202602.csv` | 240 | 107 | 2 | 61,919.063 |
| `full_v2_202603.csv` | 242 | 105 | 2 | 66,710.387 |
| `full_v2_202604.csv` | 240 | 107 | 2 | 65,323.908 |
| `full_v2_202605.csv` | 239 | 106 | 2 | 64,105.787 |
| `full_v2_202606.csv` | 241 | 103 | 2 | 68,008.669 |
| `full_v2_202605.csv`, 2026-10-16 | 239 | 104 | 2 | 64,163.133 |
| `full_v2_202607.csv`, 2026-10-16 | 240 | 106 | 2 | 66,547.619 |

- **In the full month the largest gap is 25,139 EUR**, France's intra-EU
  exports. It is not a rounding of conversions. The six reporters outside the
  euro area add up to 51.787 EUR, with at most 30.606 EUR in one group. Ten
  euro-area reporters hold 64,054 EUR, in whole euros, with the detail always
  above the totals: France 38,813, Belgium 10,518, Spain 7,399, Slovenia 2,017,
  Portugal 1,365, Ireland 1,270, Estonia 1,132, Luxembourg 761, Bulgaria 753
  and Italy 26. No documented cause explains it. The manual never mentions
  rounding, and says of `TOTAL` only that it is the one aggregate the bulk
  files precalculate (p. 83). The one exception it describes, confidential
  trade allocated to no chapter but kept in the country's total (p. 42), would
  put the totals above the detail, not below.
- **The same two groups pass the tolerance in every full month**, and no other
  does: France's intra-EU imports, 12,381 to 15,452 EUR above the totals, and
  exports, 24,144 to 26,751, in each of 202601 to 202607, over two
  publications. The gap is systematic, not a one-off of 202605. The sums in the
  table were counted from the CSV files with pandas, on the publication of
  2026-09-15 for the first row of 202605, and of 2026-10-16 for the other
  months. The last two rows, both of 2026-10-16, come from the gold facts
  through `sql/results_queries.sql`, which gives the five rows of 202601 to
  202604 and 202606 to the thousandth of a euro.

### The checks of gold

The checks of gold write to `ops.dq_check_results` with `layer` gold, and are
compared with their expectations by the rule of the silver checks, through the
same `mismatches()` (Silver, The checks). A check of a fact writes one row per
fact and source file. A check of a dimension reads every version of the table,
whatever file its facts came from, and writes one row per dimension, with
`source_file` NULL.

Expected failures: the rule of every month, `EXPECTED` in `src/gold_checks.py`,
and 0 for any check it does not name.

| Check | Rule | Reads | Expected |
|---|---|---|---:|
| `01_rows_match_silver` | Per file, as many fact rows as silver rows. Counts the rows apart | each fact and its silver table | 0 |
| `03_one_match_per_dimension` | Each fact row finds exactly one row in each of its dimensions: the reporter and the partner in `dim_country`, the product, the flow, the procedure and the month | each fact and its dimensions | 0 |
| `04_value_matches_silver` | Per file, the `value_eur` of the fact adds up to silver's. Counts the one sum | each fact and its silver table | 0 |
| `05_dimension_key_unique` | No two rows share the surrogate key or `(code, valid_from)`; in `dim_flow`, `dim_stat_procedure` and `dim_period`, the code | the five dimensions | 0 |
| `06_no_overlapping_versions` | No version of a code starts before the one before it ends | `dim_country`, `dim_product` | 0 |
| `07_detail_matches_totals` | Per reporter, trade type, flow, procedure and file, summed over every partner and product, the detail within 0.01 M EUR of the totals | both facts | 2 |
| `08_version_covers_month` | The version each key of a fact row points to, reporter, partner and product, covers the row's `period_date`. Reads every month of the facts | each fact, `dim_period` and its versioned dimensions | 0 |

- **Check 03 is the strict form of "the star keeps the rows".** Counted row by
  row, a row the star loses and another it counts twice cannot cancel out, as
  they could in a total. A key the fact could not resolve to a version is
  NULL and finds no row, and it overlaps check 05 on purpose: a repeated key
  finds two. Number 02 is not reused: a check of its own for NULL keys
  repeated this one.
- **Checks 05 and 06 read the dimension tables as merged**, with every version
  an earlier MERGE left, since a version missing from the source is kept
  (Dimensions with history). Built from the code lists, versions run
  between consecutive dates and cannot overlap. In the table they can: a row
  of a code list that changed its start would be inserted as a new version
  next to the old one, and check 06 counts the old one.
- **Check 07 compares the detail with the totals of each group**, over every
  partner and product. In the full month of 202605 the two groups beyond 0.01 M
  EUR are France's intra-EU exports and imports, 25,139 and 13,508 EUR above
  the totals, and they are the two it finds in every month (Detail against
  totals).
- **Check 08 reads every month of the facts**, not only the files of the run,
  and writes a row per fact and file. A run builds the facts of its pending
  months against the dimensions as merged then, and leaves the other months as
  they were: a later `MERGE` that starts a version later, or ends it earlier,
  leaves their rows pointing to a version that no longer covers their month,
  with the key still found, so check 03 passes. A key without a version counts
  here too.
- **An expectation names a check**, as in silver, and holds for every file and
  table the check reads. A check of a dimension writes its result without a
  file, and expects 0.
- No check starts a Python worker: every rule is a column expression.

#### What the checks found

On the full month of 202605 as published on 2026-09-15, every check of gold
found exactly its expected failures: 16 results, 1 with failures.

| Check | Full month of 202605 |
|---|---:|
| `01` and `03` | 0 of 4,975,304 detail rows and of 12,709 totals rows |
| `04` | 0 of 1 sum per fact |
| `05` | 0 of 390, 30,985, 2, 7 and 1 rows |
| `06` | 0 of 390 and 30,985 versions |
| `07` | 2 of 239 groups |

- Over the full months of 202601 to 202606, 61 results, 6 with failures, every
  one as expected: check 07 found France's two groups in each month, over 239
  to 242 groups, and the other checks nothing.
- In the incremental run, check 07 found France's two groups again in 202605 as
  republished, of 239, and in 202607, of 240; check 08 found nothing in any
  month, over the six and then the seven.
- With the expectations of check 07 and of check 06 changed, a run wrote its
  results and stopped naming both: the file for check 07, and the two versioned
  dimensions, without a file, for check 06.

## Dimensions with history (SCD type 2)

<details open>
<summary><b><code>dim_country</code> and <code>dim_product</code> keep one row
per version of a code, 390 and 30,985, and a fact row joins the version valid
in its month.</b> Bulgaria's trade of July 2026 is euro-area trade, and its
trade of 2025 is not.</summary>

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

<details>
<summary>A version is a span between the dates of any source, inside a row of
the code's own list, and its key, <code>xxhash64(code, valid_from)</code>,
never moves.</summary>

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
  version of each dimension (The checks of gold).

</details>

<details>
<summary>The load is a MERGE written by hand that updates and inserts, and
never deletes: 2026 brought 1 and 1 in <code>dim_country</code>, 14 and 27 in
<code>dim_product</code>.</summary>

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

</details>

</details>

## The incremental run

The notebooks process the months of the window that are **pending**: landed
from a publication the pipeline has not processed. `ops.etl_watermark`
records, per file, the publication it processed:

| Column | Holds |
|---|---|
| `source_file` | The landed CSV |
| `period` | Its month |
| `published_at`, `archive_bytes` | The publication of its archive, as the Volume's manifest recorded it (Landing and the manifest) |
| `batch_id`, `processed_at` | The run of the gold checks that passed on it |

A month is pending when the manifest dates its archive differently from the
watermark, or the watermark does not hold its file: a month new to the
pipeline, or one republished since it was processed. `src/watermark.py` works
it out from the manifest, the watermark and the window of the run (The window
of a run), and each notebook from 01 to 05 asks it before writing anything and
processes the pending months only: bronze, silver and both facts replace those
months whole, and the checks read their files. A month of the window the
manifest does not name stops the run, since its publication is unknown.

**The gold checks notebook writes the watermark last**, and only when every
result of both layers matched its expectation. Until then every notebook finds
the same pending months, so a run that stops leaves them pending, and the next
one writes them again in place.

With nothing pending, each notebook says so and calls
`dbutils.notebook.exit()`, which ends its task with success. The views notebook
does not read the watermark: it creates its views over every month.
`dbutils.notebook.exit()` ends a Run all at the cell that calls it. That cell
shows only `Notebook exited:` and the message, not what it printed before it,
and every later cell is marked skipped while it still shows its output of the
run before. A skipped cell is read by its mark, not by its output.

Check 08 of gold belongs to the incremental run: it reads every month of the
facts, since those of the months not processed were built against an earlier
`MERGE` (The checks of gold).

Three runs over the full months on serverless, from the six of 202601 to
202606 with 202605 in its publication of 2026-09-15:

| Run | Months | Result |
|---|---|---|
| Republished | 202601 to 202606, the Volume holding 202605 of 2026-10-16 | Pending: 202605. Bronze replaced its 4,988,013 rows of 2026-09-15 with the 4,994,361 of 2026-10-16 in one `WRITE`, 30,203,295 in total; silver and the facts, 4,981,646 and 12,715. Every check as expected, check 10 against the copy of the figures of 2026-10-16, and check 08 over the six months; both `MERGE` at 0 updated and 0 inserted. The watermark changed in 202605 alone. Of the 36 lines of the views, the 30 of the other months stayed identical, byte for byte; those of 202605 moved, the total of its trade by 294.2 M EUR |
| New month | 202601 to 202607 | Pending: 202607. 5,050,155 rows in bronze, 35,253,450 in total; 5,037,372 and 12,783 in silver and the facts, 7 rows in `dim_period`. Every check as expected; the watermark, 7 rows. The views, 42 lines, the 36 of the run before unchanged |
| Nothing pending | The same | Notebooks 01 to 05 end without writing. No table gains a version: the last commit of each of the twelve tables they write is the one of the run before |

Each count of the two files was compared with a count made first without
Spark, with the rules of the checks, which gave every known number of 202606
before theirs: 12,715 and 12,783 `TOTAL` rows; 06a 274,225 and 283,190;
check 10, 0 of 6; and gold 07, France's 2 groups in each.

The Job of this repository then landed 202607 again from Eurostat and took it
through every layer, with the same numbers (The Job).

## Environment

The pipeline runs on Databricks. What runs outside it, the deploy of the Job,
the tests, type checking and lint, needs Python 3.12. On Windows, check which
interpreter `python` and the `py` launcher resolve to before creating the
virtual environment: they can differ, and an environment built with the wrong
one fails far from the cause.

```
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
```

`requirements.txt` is a lock: every package, dependencies included, at one
version, so every install gets the same versions. It pins versions and not
the hashes of their files, so it reads as one line per package, followed by
what needs it. It is the one lock of the repository: `requests` and `py7zr` for the landing, `databricks-sdk` for the
deploy, `pyspark` and `delta-spark` as the engine of the tests, `pytest`,
`mypy` with `types-requests`, and `ruff`. The range each direct dependency may
move within, and why, is in `requirements.in`. A version changes by editing it
and compiling the lock again with pip-tools:

```
pip-compile requirements.in
```

The lock was compiled and installed with Python 3.12 on Windows. pip-compile
resolves for the machine it runs on, so a dependency that only some platforms
need is pinned without its marker: `colorama`, which pytest needs on Windows
only, and `psutil`, which `py7zr` needs everywhere but Cygwin. Both install
everywhere, unused where they are not needed.

The transformation logic was developed and verified on a local Spark 4.2 with
Delta 4.4, to spend no compute quota of Free Edition. That setup is not part
of this repository: here Spark runs locally only as the engine of the tests
(Tests and lint).

### Databricks serverless

The notebooks run on serverless environment version 6. A notebook there
reports:

| | |
|---|---|
| `DATABRICKS_RUNTIME_VERSION` | `client.6.3` |
| Python | 3.12.3 |
| Spark | 4.2.0, through Spark Connect: the session is a `pyspark.sql.connect.session.SparkSession` |

The bronze notebook needs Python 3.11 or later, for `datetime.UTC`; version 1
of the environment has Python 3.10.

The environment is set in the notebook's Environment panel, and Databricks
keeps it in the notebook's source, as a header after the first line:

```
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
```

Notebooks 01 to 06 carry that header in the repository, so a notebook opened
from a clone runs on version 6 without anyone setting the panel, and the
header Databricks writes matches it. `00_land` carries none: it runs as a task
of the Job, on the Job's environment `land`, version 6 with `py7zr` (The Job).
Databricks also saves the notebook without its final newline, which the
repository's copy keeps, so in a Git folder the notebook still shows as
modified though no code changed. A pull stashes that change, and putting it
back conflicts where the pulled commits touch the same lines, as a change to
the last cell does. Resolving the conflict keeps the part of the stash that
merged without one, and the notebook no longer equals the commit. Discard the
change before pulling; if a pull conflicts, abort it, read the diff, discard
it and pull again.

#### The window of a run

A run takes its months from two parameters, `start_period` and `end_period`,
as `YYYYMM`. Notebooks 00 to 05 read them from widgets of those names, created
in a cell of their own, `Run settings`, right after the imports, because
Databricks never shows a widget created by a command that fails:
`start_period` defaults to `202601`, and `end_period` to empty. The views
notebook creates none, since it reads every month of the facts. A Job fills the
widgets from job parameters of the same names, which reach every task (The
Job). `config.window_from()` reads them, and a value that is not a month stops
the run, naming its parameter; so does a window that ends before it starts.

| Notebooks | Window | With `end_period` empty |
|---|---|---|
| `00_land` | The months of the listing, from `start_period` to `end_period` | To the month before the last one listed (The source) |
| `01` to `05` | The months of the Volume's manifest, from `start_period` to `end_period` | To the last month the manifest names |

`config.months_in_window()` reads the months from the names of the monthly
archives, `full_v2_YYYYMM.7z`, and skips the yearly ones and every other file
of the folder. The same function reads the listing and the manifest. It lives
in `config`, not in `land`, because importing `land` imports `py7zr`, which
only the landing task installs. A window that holds no month stops the landing
before any download.

Each notebook prints its window, as `Window: from 202601 to the month before
the last one listed`, so a run says which months it took. An open window lets
a month enter on its own: the next release of Eurostat completes the month
that was the newest, the listing gains a newer one, and the landing lands the
first.

#### Silver, gold and the views on serverless

The notebooks run on serverless through Spark Connect, and the tests on
classic Spark. What runs differently there:

- **Spark Connect refuses a `withColumns()` of no column.** Its client asserts
  that the list is not empty, and the error reads `AssertionError` with no
  message; classic Spark returns the DataFrame unchanged. `read_code_list()`
  turns `valid_from` and `valid_to` into dates and skips the call for a list
  without them, as `FLOW.txt` and the two lists of the CN sections. The tests
  run on classic Spark and cannot see it.
- **Spark reads a workspace file by its path.** The seeds, in
  `/Workspace/.../reference/`, read from serverless with or without a `file:`
  prefix, though the documentation asks for one.
- **The MERGE goes through `DeltaTable` on Spark Connect**, and its `execute()`
  returns the counts of updated and inserted rows, as on classic Spark. Run
  again with nothing to change, it writes no file (Dimensions with history).
  `spark.catalog.tableExists()` takes the name of three parts.
- **A view resolves its tables from any schema.** Created after `USE
  eu_trade.gold`, `eu_trade.gold.vw_partner_ranking` read from the default
  schema returns its rows.

#### The full months on serverless

The notebooks 01 to 06 ran on serverless over the full month of 202605 as
published on 2026-09-15, into empty schemas:

| | Rows |
|---|---|
| Bronze | 4,988,013 from one file, read by `read_landed_csv()`, with 0 nulls |
| Silver | 4,975,304 in `trade_detail` and 12,709 in `trade_totals` |
| Silver checks | Each with its expected failures, 06a within its range. Check 10 compared the six figures of the copy landed with the file |
| Gold | `dim_country` 390 versions and `dim_product` 30,985, inserted into the empty tables; 2, 7 and 1 in the small dimensions; the facts, 4,975,304 and 12,709 |
| Gold checks | 16 results, 1 with failures: check 07, 2 of 239 groups |
| Views | The six lines of rows and value per view, and the answers. Read from the default schema, `eu_trade.gold.vw_partner_ranking` returns its 9,522 rows |

Over the six full months of 202601 to 202606, 202605 from the publication of
2026-09-15 and the rest from that of 2026-10-16, into empty schemas, with the
copy of the published figures of each month in the Volume:

| | Rows |
|---|---|
| Bronze | 30,196,947 from six files, each its data rows, with 0 nulls |
| Silver | 30,120,644 in `trade_detail` and 76,303 in `trade_totals` |
| Silver checks | Each with its expected failures, 06a within its range in each month |
| Gold | `dim_country` 390 versions and `dim_product` 30,985, inserted into the empty tables; 2, 7 and 6 in the small dimensions; the facts, as silver |
| Gold checks | 61 results, 6 with failures: check 07, 2 groups in each month |
| Views | The 36 lines of rows and value per view and month that the notebook prints, and the answers of 202606 |

#### The Job

`jobs/trade_pipeline_job.json` defines the Job `eu_trade_pipeline` as the Jobs
API takes it: seven notebook tasks in a chain, from `land` to `gold_views`,
each started when the one before it succeeds, since each layer reads the one
before. One task runs at a time, so the limit of 5 concurrent tasks of Free
Edition is never reached.

| Setting | Value | Why |
|---|---|---|
| Source | `git_source`: the branch `main` of the GitHub repository, every task with `source: GIT` | Each run takes a snapshot of the branch and records its commit on every task. No Git folder has to be pulled first |
| Parameters | `start_period`, `202601`, and `end_period`, empty | A job parameter reaches every task as the widget of the same name (The window of a run). Run now with different parameters changes them for one run |
| Environment | `land`: environment version 6 with `py7zr==1.1.4`, the version of the lock, for the task `land` only | A serverless notebook task can take an environment of the Job. The other tasks run on the environment of their notebook's header, and none of them needs `py7zr` |
| Compute | None declared | Serverless |
| Performance | `performance_target: STANDARD` | The same SKU as performance optimized: fewer DBU, a longer start |
| Concurrent runs | 1 | Two runs at once would find the same pending months, since the gold checks write the watermark last |
| Schedule | Monthly, on the 20th at 06:00 Europe/Brussels, paused | How it would run in production, after each monthly release. Paused, it spends no quota on its own, and a run starts by hand or through the API |

**The landing is the first task.** It downloads from `ec.europa.eu` on the
serverless compute of the Job, which reaches the internet on Free Edition, and
writes to the Volume through the driver's disk (Landing and the manifest). A
run therefore processes what it landed itself, with no step outside the
workspace.

`src/deploy_job.py` creates the Job from the file, or replaces the settings of
the one of the same name, so the workspace runs what the repository holds. Two
Jobs of that name stop it. Creating or replacing a Job starts no compute. It
authenticates through the Databricks SDK, and needs a personal access token
with the scope `jobs`, here in a profile of its own:

```
$env:DATABRICKS_CONFIG_PROFILE = 'jobs'
.venv\Scripts\python src\deploy_job.py
```

Read back from the workspace, the Job held every value the file sets, the
empty default of `end_period` included, and the server's own defaults besides:
`queue` enabled, `format`, timeouts of 0, empty notifications, and `run_if` and
`disabled` on each task. Run again, the script replaced the settings and left
one Job of that name.

Two runs over the seven full months of 202601 to 202607:

| Run | Result |
|---|---|
| `end_period` 202607, with the CSV of 202607 deleted from the Volume and its row from the watermark | The seven tasks succeeded in 13 minutes. The landing landed 202607 again from Eurostat (Landing and the manifest), and the notebooks took it as pending: 5,050,155 rows in bronze and 35,253,450 in total; 5,037,372 and 12,783 in silver and the facts; 26 silver results, 06a at 283,190 and check 10 at 0 of 6; both `MERGE` at 0 updated and 0 inserted, and 7 rows in `dim_period`; 30 gold results, check 07 at 2 of 240 groups and check 08 over the seven months; the watermark back at 7 rows. The 42 lines of the views identical, byte for byte, to those of the incremental run, and the answers with 162, 27, 27, 97, 162 and 2,749 rows |
| The defaults | The seven tasks succeeded in 8 minutes. The landing left 202608 out and landed nothing; notebooks 01 to 05 took their window to the last month landed and found nothing pending, in 10 to 17 seconds each; the views printed the same 42 lines, byte for byte |

What runs from a remote repository as from a Git folder:

- **The working directory is the notebook's folder in the checkout.** Each task
  imported `src/` through `../src`.
- **Spark reads the files of the checkout**: the seeds of `reference/`, read
  by the silver checks and the gold notebook.
- **Each task runs on its own environment**: the bronze notebook imported
  `datetime.UTC`, which version 1 lacks, from the header of its notebook, and
  the landing imported `py7zr`, from the environment of the Job.

Only the first task of a run waits for compute, and the landing also installs
its environment: it took 287 seconds in the first run, 41 of them landing
202607, and 252 in the second, which landed nothing.

## Tests and lint

The tests run with pytest from the repository root, with the lock installed:

```
.venv\Scripts\python -m pytest -m "not spark"
.venv\Scripts\python -m pytest
```

245 tests. The 130 marked `spark` start a local Spark session with Delta, which
needs a JDK 17 and `JAVA_HOME`; the other 115 start no JVM, and
`-m "not spark"` runs them wherever the lock is installed. Spark is the engine
of the tests of the transformation logic, not a second way to run the
pipeline. `tests/conftest.py` builds one session per test module, `local[2]`
with the Delta extension and catalog through `configure_spark_with_delta_pip`,
and stops it at the end of the module. The JVM outlives the sessions: PySpark
starts one for the whole run, with the first session, so its heap holds the
whole suite. The session gives the driver 4 GB of heap: with Spark's default
of 1 GB, the gold modules, which keep their dimensions cached, run out of
memory.

It also sets Spark's memory pages to 2 MB. Spark sizes a page from the heap
and the cores, 64 MB here, and every hash table and broadcast takes a whole
page, however few its rows. Measured after a full GC at the end of each test,
on a local run with the same JDK, Spark, Delta and heap as the CI:

| Pages | Live heap at its peak, in the views | Spark's storage memory there | `long[]` at the end of `test_gold.py` |
|---|---|---|---|
| 64 MB | 3,129 MB of 4,096 | 2,178 MB, at its ceiling | 2,181 MB |
| 2 MB | 1,249 MB | 161 MB | 75 MB |

With 64 MB pages the broadcasts filled Spark's storage memory, and the views
ran close enough to the 4 GB to run out of heap building a broadcast. With
2 MB pages the margin is 2.8 GB. What is left at the peak is mostly query
plans and byte arrays, the same with either page size.

The CI, `.github/workflows/ci.yml`, runs on every push to `main` and every pull
request, on Ubuntu with Python 3.12 and a Temurin JDK 17. It installs the lock,
runs ruff, `ruff format --check` and mypy, and then the
whole suite, which takes 8 to 10 minutes there, against 12 to 18 with 64 MB
pages.

On Windows the first Spark session of the tests can hang while the executor,
which runs in the same process, downloads the Delta jars from the driver. The
CI runs them on Ubuntu.

| File | What it covers |
|---|---|
| `tests/test_config.py` | The window read from its two parameters: a malformed month refused with its parameter's name, an open window reaching the last month, a closed one stopping at its end and reading no yearly archive, a window past the last month empty, and one that ends before it starts refused. A table named in the catalog, and a month naming its archive and its CSV. Starts no JVM |
| `tests/test_land.py` | The access policy: which answers are retried, the waits between attempts, `Retry-After`, the request for no content encoding. The atomic download, a transfer dropped mid-body started again and given up after five, the archive layout, a member above the size ceiling, the published names and URLs, and the skip of a file already there. The copy of the published figures: the parsing of the answer, in its own order of partners, and no copy for a month not published. The listing, over real rows: size and time read, a page without files refused. The manifest read back as written, and the status of each month. Over a Volume that is a folder of the test: a new month landed and recorded, a current one not downloaded again, a republished one replaced, and one without figures yet losing its old copy; a download of another size than listed not landed; the window landing only its months, an open window leaving the newest month out and one that names it landing it, an empty window stopping the run before any download; every code list downloaded over the one the Volume holds |
| `tests/test_watermark.py` | The pending months: none where the watermark holds the landed publication, a month landed again and a month new to it, and a month the manifest does not name stopping the run. The CSV of each month in the Volume. The rows of the watermark once months are recorded: theirs replaced, the others kept. Starts no JVM |
| `tests/test_job.py` | The definition of the Job: every key a setting the SDK knows, which it would otherwise drop; a task per notebook, in their order, each depending on the one before; one run at a time, and no task declaring a cluster; only the landing installing `py7zr`, at the version of the lock; the monthly schedule paused; the two parameters the window; and each notebook from 00 to 05 creating its widgets before it reads them. Starts no JVM |
| `tests/test_deploy_job.py` | The Job created where the workspace has none of its name, its settings replaced where it has one, and two of its name stopping the run, against a fake of the Jobs API |
| `tests/test_bronze.py` | The bronze reader on a local Spark: what it refuses and what it lets through |
| `tests/test_silver.py` | The silver transformation on a local Spark, over real rows of the landed files read with the bronze reader: the totals apart, the HS levels of each kind of code, `NO_SU` and the zeros that stay, exact decimals, the fourth decimal rounded, a value that is not a number failing, the columns and types of each table. And that a DataFrame built from a Python list reads, which needs `PYSPARK_PYTHON` |
| `tests/test_silver_checks.py` | The quality checks on a local Spark. Every check over real rows of the landed files fails only where the data does, in 06a; each one, alone, counts a real row broken on purpose and lets the row as landed pass. With real lines of the code lists: CRLF endings, a double quote in a description, a row with a field missing, codes valid only before the month. The copies of the published figures: a match, a figure 0.076 M EUR off, a figure without a counterpart, a file without a copy not evaluated |
| `tests/test_silver_expectations.py` | The expected failures and the comparison: every expectation naming a check; the rule of every month, and check 10 not evaluated for a file landed without a copy; a rate turned into a range rounded inward, and into 0 to 0 over nothing checked; a count above or below its expectation, at either end of its range or beyond it, NULL against a number either way, a check without a result and a name that is not a check. Starts no JVM |
| `tests/test_gold.py` | The gold model on a local Spark, over real rows of the landed files and real lines of the code lists. The versions: Bulgaria's three, Germany's two rows of the same name kept, QY in zone 5900 and not a country, the United Kingdom leaving the EU; an X code that stops at its chapter, an eight-digit code that reaches every level, a parent row that adds no version, the codes that close and open with 2026. The dimensions as of 2025-12-31, built by the helper `as_of()`, and the MERGE: a first load, the changes of 2026, the same MERGE again, a repeated key refused, a version missing from the source counted. The keys of the facts: one fact row per silver row, Bulgaria's euro-area version in May 2026, a code the dimension does not hold left NULL, the columns and types. Its Delta tables are written by path below the test's folder, never to a metastore |
| `tests/test_gold_checks.py` | The quality checks of gold on a local Spark, over real rows of the landed files and real lines of the code lists, built into facts and dimensions by `src/gold.py`. Every check over clean inputs fails nothing. Each check, alone, counts a fact or a dimension broken on purpose: a fact row twice or missing; a code without a version in its month; a flow, a version or a month that a dimension lacks, and a version it holds twice; a value a tenth of a cent off; a key twice in three ways; a version one day over the next; a group within the tolerance, exactly at it, beyond it, and without its TOTAL row; a version started after the month or ended before it once the facts were built, a product closed before the month, and check 08 reading every month of the facts |
| `tests/test_gold_expectations.py` | The expected failures of gold, and their comparison by the rule of silver: check 07 at 2 in every month, check 08 at 0, a check of a dimension expected at 0 without a file, a dimension off its expectation named without a file, a check without a result, and a name of silver that is not a check of gold. Starts no JVM |
| `tests/test_gold_views.py` | The views of the business questions on a local Spark, over real rows of the landed files and real lines of the code lists, built into facts and dimensions by `src/gold.py` and registered as temporary views. The six views of the file; a partner's procedures in one row; the Q codes in the flow's total and without a rank; two partners of one value sharing a rank; the rank of the calendar month before, and none where that month is not loaded; exports minus imports by chapter, an X code in its chapter; the section of the chapter, not of the row; QY intra-EU and QZ extra-EU; no share without a value; the largest partner of a heading a country, its share over the whole flow with the Q codes, and none where every partner is a Q code or the heading has no value; X codes of a chapter out of the headings and in the reporter's flow; a tie naming the lower code; reporters summed into one row, and the value per kilogram dividing sums; question 5 reading only eight-digit codes with a partner that is a country, a value and a weight; every view keeping the value of the fact rows it reads, once per grain |

The CSV files of the tests are written by the tests, from `EXPECTED_HEADER` and
three rows of `full_v2_202605.csv` as published on 2026-09-15, with one defect
each or none. The bronze reader takes them so:

| Case | Bronze reader |
|---|---|
| Good | 3 rows, 0 nulls |
| `DECLARANT` in place of `REPORTER` | Fails: "CSV header does not conform to the schema" |
| Header in lowercase | **Loads 3 rows** |
| A field too many | Fails: `MALFORMED_CSV_RECORD` |
| A field too few | Fails: `MALFORMED_CSV_RECORD` |
| An empty field | **Loads 3 rows, 1 null**, which the bronze null check catches |

The lowercase header loads because Spark resolves column names without regard
to case, and every later step resolves them the same way. The reader fails on
the first action, not on the read: Spark reads lazily.

The tests were shown able to fail. With one fault injected at a time, exactly
the tests that cover it failed, and no other:
`enforceSchema` set to true in the bronze reader, and these, each with the
tests it failed:

| Fault | Failed |
|---|---|
| The pending months without comparing the publication | The pending months, both cases |
| Two Jobs of the same name not stopping the run | Two of its name stopping the run |
| The Job always created | Its settings replaced where the workspace has one |
| The rows of the watermark without the other files | The months recorded and the rest kept |
| The comparison without the range | Every count within, at, or outside its range: without the range it is compared with a count of NULL |
| The range rounded to the nearest row, not inward | A rate turned into a range |
| Check 08 over the facts of the run only | Check 08 reading every month of the facts |
| The HS levels without the check for digits | `28XXXXXX` and `2833XXXX`. `283321XX` has digits at every level it reaches, so it passes either way |
| `NO_SU` kept | `NO_SU` to NULL |
| `try_cast` in place of `cast` | The value that is not a number |
| The totals with every column | The columns and types |
| The valid codes of a list paired with every date, not only those of their validity | The codes valid only before the month, in checks 01, 02 and 03; GB as an EU partner in check 11, a member of zone 1010 until 2020-01-31; and the clean rows, through check 11 |
| The grain of the detail without the five classifications | The X code split by SITC |
| The comparison in one direction | The failure that shrank, and NULL against a number either way |
| The MERGE updating every matched version, without its condition | The changes of 2026 and the same MERGE again, in both dimensions; the version missing from the source |
| The keys looked up by code alone, without the date | One fact row per silver row; Bulgaria's euro-area version; Germany and the United Kingdom resolved once; the code without a version |
| The MERGE without its check of repeated keys | The repeated key refused |
| The euro area seed left out of the versions | Bulgaria's three versions, Germany's, the unique keys, the first load, the changes of 2026 in `dim_country` and Bulgaria's euro-area version. Also the version missing from the source and the repeated key, which count the rows of the table |
| The detail and the totals joined inner, not full outer | The group without its TOTAL row |
| An overlap that leaves out the day two versions share | The version one day over the next |
| Only a fact row that finds no row in a dimension counted, not one that finds two | The version twice in its dimension |
| The keys of `dim_country` unique on the surrogate key alone | The same version under another surrogate key |
| Every partner ranked, the Q codes included | The Q codes without a rank |
| The flow's total over the countries only | The Q codes without a rank, through the total and the share |
| The month before as any earlier month | No previous rank where that month is not loaded |
| `ROW_NUMBER()` in place of `RANK()` | Two partners of one value sharing a rank |
| The section the fact row reads | The section of the chapter |
| A plain division in place of `try_divide` | No share without a value |
| The balance as imports minus exports | Exports minus imports by chapter |
| The intra-EU columns read from trade type E | QY intra-EU and QZ extra-EU, and every view keeping the value of the rows it reads |
| The largest partner allowed to be a Q code | The largest partner a country; none where every partner is a Q code; a tie naming the lower code, whose rows hold QY above both countries |
| The share of the largest partner over the countries only | The largest partner a country, and a tie naming the lower code, through the share |
| The X codes of a chapter in the headings | X codes of a chapter out of the headings, and every view keeping the value of the rows it reads |
| The reporter's flow over the headings only | X codes of a chapter in the reporter's flow |
| A tie naming the higher code | A tie naming the lower code |
| A partner without a value named | None where the heading has no value |
| The mean of the rows' value per kilogram | Reporters summed, the value per kilogram dividing sums |
| The value per kilogram by reporter too | Reporters summed, and every view, through its grain |
| X codes, partners that are not countries, or rows without a weight in question 5 | Question 5 reading only eight-digit codes with a country, a value and a weight, and every view keeping the value of the rows it reads |
| Rows without a value in question 5 | Question 5 reading only eight-digit codes with a country, a value and a weight, alone: a value of 0 adds nothing to the value of the rows it reads |

The tests of the landing, of the window and of the definition of the Job were
not run against injected faults.

Not covered by any test: the check cells of the bronze, silver, gold and views
notebooks, which live in the notebooks; the queries of
`sql/analysis_queries.sql`, whose answers on the landed files were compared
with numbers measured from the files without Spark, and those of
`sql/results_queries.sql`; the writing of the quality results, whose
comparison goes through the tested `mismatches()`, and of the watermark, whose
rows go through the tested `watermark_rows()`; and the real Eurostat server and
workspace, which no test contacts: the landing is tested against a Volume that
is a folder.

Lint, format and types, from the repository root:

```
.venv\Scripts\ruff check .
.venv\Scripts\ruff format --check .
.venv\Scripts\python -m mypy src notebooks
```

`pyproject.toml` holds the settings of pytest and ruff, and nothing else: the
repository is not a package. Its ruff section names the rule families on top
of ruff's defaults and says why the others are left out. mypy resolves the notebooks' imports from `src/` because
`src/` is among the folders it checks.
