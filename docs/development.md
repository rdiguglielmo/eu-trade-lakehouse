# Development

How the repository is installed, deployed and tested outside Databricks, and
what runs differently on its serverless compute. How the pipeline works is in
[`engineering-notes.md`](engineering-notes.md).

## Contents

- [Environment](#environment)
- [Deploying the Job](#deploying-the-job)
- [Databricks serverless](#databricks-serverless)
- [Tests and lint](#tests-and-lint)

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
version, so every install gets the same versions. It pins versions and not the
hashes of their files, so it reads as one line per package, followed by what
needs it. It is the one lock of the repository: `requests` and `py7zr` for the
landing, `databricks-sdk` for the deploy, `pyspark` and `delta-spark` as the
engine of the tests, `pytest`, `mypy` with `types-requests`, and `ruff`. The
range each direct dependency may move within, and why, is in `requirements.in`.
A version changes by editing it and compiling the lock again with pip-tools:

```
pip-compile requirements.in
```

The lock was compiled and installed with Python 3.12 on Windows. pip-compile
resolves for the machine it runs on, so a dependency that only some platforms
need is pinned without its marker: `colorama`, which pytest needs on Windows
only, and `psutil`, which `py7zr` needs everywhere but Cygwin. Both install
everywhere, unused where they are not needed.

The transformation logic was developed and verified on a local Spark 4.2 with
Delta 4.4, to spend no compute quota of Free Edition. That setup is not part of
this repository: here Spark runs locally only as the engine of the tests (Tests
and lint). Over the seven full months of 202601 to 202607, the views notebook
printed the same 42 lines on serverless as on the local Spark, byte for byte.

<p align="right"><a href="#development">Back to top</a></p>

## Deploying the Job

`src/deploy_job.py` creates the Job from `jobs/trade_pipeline_job.json`, or
replaces the settings of the one of the same name, so the workspace runs what
the repository holds. Two Jobs of that name stop it. Creating or replacing a
Job starts no compute. It authenticates through the Databricks SDK, and needs a
personal access token with the scope `jobs`, here in a profile of its own:

```
$env:DATABRICKS_CONFIG_PROFILE = 'jobs'
.venv\Scripts\python src\deploy_job.py
```

Read back from the workspace, the Job held every value the file sets, the
empty default of `end_period` included, and the server's own defaults besides:
`queue` enabled, `format`, timeouts of 0, empty notifications, and `run_if` and
`disabled` on each task. Run again, the script replaced the settings and left
one Job of that name.

<p align="right"><a href="#development">Back to top</a></p>

## Databricks serverless

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
from a clone runs on version 6 without anyone setting the panel, and the header
Databricks writes matches it. `00_land` carries none: it runs as a task of the
Job, on the Job's environment `land`, version 6 with `py7zr` ([The
Job](engineering-notes.md#the-job)). Databricks also saves the notebook without
its final newline, which the repository's copy keeps, so in a Git folder the
notebook still shows as modified though no code changed. A pull stashes that
change, and putting it back conflicts where the pulled commits touch the same
lines, as a change to the last cell does. Resolving the conflict keeps the part
of the stash that merged without one, and the notebook no longer equals the
commit. Discard the change before pulling; if a pull conflicts, abort it, read
the diff, discard it and pull again.

`dbutils.notebook.exit()` ends a Run all at the cell that calls it. That
cell shows only `Notebook exited:` and the message, not what it printed
before it, and every later cell is marked skipped while it still shows its
output of the run before. A skipped cell is read by its mark, not by its
output.

The Job runs the notebooks from the remote repository ([The
Job](engineering-notes.md#the-job)). What runs from it as from a Git folder:

- **The working directory is the notebook's folder in the checkout.** Each task
  imported `src/` through `../src`.
- **Spark reads the files of the checkout**: the seeds of `reference/`, read
  by the silver checks and the gold notebook.
- **Each task runs on its own environment**: the bronze notebook imported
  `datetime.UTC`, which version 1 lacks, from the header of its notebook, and
  the landing imported `py7zr`, from the environment of the Job.

### Silver, gold and the views on serverless

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
  again with nothing to change, it writes no file ([Dimensions with
  history](engineering-notes.md#dimensions-with-history-scd-type-2)).
  `spark.catalog.tableExists()` takes the name of three parts.
- **A view resolves its tables from any schema.** Created after `USE
  eu_trade.gold`, `eu_trade.gold.vw_partner_ranking` read from the default
  schema returns its rows.

<p align="right"><a href="#development">Back to top</a></p>

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
runs ruff, `ruff format --check` and mypy, and then the whole suite, whose
pytest step takes 7 to 11 minutes there, against 12 to 18 with 64 MB pages.

On Windows the first Spark session of the tests can hang while the executor,
which runs in the same process, downloads the Delta jars from the driver. The
CI runs them on Ubuntu.

<details>
<summary>What each of the 13 test files covers.</summary>

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

</details>

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
the tests that cover it failed, and no other: `enforceSchema` set to true in
the bronze reader, and the 40 faults below.

<details>
<summary>The 40 faults, each with the tests it failed.</summary>

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

</details>

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
repository is not a package. Its ruff section names the rule families on top of
ruff's defaults and says why the others are left out. mypy resolves the
notebooks' imports from `src/` because `src/` is among the folders it checks.

<p align="right"><a href="#development">Back to top</a></p>
