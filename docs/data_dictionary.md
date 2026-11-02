# Data Dictionary

Every column of the tables and views the pipeline writes, with its type as the tables hold it.
Names follow the source: each COMEXT column becomes its name in snake_case, `REPORTER` becomes
`reporter` and `VALUE_EUR` becomes `value_eur`. Why each column is typed as it is, and what each
sentinel becomes, is in [`engineering-notes.md`](engineering-notes.md), Silver.

## Bronze

### `bronze.comext_trade`

One row per data row of a landed file, as landed. Partitioned by `PERIOD`.

| Column | Type | Holds |
|---|---|---|
| `REPORTER` | string | The member state that declares the trade, a two-letter code |
| `PARTNER` | string | The partner code: a country, or a code of `reference/partner_special_codes.csv` |
| `TRADE_TYPE` | string | `I` intra-EU, `E` extra-EU |
| `PRODUCT_NC` | string | The Combined Nomenclature code: eight digits, a code ending in X, or `TOTAL` |
| `PRODUCT_SITC`, `PRODUCT_CPA21`, `PRODUCT_CPA22`, `PRODUCT_BEC`, `PRODUCT_BEC5` | string | The parallel classifications of the row |
| `PRODUCT_SECTION` | string | The CN section the row reads, `TO` in a `TOTAL` row |
| `FLOW` | string | `1` import, `2` export |
| `STAT_PROCEDURE` | string | The statistical procedure, such as `2` inward processing |
| `SUPPL_UNIT` | string | The supplementary unit, or `NO_SU` |
| `PERIOD` | string | The month, `YYYYMM` |
| `VALUE_EUR` | string | The value in euros, up to three decimals |
| `VALUE_NAC` | string | The value in the reporter's currency |
| `QUANTITY_KG` | string | The net mass in kilograms |
| `QUANTITY_SUPPL_UNIT` | string | The quantity in the supplementary unit |
| `_ingested_at` | timestamp | When the bronze run started, in UTC |
| `_source_file` | string | The landed CSV the row came from, such as `full_v2_202607.csv` |
| `_batch_id` | string | The bronze run, a UUID |

## Silver

### `silver.trade_detail`

One row per row by product, without XI or XU. Grain: `reporter`, `partner`, `product_nc`, the five
parallel classifications, `flow`, `stat_procedure`, `period`. Partitioned by `period`.

| Column | Type | Holds |
|---|---|---|
| `reporter`, `partner`, `trade_type` | string | As in bronze |
| `product_nc` | string | Eight digits, or a code ending in X |
| `hs2`, `hs4`, `hs6` | string | The first 2, 4 or 6 characters of `product_nc` when they are all digits; NULL below the level an X code keeps |
| `is_x_code` | boolean | `product_nc` is digits followed by X |
| `product_sitc`, `product_cpa21`, `product_cpa22`, `product_bec`, `product_bec5`, `product_section` | string | As in bronze |
| `flow`, `stat_procedure`, `period` | string | As in bronze. A code is text, not a number |
| `period_date` | date | The first day of `period`, which picks the valid row of each code list |
| `suppl_unit` | string | The supplementary unit; NULL where bronze reads `NO_SU` |
| `value_eur`, `value_nac` | decimal(18,3) | The two values, exact to the thousandth |
| `quantity_kg` | bigint | The net mass; a 0 stays 0, and check 06a counts it |
| `quantity_suppl_unit` | bigint | The quantity in the unit; NULL with `NO_SU` |
| `_source_file` | string | From bronze |
| `_batch_id` | string | The silver run |

### `silver.trade_totals`

One row per `TOTAL` row of the source, without XI or XU. Grain: `reporter`, `partner`, `flow`,
`stat_procedure`, `period`. Partitioned by `period`. A `TOTAL` row reads `TOTAL` in every
classification, `NO_SU` and a quantity of 0, so those columns are left out.

| Column | Type | Holds |
|---|---|---|
| `reporter`, `partner`, `trade_type`, `flow`, `stat_procedure`, `period` | string | As in the detail |
| `period_date` | date | As in the detail |
| `value_eur`, `value_nac` | decimal(18,3) | The total of the combination |
| `quantity_kg` | bigint | The total mass |
| `_source_file`, `_batch_id` | string | As in the detail |

## Gold

### `gold.fct_trade_detail`

One row per row of `silver.trade_detail`, with the codes of reporter, partner and product replaced
by the key of the version that covers `period`. Read by questions 2 to 5.

| Column | Type | Holds |
|---|---|---|
| `reporter_key`, `partner_key` | bigint | `dim_country.country_key` |
| `product_key` | bigint | `dim_product.product_key` |
| `product_sitc`, `product_cpa21`, `product_cpa22`, `product_bec`, `product_bec5` | string | The parallel classifications, as codes: they describe the row, not the product |
| `flow` | string | `dim_flow.flow` |
| `stat_procedure` | string | `dim_stat_procedure.stat_procedure` |
| `period` | string | `dim_period.period` |
| `trade_type`, `product_section`, `suppl_unit` | string | As in silver |
| `value_eur`, `value_nac` | decimal(18,3) | As in silver |
| `quantity_kg`, `quantity_suppl_unit` | bigint | As in silver |
| `_source_file`, `_batch_id` | string | The landed file, and the gold run |

### `gold.fct_trade_totals`

One row per row of `silver.trade_totals`. Its `TOTAL` rows name the real partner where the rows by
product book it under QY or QZ, so questions 1 and 3 by country read it.

| Column | Type | Holds |
|---|---|---|
| `reporter_key`, `partner_key` | bigint | `dim_country.country_key` |
| `flow`, `stat_procedure`, `period` | string | As in the detail fact |
| `trade_type` | string | As in silver |
| `value_eur`, `value_nac` | decimal(18,3) | As in silver |
| `quantity_kg` | bigint | As in silver |
| `_source_file`, `_batch_id` | string | The landed file, and the gold run |

### `gold.dim_country`

One row per version of a partner code, 390 rows. Loaded by MERGE on `(country_code, valid_from)`.

| Column | Type | Holds |
|---|---|---|
| `country_key` | bigint | `xxhash64(country_code, valid_from)` |
| `country_code` | string | The code, from `PARTNERS_ISO.txt` |
| `country_name` | string | Its name in that version |
| `is_country` | boolean | False for the 12 codes of `reference/partner_special_codes.csv` |
| `partner_category` | string | `country`, or the seed's category: `uk_split`, `high_seas`, `stores_and_provisions`, `not_specified`, `confidential` |
| `is_intra_eu` | boolean | The code belongs to zone 1010 of `GEOZONES-PARTNERS_ISO.txt`: the member states and the intra-EU codes QR, QV and QY |
| `is_euro_area` | boolean | From `reference/euro_area_members.csv` |
| `region_zone`, `region` | string | The continent zone, 5100 Europe to 5900 not specified, and its name |
| `valid_from`, `valid_to` | date | The span of the version; `2500-12-31` means still open |
| `is_current` | boolean | `valid_to` is `2500-12-31` |

### `gold.dim_product`

One row per version of an eight-character code of `CN.txt`, 30,985 rows, X codes included. Loaded
by MERGE on `(product_nc, valid_from)`.

| Column | Type | Holds |
|---|---|---|
| `product_key` | bigint | `xxhash64(product_nc, valid_from)` |
| `product_nc` | string | The code |
| `description` | string | Its description in that version |
| `is_x_code` | boolean | As in silver |
| `hs6`, `hs4`, `hs2` | string | The levels above the code, NULL below the level an X code keeps |
| `hs6_description`, `hs4_description`, `hs2_description` | string | Their descriptions |
| `section_code`, `section_description` | string | The section of the chapter, from `CN_SECTIONS-CHAPTERS.txt` |
| `valid_from`, `valid_to`, `is_current` | date, date, boolean | As in `dim_country` |

### `gold.dim_flow`, `gold.dim_stat_procedure`, `gold.dim_period`

Rebuilt by overwrite on each run.

| Table | Column | Type | Holds |
|---|---|---|---|
| `dim_flow` | `flow`, `flow_name` | string | `1` Import, `2` Export |
| `dim_stat_procedure` | `stat_procedure`, `description` | string | The 7 codes of `STATISTICAL_PROCEDURES.txt` |
| `dim_stat_procedure` | `valid_from`, `valid_to` | date | The span of the code |
| `dim_period` | `period` | string | The month, `YYYYMM`, one row per month of silver |
| `dim_period` | `period_date` | date | Its first day |
| `dim_period` | `year`, `quarter`, `month` | int | Its parts |

### The views

One per business question, over every month of the facts. Values in EUR, shares from 0 to 1.

| View | Question | One row per | Columns besides the codes and names of its grain |
|---|---|---|---|
| `vw_partner_ranking` | 1 | Month, reporter, flow and partner | `is_country`, `value_eur`, `flow_total_eur`, `share_of_flow`, `partner_rank`, `previous_month_rank`, `rank_change` |
| `vw_trade_balance_by_chapter` | 2 | Month, reporter and chapter | `exports_eur`, `imports_eur`, `balance_eur`, and the chapter's section |
| `vw_intra_extra_by_country` | 3 | Month, reporter and flow | `intra_eu_eur`, `extra_eu_eur`, `intra_eu_share` |
| `vw_intra_extra_by_chapter` | 3 | Month, reporter, chapter and flow | The same, with the chapter's section |
| `vw_partner_dependence` | 4 | Month, reporter, heading and flow | `value_eur`, `top_partner_code`, `top_partner_name`, `top_partner_eur`, `top_partner_share`, `q_codes_eur`, `q_codes_share`, `share_of_reporter_flow` |
| `vw_value_per_kg_by_partner` | 5 | Month, product, flow and partner | `is_intra_eu`, `value_eur`, `quantity_kg`, `value_per_kg` |

Sums are `decimal(28,3)`, a balance `decimal(29,3)` and the total of a flow `decimal(38,3)`; shares
are `decimal(38,10)`, and `value_per_kg` is `decimal(38,13)`. A rank is an `int`, and NULL for a
partner that is not a country.

## Operations

### `ops.dq_check_results`

One row per check, table and file of each run of the checks, appended to.

| Column | Type | Holds |
|---|---|---|
| `check_name` | string | The number of the check and what it asks, such as `06a_kg_zero_with_value` |
| `layer` | string | `silver` or `gold` |
| `table_name` | string | The table the check reads |
| `source_file` | string | The landed file, or NULL for a check of a gold dimension |
| `batch_id`, `run_at` | string, timestamp | The run of the checks |
| `rows_checked` | bigint | What the check read: rows, groups or published figures |
| `rows_failed` | bigint | How many broke the rule, or NULL if the check could not evaluate the file |
| `expected_failures` | bigint | The count expected, or NULL where a range is expected or nothing is evaluated |
| `expected_min`, `expected_max` | bigint | The range expected, both ends included, or NULL where a count is |
| `severity` | string | `error` for every check |

### `ops.etl_watermark`

One row per landed file the pipeline processed, written by the gold checks only when every result
of both layers matched its expectation.

| Column | Type | Holds |
|---|---|---|
| `source_file` | string | The landed CSV |
| `period` | string | Its month |
| `published_at` | string | The publication time of its archive, `YYYY-MM-DDTHH:MM:SS`, as the Volume's manifest recorded it |
| `archive_bytes` | bigint | The size of its archive |
| `batch_id`, `processed_at` | string, timestamp | The run of the gold checks that passed on it |
