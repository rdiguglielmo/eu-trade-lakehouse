-- The views that answer the business questions, over the gold star schema.
--
-- The facts and the dimensions are named without a schema: the notebook
-- 06_gold_views sets the gold schema first, and a view resolves its tables in
-- the schema it was created in. Every ratio goes through try_divide: Spark
-- runs in ANSI mode, where a division by zero stops the query. The rules, and
-- what each view returned on the landed files, are in
-- docs/engineering-notes.md, section Gold.

-- Question 1: the main partners of each reporter, and how the ranking changed.
-- From the TOTAL rows, which name the real partner: one row per month,
-- reporter, flow and partner, over every procedure. Only partners that are a
-- country are ranked, and tied values share a rank; the Q codes count in the
-- flow's total and take no rank. previous_month_rank is the rank in the
-- calendar month before, NULL where the partner had no trade that month or the
-- month is not loaded. rank_change is positive when the partner moved up.
CREATE OR REPLACE VIEW vw_partner_ranking AS
WITH by_partner AS (
    SELECT
        f.period,
        d.period_date,
        r.country_code AS reporter_code,
        r.country_name AS reporter_name,
        f.flow,
        fl.flow_name,
        p.country_code AS partner_code,
        p.country_name AS partner_name,
        p.is_country,
        SUM(f.value_eur) AS value_eur
    FROM fct_trade_totals AS f
    JOIN dim_period AS d ON f.period = d.period
    JOIN dim_country AS r ON f.reporter_key = r.country_key
    JOIN dim_country AS p ON f.partner_key = p.country_key
    JOIN dim_flow AS fl ON f.flow = fl.flow
    GROUP BY
        f.period,
        d.period_date,
        r.country_code,
        r.country_name,
        f.flow,
        fl.flow_name,
        p.country_code,
        p.country_name,
        p.is_country
),
ranked AS (
    SELECT
        *,
        SUM(value_eur) OVER (PARTITION BY period, reporter_code, flow) AS flow_total_eur,
        CASE
            WHEN is_country THEN RANK() OVER (
                PARTITION BY period, reporter_code, flow, is_country
                ORDER BY value_eur DESC
            )
        END AS partner_rank
    FROM by_partner
)
SELECT
    cur.period,
    cur.reporter_code,
    cur.reporter_name,
    cur.flow,
    cur.flow_name,
    cur.partner_code,
    cur.partner_name,
    cur.is_country,
    cur.value_eur,
    cur.flow_total_eur,
    try_divide(cur.value_eur, cur.flow_total_eur) AS share_of_flow,
    cur.partner_rank,
    prev.partner_rank AS previous_month_rank,
    prev.partner_rank - cur.partner_rank AS rank_change
FROM ranked AS cur
LEFT JOIN ranked AS prev
    ON prev.reporter_code = cur.reporter_code
    AND prev.flow = cur.flow
    AND prev.partner_code = cur.partner_code
    AND prev.period_date = ADD_MONTHS(cur.period_date, -1);

-- Question 2: the products behind each reporter's surplus or deficit.
-- From the rows by product, over every partner and trade type: exports minus
-- imports by chapter. The section is the chapter's, from dim_product, not the
-- section the fact row reads, which differs in some 99XXXXXX rows.
CREATE OR REPLACE VIEW vw_trade_balance_by_chapter AS
SELECT
    f.period,
    r.country_code AS reporter_code,
    r.country_name AS reporter_name,
    pr.section_code,
    pr.section_description,
    pr.hs2 AS chapter,
    pr.hs2_description AS chapter_description,
    SUM(CASE WHEN f.flow = '2' THEN f.value_eur ELSE 0 END) AS exports_eur,
    SUM(CASE WHEN f.flow = '1' THEN f.value_eur ELSE 0 END) AS imports_eur,
    SUM(CASE WHEN f.flow = '2' THEN f.value_eur ELSE 0 END)
    - SUM(CASE WHEN f.flow = '1' THEN f.value_eur ELSE 0 END) AS balance_eur
FROM fct_trade_detail AS f
JOIN dim_country AS r ON f.reporter_key = r.country_key
JOIN dim_product AS pr ON f.product_key = pr.product_key
GROUP BY
    f.period,
    r.country_code,
    r.country_name,
    pr.section_code,
    pr.section_description,
    pr.hs2,
    pr.hs2_description;

-- Question 3, by country: intra-EU against extra-EU trade of each reporter.
-- From the TOTAL rows, by their trade type, which survives confidentiality:
-- QY is intra-EU and QZ extra-EU.
CREATE OR REPLACE VIEW vw_intra_extra_by_country AS
SELECT
    f.period,
    r.country_code AS reporter_code,
    r.country_name AS reporter_name,
    f.flow,
    fl.flow_name,
    SUM(CASE WHEN f.trade_type = 'I' THEN f.value_eur ELSE 0 END) AS intra_eu_eur,
    SUM(CASE WHEN f.trade_type = 'E' THEN f.value_eur ELSE 0 END) AS extra_eu_eur,
    try_divide(
        SUM(CASE WHEN f.trade_type = 'I' THEN f.value_eur ELSE 0 END),
        SUM(f.value_eur)
    ) AS intra_eu_share
FROM fct_trade_totals AS f
JOIN dim_country AS r ON f.reporter_key = r.country_key
JOIN dim_flow AS fl ON f.flow = fl.flow
GROUP BY
    f.period,
    r.country_code,
    r.country_name,
    f.flow,
    fl.flow_name;

-- Question 3, by product: the same comparison by chapter, from the rows by
-- product. A chapter whose rows carry no value has no share.
CREATE OR REPLACE VIEW vw_intra_extra_by_chapter AS
SELECT
    f.period,
    r.country_code AS reporter_code,
    r.country_name AS reporter_name,
    pr.section_code,
    pr.section_description,
    pr.hs2 AS chapter,
    pr.hs2_description AS chapter_description,
    f.flow,
    fl.flow_name,
    SUM(CASE WHEN f.trade_type = 'I' THEN f.value_eur ELSE 0 END) AS intra_eu_eur,
    SUM(CASE WHEN f.trade_type = 'E' THEN f.value_eur ELSE 0 END) AS extra_eu_eur,
    try_divide(
        SUM(CASE WHEN f.trade_type = 'I' THEN f.value_eur ELSE 0 END),
        SUM(f.value_eur)
    ) AS intra_eu_share
FROM fct_trade_detail AS f
JOIN dim_country AS r ON f.reporter_key = r.country_key
JOIN dim_product AS pr ON f.product_key = pr.product_key
JOIN dim_flow AS fl ON f.flow = fl.flow
GROUP BY
    f.period,
    r.country_code,
    r.country_name,
    pr.section_code,
    pr.section_description,
    pr.hs2,
    pr.hs2_description,
    f.flow,
    fl.flow_name;

-- Question 4: dependence on a single partner. From the rows by product: one
-- row per month, reporter, heading (HS4) and flow, over every procedure and
-- trade type. The largest partner that is a country and has a value, and its
-- share of the whole flow, Q codes included; tied values name the lower code.
-- The Q codes may hide a larger partner, so their share stands next to it and
-- the partner's share is a lower bound. An X code of a chapter reaches no
-- heading and stays out, but share_of_reporter_flow divides the heading by the
-- reporter's flow over every product, those X codes included.
CREATE OR REPLACE VIEW vw_partner_dependence AS
WITH by_partner AS (
    SELECT
        f.period,
        r.country_code AS reporter_code,
        r.country_name AS reporter_name,
        pr.hs4 AS heading,
        pr.hs4_description AS heading_description,
        f.flow,
        fl.flow_name,
        p.country_code AS partner_code,
        p.country_name AS partner_name,
        p.is_country,
        SUM(f.value_eur) AS value_eur
    FROM fct_trade_detail AS f
    JOIN dim_country AS r ON f.reporter_key = r.country_key
    JOIN dim_country AS p ON f.partner_key = p.country_key
    JOIN dim_product AS pr ON f.product_key = pr.product_key
    JOIN dim_flow AS fl ON f.flow = fl.flow
    WHERE pr.hs4 IS NOT NULL
    GROUP BY
        f.period,
        r.country_code,
        r.country_name,
        pr.hs4,
        pr.hs4_description,
        f.flow,
        fl.flow_name,
        p.country_code,
        p.country_name,
        p.is_country
),
-- A sum of sums is DECIMAL(38,3), and Spark divides one to 6 decimals, which
-- can carry a heading just under 1% over it. Cast back to the DECIMAL(28,3) of
-- a single sum, every share below keeps 10.
by_heading AS (
    SELECT
        period,
        reporter_code,
        reporter_name,
        heading,
        heading_description,
        flow,
        flow_name,
        CAST(SUM(value_eur) AS DECIMAL(28, 3)) AS value_eur,
        CAST(SUM(CASE WHEN is_country THEN 0 ELSE value_eur END) AS DECIMAL(28, 3))
            AS q_codes_eur
    FROM by_partner
    GROUP BY
        period,
        reporter_code,
        reporter_name,
        heading,
        heading_description,
        flow,
        flow_name
),
top_partner AS (
    SELECT
        period,
        reporter_code,
        heading,
        flow,
        partner_code,
        partner_name,
        value_eur,
        ROW_NUMBER() OVER (
            PARTITION BY period, reporter_code, heading, flow
            ORDER BY value_eur DESC, partner_code
        ) AS position
    FROM by_partner
    WHERE is_country AND value_eur > 0
),
reporter_flow AS (
    SELECT
        f.period,
        r.country_code AS reporter_code,
        f.flow,
        SUM(f.value_eur) AS value_eur
    FROM fct_trade_detail AS f
    JOIN dim_country AS r ON f.reporter_key = r.country_key
    GROUP BY f.period, r.country_code, f.flow
)
SELECT
    h.period,
    h.reporter_code,
    h.reporter_name,
    h.heading,
    h.heading_description,
    h.flow,
    h.flow_name,
    h.value_eur,
    t.partner_code AS top_partner_code,
    t.partner_name AS top_partner_name,
    t.value_eur AS top_partner_eur,
    try_divide(t.value_eur, h.value_eur) AS top_partner_share,
    h.q_codes_eur,
    try_divide(h.q_codes_eur, h.value_eur) AS q_codes_share,
    try_divide(h.value_eur, rf.value_eur) AS share_of_reporter_flow
FROM by_heading AS h
LEFT JOIN top_partner AS t
    ON t.period = h.period
    AND t.reporter_code = h.reporter_code
    AND t.heading = h.heading
    AND t.flow = h.flow
    AND t.position = 1
JOIN reporter_flow AS rf
    ON rf.period = h.period
    AND rf.reporter_code = h.reporter_code
    AND rf.flow = h.flow;

-- Question 5: the value per kilogram of each partner for the same product, in
-- the EU as a whole. From the rows by product: one row per month, product,
-- flow and partner, over every reporter, procedure and trade type. Only
-- eight-digit codes, a partner that is a country, and rows with a value and a
-- weight: an X code is no single product, a Q code no partner, and a row
-- without a value or a weight has no price. The value per kilogram divides the
-- sums, never averages the rows' ratios.
CREATE OR REPLACE VIEW vw_value_per_kg_by_partner AS
SELECT
    f.period,
    pr.product_nc,
    pr.description AS product_description,
    f.flow,
    fl.flow_name,
    p.country_code AS partner_code,
    p.country_name AS partner_name,
    p.is_intra_eu,
    SUM(f.value_eur) AS value_eur,
    SUM(f.quantity_kg) AS quantity_kg,
    try_divide(SUM(f.value_eur), SUM(f.quantity_kg)) AS value_per_kg
FROM fct_trade_detail AS f
JOIN dim_country AS p ON f.partner_key = p.country_key
JOIN dim_product AS pr ON f.product_key = pr.product_key
JOIN dim_flow AS fl ON f.flow = fl.flow
WHERE NOT pr.is_x_code
    AND p.is_country
    AND f.value_eur > 0
    AND f.quantity_kg > 0
GROUP BY
    f.period,
    pr.product_nc,
    pr.description,
    f.flow,
    fl.flow_name,
    p.country_code,
    p.country_name,
    p.is_intra_eu;
