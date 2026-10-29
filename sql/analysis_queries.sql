-- The answer to each business question for the latest month loaded, from the
-- views of gold_views.sql. Each query opens with its question. Values in
-- million EUR and shares in percent, both to one decimal, rounded half up.

-- Question 1: the three main partners of each reporter and flow.
-- With their rank the month before and in the first month loaded, which is
-- how the ranking changed over the months loaded.
WITH months AS (
    SELECT MIN(period) AS first_period, MAX(period) AS latest_period
    FROM vw_partner_ranking
)
SELECT
    latest_month.reporter_code,
    latest_month.reporter_name,
    latest_month.flow_name,
    latest_month.partner_rank,
    latest_month.partner_code,
    latest_month.partner_name,
    ROUND(latest_month.value_eur / 1000000, 1) AS value_m_eur,
    ROUND(latest_month.share_of_flow * 100, 1) AS share_pct,
    latest_month.previous_month_rank,
    first_month.partner_rank AS first_month_rank
FROM vw_partner_ranking AS latest_month
JOIN months ON latest_month.period = months.latest_period
LEFT JOIN vw_partner_ranking AS first_month
    ON first_month.period = months.first_period
    AND first_month.reporter_code = latest_month.reporter_code
    AND first_month.flow = latest_month.flow
    AND first_month.partner_code = latest_month.partner_code
WHERE latest_month.partner_rank <= 3
ORDER BY
    latest_month.reporter_code,
    latest_month.flow,
    latest_month.partner_rank,
    latest_month.partner_code;

-- Question 2: the chapters behind each reporter's surplus or deficit.
-- Per reporter, the balance over every chapter, and the chapters with the
-- largest and the smallest balance. A tie goes to the lower chapter code.
WITH latest_month AS (
    SELECT *
    FROM vw_trade_balance_by_chapter
    WHERE period = (SELECT MAX(period) FROM vw_trade_balance_by_chapter)
),
ordered AS (
    SELECT
        *,
        SUM(balance_eur) OVER (PARTITION BY reporter_code) AS reporter_balance_eur,
        ROW_NUMBER() OVER (
            PARTITION BY reporter_code ORDER BY balance_eur DESC, chapter
        ) AS from_largest,
        ROW_NUMBER() OVER (
            PARTITION BY reporter_code ORDER BY balance_eur, chapter
        ) AS from_smallest
    FROM latest_month
)
SELECT
    surplus.reporter_code,
    surplus.reporter_name,
    ROUND(surplus.reporter_balance_eur / 1000000, 1) AS balance_m_eur,
    surplus.chapter AS surplus_chapter,
    surplus.chapter_description AS surplus_chapter_description,
    ROUND(surplus.balance_eur / 1000000, 1) AS surplus_m_eur,
    deficit.chapter AS deficit_chapter,
    deficit.chapter_description AS deficit_chapter_description,
    ROUND(deficit.balance_eur / 1000000, 1) AS deficit_m_eur
FROM ordered AS surplus
JOIN ordered AS deficit
    ON deficit.reporter_code = surplus.reporter_code
    AND deficit.from_smallest = 1
WHERE surplus.from_largest = 1
ORDER BY surplus.reporter_balance_eur DESC, surplus.reporter_code;

-- Question 3, by country: the intra-EU share of each reporter's trade.
SELECT
    reporter_code,
    reporter_name,
    ROUND(MAX(CASE WHEN flow = '1' THEN intra_eu_share END) * 100, 1)
        AS imports_intra_eu_pct,
    ROUND(MAX(CASE WHEN flow = '2' THEN intra_eu_share END) * 100, 1)
        AS exports_intra_eu_pct
FROM vw_intra_extra_by_country
WHERE period = (SELECT MAX(period) FROM vw_intra_extra_by_country)
GROUP BY reporter_code, reporter_name
ORDER BY imports_intra_eu_pct, reporter_code;

-- Question 3, by product: the intra-EU share of every reporter's trade, by chapter.
SELECT
    chapter,
    chapter_description,
    ROUND(
        try_divide(
            SUM(CASE WHEN flow = '1' THEN intra_eu_eur END),
            SUM(CASE WHEN flow = '1' THEN intra_eu_eur + extra_eu_eur END)
        ) * 100,
        1
    ) AS imports_intra_eu_pct,
    ROUND(
        try_divide(
            SUM(CASE WHEN flow = '2' THEN intra_eu_eur END),
            SUM(CASE WHEN flow = '2' THEN intra_eu_eur + extra_eu_eur END)
        ) * 100,
        1
    ) AS exports_intra_eu_pct
FROM vw_intra_extra_by_chapter
WHERE period = (SELECT MAX(period) FROM vw_intra_extra_by_chapter)
GROUP BY chapter, chapter_description
ORDER BY imports_intra_eu_pct, chapter;

-- Question 4: the flows that depend most on a single partner.
-- Per reporter and flow, among the headings that hold at least 1% of the
-- reporter's flow, the three whose largest partner holds the largest share,
-- with the share under the Q codes next to it: the partner's share is a lower
-- bound. A tie goes to the heading of larger value, then to the lower code.
WITH material AS (
    SELECT *
    FROM vw_partner_dependence
    WHERE period = (SELECT MAX(period) FROM vw_partner_dependence)
        AND share_of_reporter_flow >= 0.01
),
ordered AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY reporter_code, flow
            ORDER BY top_partner_share DESC NULLS LAST, value_eur DESC, heading
        ) AS dependence_rank
    FROM material
)
SELECT
    reporter_code,
    reporter_name,
    flow_name,
    dependence_rank,
    heading,
    heading_description,
    ROUND(value_eur / 1000000, 1) AS value_m_eur,
    ROUND(share_of_reporter_flow * 100, 1) AS of_reporter_flow_pct,
    top_partner_code,
    top_partner_name,
    ROUND(top_partner_share * 100, 1) AS top_partner_pct,
    ROUND(q_codes_share * 100, 1) AS q_codes_pct
FROM ordered
WHERE dependence_rank <= 3
ORDER BY reporter_code, flow, dependence_rank;

-- Question 5: where the value per kilogram diverges between partners.
-- For the same product, in the EU as a whole, among the partners of at least
-- 10 M EUR in the month: the cheapest and the dearest partner of every product
-- and flow with two such partners or more, and the dearest's value per
-- kilogram over the cheapest's. A tie goes to the lower partner code.
WITH material AS (
    SELECT *
    FROM vw_value_per_kg_by_partner
    WHERE period = (SELECT MAX(period) FROM vw_value_per_kg_by_partner)
        AND value_eur >= 10000000
),
ordered AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY product_nc, flow) AS partners,
        ROW_NUMBER() OVER (
            PARTITION BY product_nc, flow ORDER BY value_per_kg, partner_code
        ) AS from_cheapest,
        ROW_NUMBER() OVER (
            PARTITION BY product_nc, flow ORDER BY value_per_kg DESC, partner_code
        ) AS from_dearest
    FROM material
)
SELECT
    cheapest.product_nc,
    cheapest.product_description,
    cheapest.flow_name,
    cheapest.partners,
    cheapest.partner_code AS cheapest_partner_code,
    cheapest.partner_name AS cheapest_partner_name,
    ROUND(cheapest.value_per_kg, 2) AS cheapest_eur_per_kg,
    dearest.partner_code AS dearest_partner_code,
    dearest.partner_name AS dearest_partner_name,
    ROUND(dearest.value_per_kg, 2) AS dearest_eur_per_kg,
    ROUND(try_divide(dearest.value_per_kg, cheapest.value_per_kg), 2)
        AS dearest_over_cheapest
FROM ordered AS cheapest
JOIN ordered AS dearest
    ON dearest.product_nc = cheapest.product_nc
    AND dearest.flow = cheapest.flow
    AND dearest.from_dearest = 1
WHERE cheapest.from_cheapest = 1
    AND cheapest.partners >= 2
ORDER BY
    try_divide(dearest.value_per_kg, cheapest.value_per_kg) DESC,
    cheapest.product_nc,
    cheapest.flow;
