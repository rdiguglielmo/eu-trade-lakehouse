-- The queries behind the README's Results and Limitations that are not the
-- answer of a question itself: counts over an answer, and the rows behind a
-- case it names. They read the views of gold_views.sql and the facts, in the
-- gold schema, for the latest month loaded unless they say otherwise. Values
-- in million EUR and shares in percent, both to one decimal, rounded half up.

-- Question 1, how the ranking changed: the top three partners of each
-- reporter and flow in the latest month, by their rank in the first month
-- loaded.
WITH months AS (
    SELECT MIN(period) AS first_period, MAX(period) AS latest_period
    FROM vw_partner_ranking
),
compared AS (
    SELECT
        latest_month.flow,
        latest_month.flow_name,
        latest_month.partner_rank,
        first_month.partner_rank AS first_month_rank
    FROM vw_partner_ranking AS latest_month
    JOIN months ON latest_month.period = months.latest_period
    LEFT JOIN vw_partner_ranking AS first_month
        ON first_month.period = months.first_period
        AND first_month.reporter_code = latest_month.reporter_code
        AND first_month.flow = latest_month.flow
        AND first_month.partner_code = latest_month.partner_code
    WHERE latest_month.partner_rank <= 3
)
SELECT
    flow_name,
    COUNT(*) AS top_three_rows,
    SUM(CASE WHEN first_month_rank = partner_rank THEN 1 ELSE 0 END) AS same_rank,
    SUM(CASE WHEN first_month_rank <= 3 AND first_month_rank <> partner_rank THEN 1 ELSE 0 END)
        AS other_rank_in_top_three,
    SUM(CASE WHEN first_month_rank > 3 OR first_month_rank IS NULL THEN 1 ELSE 0 END)
        AS new_to_top_three
FROM compared
GROUP BY flow, flow_name
ORDER BY flow;

-- Question 1, the first partners: how many reporters each partner is first
-- for, by flow.
SELECT
    flow_name,
    partner_code,
    partner_name,
    COUNT(*) AS reporters
FROM vw_partner_ranking
WHERE period = (SELECT MAX(period) FROM vw_partner_ranking)
    AND partner_rank = 1
GROUP BY flow, flow_name, partner_code, partner_name
ORDER BY flow, reporters DESC, partner_code;

-- Question 2, surplus and deficit: how many reporters are in surplus, and how
-- many reporters have each chapter as their largest deficit. A tie goes to the
-- lower chapter code, as in the answer.
WITH latest_month AS (
    SELECT *
    FROM vw_trade_balance_by_chapter
    WHERE period = (SELECT MAX(period) FROM vw_trade_balance_by_chapter)
),
reporters AS (
    SELECT reporter_code, SUM(balance_eur) AS balance_eur
    FROM latest_month
    GROUP BY reporter_code
),
largest_deficit AS (
    SELECT
        reporter_code,
        chapter,
        chapter_description,
        ROW_NUMBER() OVER (
            PARTITION BY reporter_code ORDER BY balance_eur, chapter
        ) AS from_smallest
    FROM latest_month
)
SELECT
    d.chapter,
    d.chapter_description,
    COUNT(*) AS reporters_with_it_as_largest_deficit,
    (SELECT COUNT(*) FROM reporters WHERE balance_eur > 0) AS reporters_in_surplus,
    (SELECT COUNT(*) FROM reporters) AS reporters
FROM largest_deficit AS d
WHERE d.from_smallest = 1
GROUP BY d.chapter, d.chapter_description
ORDER BY reporters_with_it_as_largest_deficit DESC, d.chapter;

-- Question 5, the spread of the gaps: over the products and flows with two
-- partners of at least 10 M EUR, how many there are, how many of them have a
-- dearest partner at ten times the cheapest or more, and the median gap. Counted
-- on the quotient, not on the gap rounded to two decimals.
WITH material AS (
    SELECT *
    FROM vw_value_per_kg_by_partner
    WHERE period = (SELECT MAX(period) FROM vw_value_per_kg_by_partner)
        AND value_eur >= 10000000
),
gaps AS (
    SELECT
        product_nc,
        flow,
        COUNT(*) AS partners,
        try_divide(MAX(value_per_kg), MIN(value_per_kg)) AS dearest_over_cheapest
    FROM material
    GROUP BY product_nc, flow
)
SELECT
    COUNT(*) AS products,
    SUM(CASE WHEN dearest_over_cheapest >= 10 THEN 1 ELSE 0 END) AS ten_times_or_more,
    ROUND(PERCENTILE(CAST(dearest_over_cheapest AS DOUBLE), 0.5), 2) AS median_gap
FROM gaps
WHERE partners >= 2;

-- Processing trade: Slovenia's trade with Switzerland by statistical
-- procedure, from the TOTAL rows, which name the real partner. Procedure 2 is
-- inward processing in extra-EU trade.
SELECT
    fl.flow_name,
    f.stat_procedure,
    ROUND(SUM(f.value_eur) / 1000000, 1) AS value_m_eur,
    ROUND(
        try_divide(SUM(f.value_eur), SUM(SUM(f.value_eur)) OVER (PARTITION BY f.flow)) * 100,
        1
    ) AS share_pct
FROM fct_trade_totals AS f
JOIN dim_country AS r ON f.reporter_key = r.country_key
JOIN dim_country AS p ON f.partner_key = p.country_key
JOIN dim_flow AS fl ON f.flow = fl.flow
WHERE f.period = (SELECT MAX(period) FROM fct_trade_totals)
    AND r.country_code = 'SI'
    AND p.country_code = 'CH'
GROUP BY f.flow, fl.flow_name, f.stat_procedure
ORDER BY f.flow, f.stat_procedure;

-- Processing trade: Slovenia's trade with Switzerland by chapter and
-- procedure, from the rows by product, its five largest in each flow.
WITH by_chapter AS (
    SELECT
        f.flow,
        fl.flow_name,
        pr.hs2 AS chapter,
        pr.hs2_description AS chapter_description,
        f.stat_procedure,
        SUM(f.value_eur) AS value_eur
    FROM fct_trade_detail AS f
    JOIN dim_country AS r ON f.reporter_key = r.country_key
    JOIN dim_country AS p ON f.partner_key = p.country_key
    JOIN dim_product AS pr ON f.product_key = pr.product_key
    JOIN dim_flow AS fl ON f.flow = fl.flow
    WHERE f.period = (SELECT MAX(period) FROM fct_trade_detail)
        AND r.country_code = 'SI'
        AND p.country_code = 'CH'
    GROUP BY f.flow, fl.flow_name, pr.hs2, pr.hs2_description, f.stat_procedure
),
ordered AS (
    SELECT
        *,
        SUM(value_eur) OVER (PARTITION BY flow) AS flow_eur,
        ROW_NUMBER() OVER (PARTITION BY flow ORDER BY value_eur DESC, chapter, stat_procedure)
            AS position
    FROM by_chapter
)
SELECT
    flow_name,
    position,
    chapter,
    chapter_description,
    stat_procedure,
    ROUND(value_eur / 1000000, 1) AS value_m_eur,
    ROUND(try_divide(value_eur, flow_eur) * 100, 1) AS share_pct
FROM ordered
WHERE position <= 5
ORDER BY flow, position;

-- Partners far from a reporter's routes: Malta's imports from its three main
-- partners, and Cyprus's exports to its three main partners, by heading and
-- procedure, from the rows by product. The partner's total from the TOTAL rows
-- stands next to it: what the rows by product book under a Q code is missing.
WITH top_partners AS (
    SELECT
        reporter_code,
        flow,
        partner_code,
        partner_rank,
        value_eur AS partner_total_eur
    FROM vw_partner_ranking
    WHERE period = (SELECT MAX(period) FROM vw_partner_ranking)
        AND ((reporter_code = 'MT' AND flow = '1') OR (reporter_code = 'CY' AND flow = '2'))
        AND partner_rank <= 3
),
by_heading AS (
    SELECT
        r.country_code AS reporter_code,
        f.flow,
        fl.flow_name,
        p.country_code AS partner_code,
        p.country_name AS partner_name,
        pr.hs4 AS heading,
        pr.hs4_description AS heading_description,
        f.stat_procedure,
        SUM(f.value_eur) AS value_eur
    FROM fct_trade_detail AS f
    JOIN dim_country AS r ON f.reporter_key = r.country_key
    JOIN dim_country AS p ON f.partner_key = p.country_key
    JOIN dim_product AS pr ON f.product_key = pr.product_key
    JOIN dim_flow AS fl ON f.flow = fl.flow
    WHERE f.period = (SELECT MAX(period) FROM fct_trade_detail)
        AND r.country_code IN ('MT', 'CY')
    GROUP BY
        r.country_code,
        f.flow,
        fl.flow_name,
        p.country_code,
        p.country_name,
        pr.hs4,
        pr.hs4_description,
        f.stat_procedure
),
ordered AS (
    SELECT
        h.*,
        t.partner_rank,
        t.partner_total_eur,
        ROW_NUMBER() OVER (
            PARTITION BY h.reporter_code, h.flow, h.partner_code
            ORDER BY h.value_eur DESC, h.heading, h.stat_procedure
        ) AS position
    FROM by_heading AS h
    JOIN top_partners AS t
        ON t.reporter_code = h.reporter_code
        AND t.flow = h.flow
        AND t.partner_code = h.partner_code
)
SELECT
    reporter_code,
    flow_name,
    partner_rank,
    partner_code,
    partner_name,
    ROUND(partner_total_eur / 1000000, 1) AS partner_total_m_eur,
    position,
    heading,
    heading_description,
    stat_procedure,
    ROUND(value_eur / 1000000, 1) AS value_m_eur,
    ROUND(try_divide(value_eur, partner_total_eur) * 100, 1) AS of_partner_total_pct
FROM ordered
WHERE position <= 3
ORDER BY reporter_code, partner_rank, position;

-- Processing trade, month by month: Slovenia's exports of medicaments
-- (heading 3004) in every month loaded, their largest partner and its share,
-- from the view of question 4.
SELECT
    period,
    ROUND(value_eur / 1000000, 1) AS value_m_eur,
    ROUND(share_of_reporter_flow * 100, 1) AS of_reporter_flow_pct,
    top_partner_code,
    ROUND(top_partner_share * 100, 1) AS top_partner_pct
FROM vw_partner_dependence
WHERE reporter_code = 'SI'
    AND heading = '3004'
    AND flow = '2'
ORDER BY period;

-- Value per kilogram within one code: every partner of at least 10 M EUR of
-- 09011100, coffee neither roasted nor decaffeinated, cheapest first.
SELECT
    flow_name,
    partner_code,
    partner_name,
    ROUND(value_eur / 1000000, 1) AS value_m_eur,
    ROUND(value_per_kg, 2) AS eur_per_kg
FROM vw_value_per_kg_by_partner
WHERE period = (SELECT MAX(period) FROM vw_value_per_kg_by_partner)
    AND product_nc = '09011100'
    AND value_eur >= 10000000
ORDER BY flow, value_per_kg, partner_code;

-- Detail against totals, in every month loaded: per reporter, trade type,
-- flow and procedure, summed over every partner and product, the rows by
-- product less the TOTAL rows. The rule of gold check 07, with its tolerance
-- of 0.01 M EUR.
WITH detail AS (
    SELECT period, reporter_key, trade_type, flow, stat_procedure, SUM(value_eur) AS detail_eur
    FROM fct_trade_detail
    GROUP BY period, reporter_key, trade_type, flow, stat_procedure
),
totals AS (
    SELECT period, reporter_key, trade_type, flow, stat_procedure, SUM(value_eur) AS totals_eur
    FROM fct_trade_totals
    GROUP BY period, reporter_key, trade_type, flow, stat_procedure
),
gaps AS (
    SELECT
        period,
        COALESCE(detail_eur, 0) - COALESCE(totals_eur, 0) AS gap_eur
    FROM detail
    FULL OUTER JOIN totals USING (period, reporter_key, trade_type, flow, stat_procedure)
)
SELECT
    period,
    COUNT(*) AS groups,
    SUM(CASE WHEN gap_eur <> 0 THEN 1 ELSE 0 END) AS gap_other_than_0,
    SUM(CASE WHEN ABS(gap_eur) / 1000000 > 0.01 THEN 1 ELSE 0 END) AS beyond_tolerance,
    SUM(gap_eur) AS sum_of_gaps_eur
FROM gaps
GROUP BY period
ORDER BY period;

-- Detail against totals: the groups beyond the tolerance, in every month.
WITH detail AS (
    SELECT period, reporter_key, trade_type, flow, stat_procedure, SUM(value_eur) AS detail_eur
    FROM fct_trade_detail
    GROUP BY period, reporter_key, trade_type, flow, stat_procedure
),
totals AS (
    SELECT period, reporter_key, trade_type, flow, stat_procedure, SUM(value_eur) AS totals_eur
    FROM fct_trade_totals
    GROUP BY period, reporter_key, trade_type, flow, stat_procedure
),
gaps AS (
    SELECT
        period,
        reporter_key,
        trade_type,
        flow,
        stat_procedure,
        COALESCE(detail_eur, 0) - COALESCE(totals_eur, 0) AS gap_eur
    FROM detail
    FULL OUTER JOIN totals USING (period, reporter_key, trade_type, flow, stat_procedure)
)
SELECT
    g.period,
    r.country_code AS reporter_code,
    g.trade_type,
    fl.flow_name,
    g.stat_procedure,
    g.gap_eur
FROM gaps AS g
JOIN dim_country AS r ON g.reporter_key = r.country_key
JOIN dim_flow AS fl ON g.flow = fl.flow
WHERE ABS(g.gap_eur) / 1000000 > 0.01
ORDER BY g.period, reporter_code, g.trade_type, g.flow;
