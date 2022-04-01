/*
Calculates funnel stats
*/

WITH agg_sessions_events AS (
    SELECT *
    FROM {{ ref('agg_sessions_events') }}
)

,unioned AS (
    SELECT
        'Level 1: page_view, add_to_cart, checkout' AS level
        ,SUM(funnel_page_view_add_to_cart_checkout) AS number_of_sessions
    FROM agg_sessions_events
    
    UNION ALL

    SELECT
        'Level 2: add_to_cart, checkout' AS level
        ,SUM(funnel_add_to_cart_checkout) AS number_of_sessions
    FROM agg_sessions_events

    UNION ALL

    SELECT
        'Level 3: checkout' AS level
        ,SUM(funnel_checkout) AS number_of_sessions
    FROM agg_sessions_events
)

,final AS (
    SELECT
        *
        ,CAST(100.0 * number_of_sessions / MAX(number_of_sessions) OVER() AS DECIMAL(5,2)) AS percent_of_level_one
    FROM unioned
)

SELECT * FROM final