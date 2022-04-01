/*
Aggregate events data by session
*/

WITH fact_events AS (
    SELECT *
    FROM {{ ref('fact_events') }}
)

,dim_products AS (
    SELECT *
    FROM {{ ref('dim_products') }}
)

,int_session_duration AS (
    SELECT *
    FROM {{ ref('int_session_duration') }}
)

,agg AS (
    SELECT
        fact_events.session_id
        ,COUNT(DISTINCT fact_events.event_id) AS number_of_events
        ,SUM(CASE WHEN fact_events.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS number_of_events_add_to_cart
        ,SUM(CASE WHEN fact_events.event_type = 'checkout' THEN 1 ELSE 0 END) AS number_of_events_checkout
        ,SUM(CASE WHEN fact_events.event_type = 'page_view' THEN 1 ELSE 0 END) AS number_of_events_page_view
        ,SUM(CASE WHEN fact_events.event_type = 'package_shipped' THEN 1 ELSE 0 END) AS number_of_events_package_shipped
        ,STRING_AGG(DISTINCT dim_products.product_name, ', ') AS products
    FROM fact_events
    LEFT JOIN dim_products -- unique in dim_products, no dupes
        ON fact_events.product_id = dim_products.product_id
    GROUP BY 1
)

,funnel AS (
    SELECT
        agg.*
        ,CASE
            WHEN number_of_events_page_view > 0
                OR number_of_events_add_to_cart > 0
                OR number_of_events_checkout > 0 THEN 1
            ELSE 0
        END AS funnel_page_view_add_to_cart_checkout
        ,CASE
            WHEN number_of_events_add_to_cart > 0
                OR number_of_events_checkout > 0 THEN 1
            ELSE 0
        END AS funnel_add_to_cart_checkout
        ,CASE
            WHEN number_of_events_checkout > 0 THEN 1
            ELSE 0
        END AS funnel_checkout
    FROM agg
)

,joined AS (
    SELECT
        funnel.session_id
        ,funnel.number_of_events
        ,funnel.number_of_events_add_to_cart
        ,funnel.number_of_events_checkout
        ,funnel.number_of_events_page_view
        ,funnel.number_of_events_package_shipped
        ,funnel.products
        ,funnel.funnel_page_view_add_to_cart_checkout
        ,funnel.funnel_add_to_cart_checkout
        ,funnel.funnel_checkout
        ,int_session_duration.session_duration
    FROM funnel
    LEFT JOIN int_session_duration -- unique in session_id, no dupes
        ON funnel.session_id = int_session_duration.session_id
)

SELECT * FROM joined