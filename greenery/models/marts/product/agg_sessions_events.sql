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

,joined AS (
    SELECT
        agg.session_id
        ,agg.number_of_events
        ,agg.number_of_events_add_to_cart
        ,agg.number_of_events_checkout
        ,agg.number_of_events_page_view
        ,agg.number_of_events_package_shipped
        ,agg.products

        ,int_session_duration.session_duration
    FROM agg
    LEFT JOIN int_session_duration -- unique in session_id, no dupes
        ON agg.session_id = int_session_duration.session_id
)

SELECT * FROM joined