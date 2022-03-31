/*
Aggregates sessions and products to determine
- was_viewed => event_type = page_view
- was_purchased => event_type = checkout and joining on order_items to get products
*/

WITH fact_events AS (
    SELECT *
    FROM {{ ref('fact_events') }}
)

,stg_order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
)

,dim_products AS (
    SELECT *
    FROM {{ ref('dim_products') }}
)

,sessions_products_pageviews AS (
    SELECT DISTINCT -- A session/product can have multiple page views
        session_id
        ,product_id -- Each row will have a product_id if event_type='page_view'
        ,1 as was_viewed
    FROM fact_events
    WHERE event_type='page_view'
)

,sessions_products_purchases AS (
    SELECT -- will not be multiple session/products
        fact_events.session_id
        ,stg_order_items.product_id
        ,1 as was_purchased
    FROM fact_events
    INNER JOIN stg_order_items
        ON fact_events.order_id = stg_order_items.order_id
    WHERE fact_events.event_type='checkout'
)

,joined AS (
    SELECT
        COALESCE(sessions_products_pageviews.session_id, sessions_products_purchases.session_id) AS session_id
        ,COALESCE(sessions_products_pageviews.product_id, sessions_products_purchases.product_id) AS product_id
        ,COALESCE(sessions_products_pageviews.was_viewed, 0) AS was_viewed
        ,COALESCE(sessions_products_purchases.was_purchased, 0) AS was_purchased
    FROM sessions_products_pageviews
    FULL OUTER JOIN sessions_products_purchases
        ON sessions_products_pageviews.session_id = sessions_products_purchases.session_id
            AND sessions_products_pageviews.product_id = sessions_products_purchases.product_id
)

,final AS (
    SELECT
        joined.session_id
        ,joined.product_id
        ,dim_products.product_name
        ,joined.was_viewed
        ,joined.was_purchased
    FROM joined
    LEFT JOIN dim_products
        ON joined.product_id = dim_products.product_id
)

SELECT * FROM final