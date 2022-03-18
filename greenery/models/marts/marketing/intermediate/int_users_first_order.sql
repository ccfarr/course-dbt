/*
Join in dimensional attributes to stg_orders table
*/

WITH fact_orders AS (
    SELECT *
    FROM {{ ref('fact_orders') }}
)

,order_count AS (
    SELECT
        *
        ,row_number() OVER (PARTITION BY user_id ORDER BY created_at ASC) AS user_order_count
    FROM fact_orders
)

,final AS (
    SELECT
        user_id
        ,order_state AS first_order_state
        ,shipping_service AS first_order_shipping_service
        ,has_promotion AS first_order_has_promotion
    FROM order_count
    WHERE user_order_count = 1
)

SELECT * FROM final