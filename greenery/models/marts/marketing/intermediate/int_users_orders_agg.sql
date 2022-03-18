/*
Join in dimensional attributes to stg_orders table
*/

WITH fact_orders AS (
    SELECT *
    FROM {{ ref('fact_orders') }}
)

,int_users_first_order AS (
    SELECT *
    FROM {{ ref('int_users_first_order') }}
)

,agg AS (
    SELECT
        user_id
        ,COUNT(DISTINCT order_id) AS number_of_orders
        ,SUM(order_total) AS customer_lifetime_value
    FROM fact_orders
    GROUP BY 1
)

,agg2 AS (
    SELECT
        *
        ,CASE WHEN number_of_orders > 1 THEN 1 ELSE 0 END AS is_repeat_customer
    FROM agg
)

,joined AS (

    SELECT
        agg2.user_id
        ,agg2.number_of_orders
        ,agg2.customer_lifetime_value
        ,agg2.is_repeat_customer
        ,int_users_first_order.first_order_state
        ,int_users_first_order.first_order_shipping_service
        ,int_users_first_order.first_order_has_promotion
    FROM agg2
    LEFT JOIN int_users_first_order
        ON agg2.user_id = int_users_first_order.user_id -- unique
)

,final AS (
    SELECT
        *
    FROM joined
)

SELECT * FROM final