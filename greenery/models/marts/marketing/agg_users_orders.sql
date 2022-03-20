/*
Aggregates order data by user
*/

WITH fact_orders AS (
    SELECT *
    FROM {{ ref('fact_orders') }}
)

,dim_users AS (
    SELECT *
    FROM {{ ref('dim_users') }}
)

,int_users_first_order AS (
    SELECT *
    FROM {{ ref('int_users_first_order') }}
)

,agg AS (
    SELECT
        user_id
        ,COUNT(DISTINCT order_id) AS number_of_orders
        ,SUM(CASE WHEN order_status = 'shipped' THEN 1 ELSE 0 END) AS number_of_orders_shipped
        ,SUM(CASE WHEN order_status = 'preparing' THEN 1 ELSE 0 END) AS number_of_orders_preparing
        ,SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS number_of_orders_delivered
        ,SUM(order_total) AS customer_lifetime_value
        ,CASE WHEN COUNT(DISTINCT order_id) > 1 THEN 1 ELSE 0 END AS is_repeat_customer
    FROM fact_orders
    GROUP BY 1
)

,joined AS (
    SELECT
        agg.user_id
        ,dim_users.full_name
        ,dim_users.state
        ,dim_users.country
        ,agg.number_of_orders
        ,agg.number_of_orders_shipped
        ,agg.number_of_orders_preparing
        ,agg.number_of_orders_delivered
        ,agg.customer_lifetime_value
        ,agg.is_repeat_customer
        ,int_users_first_order.first_order_state
        ,int_users_first_order.first_order_shipping_service
        ,int_users_first_order.first_order_has_promotion
    FROM agg
    LEFT JOIN dim_users -- unique in user_id, no dupes
        ON agg.user_id = dim_users.user_id
    LEFT JOIN int_users_first_order -- unique in user_id, no dupes
        ON agg.user_id = int_users_first_order.user_id
)

SELECT * FROM joined