/*
TODO
*/

WITH int_users_orders_agg AS (
    SELECT *
    FROM {{ ref('int_users_orders_agg') }}
)

SELECT * FROM int_users_orders_agg