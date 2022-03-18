/*
Orders fact table, just simple SELECT * for now
*/

WITH int_orders_joined AS (
    SELECT *
    FROM {{ ref('int_orders_joined') }}
)

SELECT * FROM int_orders_joined