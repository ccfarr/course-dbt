/*
Compute the cost of an order, price x quantity
*/

WITH stg_order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
)

,stg_products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
)

,final AS (
    SELECT
        stg_order_items.order_id
        ,CAST(SUM(stg_order_items.quantity * stg_products.price) AS DECIMAL(5,2))AS derived_order_cost
    FROM stg_order_items
    LEFT JOIN stg_products -- unique in product_id, no dupes
        ON stg_order_items.product_id = stg_products.product_id
    GROUP BY 1
)

SELECT * FROM final