/*
Orders fact table
*/

WITH stg_orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
)

,stg_users AS (
    SELECT *
    FROM {{ ref('stg_users')}}
)

,stg_promos AS (
    SELECT *
    FROM {{ ref('stg_promos')}}
)

,stg_addresses AS (
    SELECT *
    FROM {{ ref('stg_addresses')}}
)

,int_orders_derived_cost AS (
    SELECT *
    FROM {{ ref('int_orders_derived_cost')}}
)

,joined AS (
    SELECT
        stg_orders.order_id
        ,stg_orders.user_id

        ,stg_users.full_name

        ,stg_promos.discount AS promotional_discount
        ,stg_promos.status AS promotional_status

        ,stg_addresses.state AS order_state
        ,stg_addresses.zipcode AS order_zipcode

        -- These two columns yield same results, good!
        ,int_orders_derived_cost.derived_order_cost
        ,stg_orders.order_cost
        
        ,stg_orders.shipping_cost
        ,stg_orders.order_total

        ,stg_orders.created_at
        ,stg_orders.shipping_service
        ,stg_orders.status AS order_status

        ,CASE WHEN stg_promos.discount IS NOT NULL THEN 1 ELSE 0 END AS has_promotion
    FROM stg_orders
    LEFT JOIN stg_users -- unique in user_id, no dupes
        ON stg_orders.user_id = stg_users.user_id
    LEFT JOIN stg_promos -- unique in promo_id, no dupes
        ON stg_orders.promo_id = stg_promos.promo_id
    LEFT JOIN stg_addresses -- unique in address_id, no dupes
        ON stg_orders.address_id = stg_addresses.address_id
    LEFT JOIN int_orders_derived_cost -- unique in order_id, no dupes
        ON stg_orders.order_id = int_orders_derived_cost.order_id
)

SELECT * FROM joined