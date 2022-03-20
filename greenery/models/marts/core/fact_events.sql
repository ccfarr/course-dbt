/*
Events fact table
*/

WITH stg_events AS (
    SELECT *
    FROM {{ ref('stg_events') }}
)

,stg_users AS (
    SELECT *
    FROM {{ ref('stg_users') }}
)

,stg_products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
)

,joined AS (
    SELECT
        stg_events.event_id
        ,stg_events.session_id
        ,stg_events.user_id
        ,stg_events.page_url
        ,stg_events.created_at
        ,stg_events.event_type
        ,stg_events.order_id
        ,stg_events.product_id

        ,stg_users.full_name
        ,stg_products.product_name
    FROM stg_events
    LEFT JOIN stg_users -- unique in user_id, no dupes
        ON stg_events.product_id = stg_users.user_id
    LEFT JOIN stg_products -- unique in product_id, no dupes
        ON stg_events.product_id = stg_products.product_id
)

SELECT * FROM joined