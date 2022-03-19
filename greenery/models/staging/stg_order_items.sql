WITH source AS (
    SELECT *
    FROM {{ source('raw', 'order_items') }}
)

,final AS (
    SELECT 
        order_id
        ,product_id
        ,quantity
    FROM source
)

SELECT * FROM final