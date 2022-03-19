WITH source AS (
    SELECT *
    FROM {{ source('raw', 'products') }}
)

,final AS (
    SELECT
        product_id
        ,name AS product_name
        ,price
        ,inventory
    FROM source
)

SELECT * FROM final