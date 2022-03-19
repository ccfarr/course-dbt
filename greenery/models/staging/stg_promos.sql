WITH source AS (
    SELECT *
    FROM {{ source('raw', 'promos') }}
)

,final AS (
    SELECT 
        promo_id
        ,discount
        ,status
    FROM source
)

SELECT * FROM final