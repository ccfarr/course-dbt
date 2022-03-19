WITH source AS (
    SELECT *
    FROM {{ source('raw', 'addresses') }}
)

,final AS (
    SELECT 
        address_id
        ,address
        ,zipcode
        ,state
        ,country
    FROM source
)

SELECT * FROM final