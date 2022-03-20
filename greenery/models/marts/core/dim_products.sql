/*
Dimensional model for products
*/

WITH stg_products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
)

SELECT * FROM stg_products -- no transformations at this time