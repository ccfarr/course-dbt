/*
TODO
*/

WITH stg_users AS (
    SELECT *
    FROM {{ ref('stg_users') }}
)

SELECT * FROM stg_users