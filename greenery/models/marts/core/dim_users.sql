/*
Dimensional model for users
*/

WITH stg_users AS (
    SELECT *
    FROM {{ ref('stg_users') }}
)

,stg_addresses AS (
    SELECT *
    FROM {{ ref('stg_addresses') }}
)

,final AS (
    SELECT
        stg_users.user_id
        ,stg_users.first_name || ' ' || stg_users.last_name AS full_name
        ,stg_addresses.address
        ,stg_addresses.state
        ,stg_addresses.zipcode
        ,stg_addresses.country
        ,stg_users.email
        ,stg_users.phone_number
        ,stg_users.created_at
        ,stg_users.updated_at
    FROM stg_users
    LEFT JOIN stg_addresses
        ON stg_users.address_id = stg_addresses.address_id -- RHS table is unique in join col
)

SELECT * FROM final