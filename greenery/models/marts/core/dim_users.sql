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

,joined AS (
    SELECT
        stg_users.user_id
        ,full_name
        ,stg_addresses.address
        ,stg_addresses.state
        ,stg_addresses.zipcode
        ,stg_addresses.country
        ,stg_users.email
        ,stg_users.phone_number
        ,stg_users.created_at
        ,stg_users.updated_at
    FROM stg_users
    LEFT JOIN stg_addresses -- unique in address_id, no dupes
        ON stg_users.address_id = stg_addresses.address_id
)

SELECT * FROM joined