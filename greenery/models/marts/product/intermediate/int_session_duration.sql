/*
Compute the duration of events
*/

WITH fact_events AS (
    SELECT *
    FROM {{ ref('fact_events') }}
)

,min_max AS (
    SELECT
        session_id
        ,event_id
        ,created_at
        ,ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created_at ASC) AS row_number
        ,MIN(created_at) OVER (PARTITION BY session_id) AS min_session_created_at
        ,MAX(created_at) OVER (PARTITION BY session_id) AS max_session_created_at
    FROM fact_events
)

,final AS (
    SELECT
        session_id
        ,min_session_created_at
        ,max_session_created_at
        ,max_session_created_at - min_session_created_at AS session_duration
    FROM min_max
    WHERE row_number = 1 -- arbitrarily keep first row of parititon for uniqueness
)

SELECT * FROM final