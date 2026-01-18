/*
This model creates a historical snapshot of player attributes for each gameweek.
By leveraging the append-only nature of the raw players table, it preserves point-in-time
metrics like form, status, and cost that are essential for ML model training.
The model uses QUALIFY to ensure only the latest ingestion per player/gameweek is kept.
*/

{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('fpl_raw', 'players') }}

),

renamed AS (
    SELECT
        PLAYER_ID                                       AS player_id,
        GAMEWEEK_FETCHED                                AS gameweek_id,
        FORM                                            AS form,
        CASE 
            WHEN STATUS = 'a' 
                THEN 'Available'
            WHEN STATUS = 'u'
                THEN 'Unavailable'
            WHEN STATUS = 's'
                THEN 'Suspended'
            WHEN STATUS = 'i'
                THEN 'Injured'
            WHEN STATUS = 'd'
                THEN 'Doubtful'
            WHEN STATUS = 'n'
                THEN 'Not playing'  -- cannot find official doc on what 'n' means (likely "not playing" or similar)
            ELSE STATUS
        END                                             AS status,
        NOW_COST / 10.0                                 AS now_cost,
        INGESTION_TIMESTAMP                             AS ingestion_at

    FROM source
),

final AS (
    SELECT *
    FROM renamed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY player_id, gameweek_id ORDER BY ingestion_at DESC) = 1
)

SELECT * 
FROM final
order by gameweek_id