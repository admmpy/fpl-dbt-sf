WITH source AS (
    SELECT * 
    FROM {{ source('fpl_raw', 'gameweeks') }}
),

cleaned AS (
    SELECT 
        GAMEWEEK_ID                         AS gameweek_id,
        IS_CURRENT                          AS is_current,
        DEADLINE_TIME                       AS deadline_time,
        FINISHED                            AS is_finished,
        IS_NEXT                             AS is_next,
        IS_PREVIOUS                         AS is_previous,
        AVERAGE_ENTRY_SCORE                 AS average_entry_score,
        COALESCE(HIGHEST_SCORE, 0)          AS highest_score,
        COALESCE(MOST_SELECTED, 0)          AS most_selected,
        COALESCE(MOST_TRANSFERRED_IN, 0)    AS most_transferred_in,
        COALESCE(MOST_CAPTAINED, 0)         AS most_captained,
        COALESCE(MOST_VICE_CAPTAINED, 0)    AS most_vice_captained,
        INGESTION_TIMESTAMP                 AS ingestion_at

    FROM source
)

SELECT * 
FROM cleaned