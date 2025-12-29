WITH source AS (
    SELECT *
    FROM {{ source('fpl_raw', 'players') }}

),

cleaned AS (
    SELECT
        FIRST_NAME                                      AS first_name,
        SECOND_NAME                                     AS second_name,
        WEB_NAME                                        AS web_name,  -- this is the name used on the FPL website and most straightforward display name

        PLAYER_ID                                       AS player_id,
        POSITION_ID                                     AS position_id,
        TEAM_ID                                         AS team_id,

        NOW_COST                                        AS now_cost,
        MINUTES                                         AS total_minutes,
        GOALS_SCORED                                    AS goals_scored,
        GOALS_CONCEDED                                  AS goals_conceded,
        ASSISTS                                         AS assists,
        CLEAN_SHEETS                                    AS clean_sheets,
        BONUS                                           AS bonus,
        COALESCE(CHANCE_OF_PLAYING_NEXT_ROUND, 0)       AS next_gameweek_odds,
        TOTAL_POINTS                                    AS total_points,
        FORM                                            AS form,
        VALUE_FORM                                      AS value_form,
        VALUE_SEASON                                    AS value_season,
        SELECTED_BY_PERCENT                             AS selected_by_percent,
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
        
        GAMEWEEK_FETCHED                                AS gameweek,
        INGESTION_TIMESTAMP                             AS ingestion_at,
        
    FROM source
)

SELECT * 
FROM cleaned