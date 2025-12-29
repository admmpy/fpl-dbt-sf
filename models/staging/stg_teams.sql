WITH source AS (
    SELECT * 
    FROM {{ source('fpl_raw', 'teams') }}
),

cleaned AS (
    SELECT 
        TEAM_ID                 AS team_id,
        NAME                    AS team_name,
        SHORT_NAME              AS short_name,
        POINTS                  AS points,
        FORM                    AS form,
        POSITION                AS position,
        STRENGTH                AS team_strength,
        STRENGTH_ATTACK_AWAY    AS strength_attack_away,
        STRENGTH_DEFENCE_AWAY   AS strength_defence_away,
        STRENGTH_ATTACK_HOME    AS strength_attack_home,
        STRENGTH_DEFENCE_HOME   AS strength_defence_home,
        STRENGTH_OVERALL_AWAY   AS strength_overall_away,
        STRENGTH_OVERALL_HOME   AS strength_overall_home,
        INGESTION_TIMESTAMP     AS ingestion_at

    FROM source
)

SELECT * 
FROM cleaned