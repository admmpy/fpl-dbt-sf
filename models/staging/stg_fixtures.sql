WITH sourced AS (
    SELECT *
    FROM {{ source('fpl_raw', 'fixtures') }}
),

cleaned AS (
    SELECT
        FIXTURE_ID      AS fixture_id,
        GAMEWEEK_ID     AS gameweek_id,
        KICKOFF_TIME    AS kickoff_at,
        TEAM_H          AS home_team_id,
        TEAM_A          AS away_team_id,
        TEAM_H_SCORE    AS home_team_score,
        TEAM_H_SCORE    AS away_team_score,
        FINISHED        AS has_finished

    FROM sourced
)

SELECT *
FROM cleaned