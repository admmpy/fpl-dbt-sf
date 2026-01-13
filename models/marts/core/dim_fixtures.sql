/*
This model creates a fixture dimension table by joining fixture data with team information.
It enriches the fixture details with home and away team names, providing a complete view of all match fixtures.
*/

{{ 
    config(
        materialized='table'
    ) 
}}

WITH fixtures AS (
    SELECT 
        fx.fixture_id,
        fx.gameweek_id,
        fx.kickoff_at,
        ht.team_name        AS home_team_name,
        fx.home_team_id,
        at.team_name        AS away_team_name,
        fx.away_team_id,
        fx.home_team_score,
        fx.away_team_score,
        fx.is_finished

    FROM {{ ref('stg_fixtures') }} AS fx
         LEFT JOIN {{ ref('dim_teams') }} AS ht ON fx.home_team_id = ht.team_id
         LEFT JOIN {{ ref('dim_teams') }} AS at ON fx.away_team_id = at.team_id
)

SELECT * 
FROM fixtures