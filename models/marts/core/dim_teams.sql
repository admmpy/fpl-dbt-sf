/* 
This model creates a dimension table for team information,
including team details, current standings, and strength metrics
for both home and away performance.
*/

{{
    config(
        materialized='table',
        tags=['core', 'marts']
    )
}}

WITH teams AS (
    SELECT
        team_id,
        team_name,
        short_name,
        team_points,
        team_form,
        team_position,
        team_strength,
        strength_attack_away,
        strength_defence_away,
        strength_attack_home,
        strength_defence_home,
        strength_overall_away,
        strength_overall_home

    FROM {{ ref('stg_teams') }}
),

final AS (
    SELECT * 
    FROM teams
)

SELECT * 
FROM final
