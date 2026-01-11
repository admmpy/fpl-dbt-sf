/*
This model creates a player dimension table by joining player data with team information.
It includes the latest player value from their most recent gameweek performance.
*/

WITH players AS (
    SELECT 
        p.player_id,
        p.position_id,
        p.team_id,
        p.first_name,
        p.second_name,
        p.web_name,
        p.now_cost AS current_value


    FROM {{ ref('stg_players') }} AS p
),

final AS (
    SELECT *
    FROM players
)

SELECT * 
FROM final
