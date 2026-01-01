/*
This model creates a player dimension table by joining player data with team information.
It includes the latest player value from their most recent gameweek performance.
*/

WITH players AS (
    SELECT 
        p.player_id,
        p.position_id,
        p.team_id,
        t.team_name,
        p.first_name,
        p.second_name,
        p.web_name,


    FROM {{ ref('stg_players') }} AS p
         LEFT JOIN {{ ref('stg_teams') }} AS t ON p.team_id = t.team_id
),

player_history AS (
    SELECT
        player_id,
        value

    FROM {{ ref('stg_player_history') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY gameweek_id DESC) = 1 
),

final AS (
    SELECT
        p.player_id,
        p.position_id,
        p.team_id,
        p.team_name,
        p.first_name,
        p.second_name,
        p.web_name,
        h.value

    FROM players AS p
    LEFT JOIN player_history AS h ON p.player_id = h.player_id
)

SELECT * 
FROM final
