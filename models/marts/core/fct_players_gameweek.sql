/*
This model creates a fact table showing player performance metrics by gameweek.
It combines player statistics from stg_player_history with team strength data
from dim_teams to provide context about offensive and defensive capabilities
of both the player's team and their opponents. The model joins multiple
dimensions to enrich the performance data with team context and strength metrics.
*/

WITH performance AS (
    SELECT  
        player_id,
        gameweek_id,
        fixture_id,
        opponent_team_id,
        total_points,
        was_home,
        minutes_played,
        goals_scored,
        expected_goals,
        expected_goal_involvements,
        assists,
        expected_assists,
        clean_sheets,
        goals_conceded,
        expected_goals_conceded,
        yellow_cards,
        red_cards,
        saves,
        bonus,
        influence,
        creativity,
        threat,
        ict_index,
        value

    FROM {{ ref('stg_player_history') }}
),

opponent_strength AS (
    SELECT 
        pe.*,
        tm.team_id,
        CASE 
            WHEN pe.was_home THEN tm.strength_defence_home  
            ELSE  tm.strength_defence_away
        END                                                     AS opponent_defence_strength
        FROM performance                        AS pe
             LEFT JOIN {{ ref('dim_teams') }}   AS tm ON pe.opponent_team_id = tm.team_id
),

team_context AS (
    SELECT 
        oc.*,
        CASE 
            WHEN oc.was_home THEN tm.strength_attack_home
            ELSE tm.strength_attack_away
        END                                                     AS team_attack_strength
    FROM opponent_strength              AS oc      
    LEFT JOIN {{ ref('dim_teams') }}    AS tm ON oc.team_id = tm.team_id
),

final AS (
    SELECT
        pl.web_name,
        tc.*,


    FROM team_context                           AS tc
         INNER JOIN {{ ref('dim_players')}}      AS pl ON tc.player_id = pl.player_id

)

SELECT 
    *
FROM final

