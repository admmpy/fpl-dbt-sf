WITH player_base AS (
    SELECT *
    FROM {{ ref('fct_players_gameweek') }}
),

player_rolling AS (
    SELECT *
    FROM {{ ref('int_player_rolling_stats') }}
),

final AS (
    SELECT 
        pb.*,
        pr.three_week_players_roll_avg_points,
        pr.five_week_players_roll_avg_points,
        pr.total_games_played
    
    FROM player_base                        AS pb
         LEFT JOIN player_rolling           AS pr ON pb.player_id = pr.player_id
                                                     AND pb.gameweek_id = pr.gameweek_id
)

SELECT *
FROM final