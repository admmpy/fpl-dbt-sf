WITH player_base AS (
    SELECT *
    FROM {{ ref('fct_players_gameweek') }}
),

player_rolling AS (
    SELECT *
    FROM {{ ref('int_player_rolling_stats') }}
),

team_rolling AS (
    SELECT *
    FROM {{ ref('int_team_rolling_stats') }}
),

team_positions AS (
    SELECT 
        team_id,
        team_position

    FROM {{ ref('dim_teams') }}
),

final AS (
    SELECT 
        pb.*,

        -- player rolling stats
        pr.three_week_players_roll_avg_points,
        pr.five_week_players_roll_avg_points,
        pr.total_games_played,

        -- team rolling stats
        tr.three_week_team_roll_avg_goals_scored                AS team_roll_avg_goals_scored,
        tr.three_week_team_roll_avg_expected_goals              AS team_roll_avg_xg,
        tr.three_week_team_roll_avg_clean_sheets                AS team_roll_avg_clean_sheets,
        tr.three_week_team_roll_avg_wins                        AS team_roll_avg_wins_pct,

        -- opponent rolling stats
        otr.three_week_team_roll_avg_opponent_score             AS opponent_roll_avg_goals_conceded,
        otr.three_week_team_roll_avg_expected_goals_conceded    AS opponent_roll_avg_xg,

        -- team positions 
        tp.team_position                                        AS team_position,
        opt.team_position                                       AS opponent_team_position,
        tp.team_position - opt.team_position                    AS team_position_difference -- the higer the number the better the opposition is
    
    FROM player_base                        AS pb
         LEFT JOIN player_rolling           AS pr ON pb.player_id = pr.player_id
                                                     AND pb.gameweek_id = pr.gameweek_id
         LEFT JOIN team_rolling             AS tr ON pb.team_id = tr.team_id
                                                     AND pb.gameweek_id = tr.gameweek_id
         LEFT JOIN team_rolling             AS otr ON pb.opponent_team_id = otr.team_id
                                                     AND pb.gameweek_id = otr.gameweek_id

         LEFT JOIN team_positions           AS tp ON pb.team_id = tp.team_id
         LEFT JOIN team_positions           AS opt ON pb.opponent_team_id = opt.team_id

)

SELECT *
FROM final