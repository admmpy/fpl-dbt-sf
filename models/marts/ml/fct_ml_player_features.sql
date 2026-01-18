/*
This model creates a denormalized feature table for machine learning model training.
It combines player performance metrics with player and team rolling statistics, and league
position context into a single table optimized for ML pipelines.
*/

{{
    config(
        materialized='table',
        tags=['ml', 'marts']
    )
}}

WITH player_base AS (
    SELECT *
    FROM {{ ref('fct_players_gameweek') }}
),

player_snapshots AS (
    SELECT *
    FROM {{ ref('stg_players_gameweek_snapshot') }}
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
        pb.player_gameweek_key,
        pb.web_name,
        pb.player_id,
        pb.gameweek_id,
        pb.fixture_id,
        pb.team_id,
        pb.opponent_team_id,
        pb.total_points,
        pb.was_home,
        pb.minutes_played,
        pb.goals_scored,
        pb.expected_goals,
        pb.expected_goal_involvements,
        pb.assists,
        pb.expected_assists,
        pb.clean_sheets,
        pb.goals_conceded,
        pb.expected_goals_conceded,
        pb.yellow_cards,
        pb.red_cards,
        pb.saves,
        pb.bonus,
        pb.influence,
        pb.creativity,
        pb.threat,
        pb.ict_index,
        pb.opponent_defence_strength,
        pb.team_attack_strength,

        -- player snapshots
        ps.form,
        ps.status,
        ps.now_cost,
        ps.ingestion_at,

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
        tp.team_position - opt.team_position                    AS team_position_difference, -- the higer the number the better the opposition is

    FROM player_base                        AS pb
         LEFT JOIN player_rolling           AS pr ON pb.player_id = pr.player_id
                                                     AND pb.gameweek_id = pr.gameweek_id
         LEFT JOIN team_rolling             AS tr ON pb.team_id = tr.team_id
                                                     AND pb.gameweek_id = tr.gameweek_id
         LEFT JOIN team_rolling             AS otr ON pb.opponent_team_id = otr.team_id
                                                     AND pb.gameweek_id = otr.gameweek_id

         LEFT JOIN team_positions           AS tp ON pb.team_id = tp.team_id
         LEFT JOIN team_positions           AS opt ON pb.opponent_team_id = opt.team_id

         LEFT JOIN player_snapshots         AS ps ON pb.player_id = ps.player_id
                                                  AND pb.gameweek_id = ps.gameweek_id

)

SELECT *
FROM final