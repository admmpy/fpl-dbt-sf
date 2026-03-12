/*
This model creates the latest serving table for live ML inference.

GRAIN: One row per player (player_id)

LOGIC:
- Starts from player-gameweek ML features already collapsed for double gameweeks
- Selects the latest available gameweek for each player
- Keeps the output contract aligned with live inference feature consumption
*/

{{
    config(
        materialized='table',
        tags=['ml', 'marts']
    )
}}

WITH player_gameweek_features AS (
    SELECT *
    FROM {{ ref('int_ml_player_features_gameweek') }}
),

final AS (
    SELECT
        player_gameweek_key,
        web_name,
        player_id,
        gameweek_id,
        team_id,
        opponent_team_id,
        total_points,
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
        opponent_defence_strength,
        team_attack_strength,
        form,
        status,
        now_cost,
        ingestion_at,
        three_week_players_roll_avg_points,
        five_week_players_roll_avg_points,
        total_games_played,
        team_roll_avg_goals_scored,
        team_roll_avg_xg,
        team_roll_avg_clean_sheets,
        team_roll_avg_wins_pct,
        opponent_roll_avg_goals_conceded,
        opponent_roll_avg_xg,
        team_position,
        opponent_team_position,
        team_position_difference

    FROM player_gameweek_features
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY player_id
        ORDER BY gameweek_id DESC, ingestion_at DESC
    ) = 1
)

SELECT *
FROM final
