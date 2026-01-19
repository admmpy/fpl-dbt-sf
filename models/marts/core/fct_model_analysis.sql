/*
This model provides a structured comparison between ML predictions and actual FPL performance.

GRAIN: One row per player per gameweek (player_id x gameweek_id x recommended_at).
MATERIALIZATION: Table (For historical analysis of model accuracy).

KEY ARCHITECTURAL DECISIONS:
1. Deduplication: Uses ROW_NUMBER() to isolate the latest prediction made for each player-gameweek.
2. Grain Alignment: Joins predictions to actual gameweek performance.
3. Accuracy Focus: Uses INNER JOIN to evaluate accuracy only where model made a prediction AND player played.
4. Error Metrics: Calculates absolute error and bias to measure model performance.
5. Context: Includes squad selection flags to understand strategic impact of errors.
*/

{{
    config(
        materialized='table',
        tags=['analysis'],
        cluster_by=['gameweek_id', 'player_id']
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('stg_recommended_squad') }}
),

latest_recommendations AS (
    SELECT
        recommendation_key,
        recommended_at,
        player_id,
        position_id,
        team_id,
        now_cost,
        gameweek_id,
        expected_points_next_gw                                                              AS predicted_points,
        expected_points_5_gw,
        is_in_squad,
        is_starter,
        is_captain,
        is_vice_captain,
        ROW_NUMBER() OVER (PARTITION BY player_id, gameweek_id ORDER BY recommended_at DESC) AS rec_rank

    FROM base
),

final_predictions AS (
    SELECT *
    FROM latest_recommendations
    WHERE rec_rank = 1
),

actual_performance AS (
    SELECT
        player_id,
        gameweek_id,
        SUM(total_points)           AS actual_points,
        SUM(minutes_played)         AS minutes_played,
        SUM(goals_scored)           AS goals_scored,
        SUM(assists)                AS assists,
        SUM(clean_sheets)           AS clean_sheets,
        SUM(goals_conceded)         AS goals_conceded,
        SUM(yellow_cards)           AS yellow_cards,
        SUM(red_cards)              AS red_cards,
        SUM(saves)                  AS saves,
        SUM(bonus)                  AS bonus,
        AVG(influence)              AS avg_influence,
        AVG(creativity)             AS avg_creativity,
        AVG(threat)                 AS avg_threat,
        AVG(ict_index)              AS avg_ict_index,
        MAX(value)                  AS value

    FROM {{ ref('fct_players_gameweek') }}
    GROUP BY 1, 2
),

final AS (
    SELECT
        fp.recommendation_key,
        fp.recommended_at,
        fp.player_id,
        fp.gameweek_id,
        fp.position_id,
        fp.team_id,
        fp.now_cost,
        fp.predicted_points,
        ap.actual_points,
        ABS(ap.actual_points - fp.predicted_points) AS absolute_error,
        ap.actual_points - fp.predicted_points      AS error_bias,
        fp.is_in_squad,
        fp.is_starter,
        fp.is_captain,
        fp.is_vice_captain,
        ap.minutes_played,
        ap.goals_scored,
        ap.assists,
        ap.clean_sheets,
        ap.goals_conceded,
        ap.yellow_cards,
        ap.red_cards,
        ap.saves,
        ap.bonus,
        ap.avg_influence,
        ap.avg_creativity,
        ap.avg_threat,
        ap.avg_ict_index,
        ap.value

    FROM final_predictions             AS fp
         LEFT JOIN actual_performance  AS ap ON fp.player_id = ap.player_id
                                                AND fp.gameweek_id = ap.gameweek_id
)

SELECT *
FROM final