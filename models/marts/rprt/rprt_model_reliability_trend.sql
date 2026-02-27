/*
Aggregated metrics from fct_model_analysis to analyse model performance on a gameweek basis
*/

{{
    config(
        materialized='table',
        tags=['reporting'],
        cluster_by=['gameweek_id', 'recommended_at']
    )
}}

WITH base AS (
    SELECT
        gameweek_id,
        recommended_at,
        player_id,
        predicted_points,
        actual_points,
        absolute_error,
        error_bias,
        is_starter, 
        is_finished

    FROM {{ ref('fct_model_analysis') }}
    WHERE is_starter = TRUE
          AND is_finished = TRUE
    QUALIFY recommended_at = MAX(recommended_at) OVER (PARTITION BY gameweek_id)
),


gameweek_metrics AS (
    SELECT
        recommended_at,
        gameweek_id,
        SUM(predicted_points)              AS total_predicted_points,
        SUM(actual_points)                 AS total_actual_points,
        COUNT(*)                           AS player_count,
        AVG(predicted_points)              AS avg_predicted_points,
        AVG(actual_points)                 AS avg_actual_points,
        AVG(absolute_error)                AS mean_absolute_error,
        AVG(error_bias)                    AS mean_error_bias

    FROM base
    GROUP BY 1, 2
),

final AS (
    SELECT
        gameweek_id,
        recommended_at,
        player_count,
        total_predicted_points,
        total_actual_points,
        avg_predicted_points,
        avg_actual_points,
        mean_absolute_error,
        mean_error_bias,
        AVG(mean_absolute_error) OVER (
            ORDER BY gameweek_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS cumulative_mae,
        AVG(mean_error_bias) OVER (
            ORDER BY gameweek_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS cumulative_error_bias,
        AVG(mean_absolute_error) OVER (
            ORDER BY gameweek_id
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)          AS rolling_3gw_mae,
        AVG(mean_error_bias) OVER (
            ORDER BY gameweek_id
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)          AS rolling_3gw_error_bias

    FROM gameweek_metrics
)

SELECT *
FROM final