/*
Aggregated metrics from fct_model_analysis
MAE, RMSE, bias by position, team, gameweek
Rolling accuracy trends
Optimized for model monitoring dashboards
*/

{{
    config(
        materialized='table',
        tags=['reporting']
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('fct_model_analysis') }}
),

final AS (
    SELECT
        recommended_at,
        gameweek_id,
        COUNT(*)                           AS player_count,
        AVG(predicted_points)              AS avg_predicted_points,
        AVG(actual_points)                 AS avg_actual_points,
        AVG(absolute_error)                AS mean_absolute_error,
        AVG(error_bias)                    AS mean_error_bias

    FROM base
    GROUP BY 1, 2
)

SELECT *
FROM final