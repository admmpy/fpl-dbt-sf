/*
Denormalized view of the current week's recommended squad
Joins with dim_players, dim_teams, dim_positions for full context
Pre-calculated squad totals (cost, expected points)
Optimized for dashboard display
*/

{{
    config(
        materialized='table',
        tags=['reporting'],
        cluster_by=['gameweek_id', 'player_id']
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('fct_model_analysis') }}
),

players AS (
    SELECT
        player_id,
        web_name,
        first_name,
        second_name

    FROM {{ ref('dim_players') }}
),

teams AS (
    SELECT
        team_id,
        team_name,
        short_name

    FROM {{ ref('dim_teams') }}
),

positions AS (
    SELECT
        position_id,
        position_name

    FROM {{ ref('dim_positions') }}
),

final AS (
    SELECT
        bs.recommended_at,
        bs.gameweek_id,
        bs.player_id,
        pl.web_name,
        pl.first_name,
        pl.second_name,
        bs.position_id,
        ps.position_name,
        bs.team_id,
        tm.team_name,
        tm.short_name                      AS team_short_name,
        bs.now_cost,
        bs.predicted_points,
        bs.actual_points,
        bs.absolute_error,
        bs.error_bias,
        bs.is_in_squad,
        bs.is_starter,
        bs.is_captain,
        bs.is_vice_captain,
        CASE WHEN bs.gameweek_id = MAX(bs.gameweek_id) OVER () THEN 1 ELSE 0 END AS is_current_week,
        CASE WHEN bs.gameweek_id = MAX(bs.gameweek_id) OVER () - 1 THEN 1 ELSE 0 END AS is_previous_week

    FROM base                AS bs
         LEFT JOIN players   AS pl ON bs.player_id = pl.player_id
         LEFT JOIN teams     AS tm ON bs.team_id = tm.team_id
         LEFT JOIN positions AS ps ON bs.position_id = ps.position_id      
)

SELECT *
FROM final
