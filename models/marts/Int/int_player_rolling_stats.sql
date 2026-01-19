/*
This model calculates rolling player performance statistics for each player and gameweek.

GRAIN: One row per player per gameweek (player_id x gameweek_id)

LOGIC:
- Aggregates fixture-level data to gameweek level (handles double gameweeks)
- Calculates 3-week and 5-week rolling averages based on GAMEWEEKS not fixtures
- In double gameweeks, player points and minutes are SUMMED across all fixtures
- Window functions exclude the current gameweek to prevent data leakage

EXAMPLE:
  Player in GW15 (double gameweek): 12 pts (Fixture A) + 8 pts (Fixture B) = 20 pts total
  3-week average looks back at GW14, GW13, GW12 (summed per gameweek if double)
*/

{{
    config(
        materialized='table',
        tags=['intermediate']
    )
}}

WITH base_data AS (
    SELECT
        fct.player_id,
        fct.gameweek_id,
        COALESCE(fct.total_points, 0)                                              AS total_points,
        COALESCE(fct.minutes_played, 0)                                            AS minutes_played,
        fct.fixture_id
    FROM {{ ref('fct_players_gameweek') }}       AS fct
         INNER JOIN {{ ref('stg_gameweeks') }}   AS gw ON fct.gameweek_id = gw.gameweek_id
    WHERE gw.is_finished = TRUE
),

-- CRITICAL FIX: Aggregate to gameweek level before calculating rolling stats
-- This handles double gameweeks where players play multiple fixtures per gameweek
gameweek_aggregated AS (
    SELECT
        player_id,
        gameweek_id,
        SUM(total_points)                                                          AS total_points,
        -- Player played if they had any minutes in any fixture that gameweek
        MAX(minutes_played)                                                        AS max_minutes_played,
        -- Total minutes across all fixtures in the gameweek
        SUM(minutes_played)                                                        AS total_minutes_played,
        COUNT(DISTINCT fixture_id)                                                 AS fixtures_played
        
    FROM base_data
    GROUP BY 1, 2
),

rolling_stats AS (
    SELECT
        player_id,
        gameweek_id,
        total_points,
        AVG(total_points) OVER (
            PARTITION BY player_id 
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                      AS three_week_players_roll_avg_points,
        AVG(total_points) OVER (
            PARTITION BY player_id 
            ORDER BY gameweek_id DESC 
            ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING
            )                                                                      AS five_week_players_roll_avg_points

    FROM gameweek_aggregated
),

games_played AS (
    SELECT
        player_id,
        gameweek_id,
        COALESCE(SUM(CASE WHEN total_minutes_played > 0 THEN 1 ELSE 0 END) OVER (
                    PARTITION BY player_id
                    ORDER BY gameweek_id DESC
                    ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                ), 0)                                                              AS total_games_played

    FROM gameweek_aggregated
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['rs.player_id', 'rs.gameweek_id']) }} AS player_gameweek_key,
        rs.player_id,
        rs.gameweek_id,
        rs.total_points,
        rs.three_week_players_roll_avg_points,
        rs.five_week_players_roll_avg_points,
        gp.total_games_played

    FROM rolling_stats          AS rs
         LEFT JOIN games_played AS gp ON rs.player_id = gp.player_id
                                         AND rs.gameweek_id = gp.gameweek_id
)

SELECT *
FROM final