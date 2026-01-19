/*
This model calculates 3-week rolling team performance statistics for each team and gameweek.

GRAIN: One row per team per gameweek (team_id x gameweek_id)

LOGIC:
- Aggregates fixture-level data to gameweek level (handles double gameweeks)
- Calculates 3-week rolling averages based on GAMEWEEKS not fixtures
- In double gameweeks, goals/xG are SUMMED across all fixtures
- Clean sheet = TRUE only if team conceded 0 across ALL fixtures in gameweek
- Win percentage = fixtures won / total fixtures in gameweek
- Window functions exclude the current gameweek to prevent data leakage

EXAMPLE:
  Team in GW15 (double gameweek): Win 2-1 (Fixture A) + Lose 0-3 (Fixture B)
  Goals: 2 total, Win%: 0.5, Clean sheet: FALSE
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
        team_id,
        gameweek_id,
        team_score,
        opponent_score,
        expected_goals,
        expected_goals_conceded,
        clean_sheets,
        fixture_id
        
    FROM {{ ref('fct_team_fixtures') }}
    WHERE is_finished = TRUE
),

-- CRITICAL FIX: Aggregate to gameweek level before calculating rolling stats
-- This handles double gameweeks where teams play multiple fixtures per gameweek
gameweek_aggregated AS (
    SELECT
        team_id,
        gameweek_id,
        SUM(team_score)                                                       AS total_goals_scored,
        SUM(opponent_score)                                                   AS total_goals_conceded,
        SUM(expected_goals)                                                   AS total_expected_goals,
        SUM(expected_goals_conceded)                                          AS total_expected_goals_conceded,
        -- Clean sheet is TRUE only if team conceded 0 goals across ALL fixtures in gameweek
        CASE WHEN SUM(opponent_score) = 0 THEN 1.0 ELSE 0.0 END              AS had_clean_sheet,
        -- CRITICAL FIX: Win percentage = fixtures won / total fixtures (not gameweek win/loss)
        -- This correctly handles double gameweeks where a team might win 1 and lose 1
        SUM(CASE WHEN team_score > opponent_score THEN 1.0 ELSE 0.0 END) / 
            NULLIF(COUNT(DISTINCT fixture_id), 0)                             AS win_percentage,
        COUNT(DISTINCT fixture_id)                                            AS fixtures_played
        
    FROM base_data
    GROUP BY 1, 2
),

rolling_stats AS (
    SELECT
        team_id,
        gameweek_id,
        total_goals_scored,
        total_goals_conceded,
        fixtures_played,

        -- offensive momentum
        AVG(total_goals_scored) OVER (
            PARTITION BY team_id
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                      AS three_week_team_roll_avg_goals_scored,
        AVG(total_expected_goals) OVER (
            PARTITION BY team_id
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                      AS three_week_team_roll_avg_expected_goals,
        -- defensive solidarity
        AVG(total_goals_conceded) OVER (
            PARTITION BY team_id
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                      AS three_week_team_roll_avg_opponent_score,
        AVG(total_expected_goals_conceded) OVER (
            PARTITION BY team_id
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                      AS three_week_team_roll_avg_expected_goals_conceded,
        -- CRITICAL FIX: Clean sheet now correctly represents gameweek-level clean sheet
        AVG(had_clean_sheet) OVER (
            PARTITION BY team_id
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                      AS three_week_team_roll_avg_clean_sheets,
        -- CRITICAL FIX: Win percentage now averages fixture-level win rates across gameweeks
        AVG(win_percentage) OVER (
            PARTITION BY team_id
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                      AS three_week_team_roll_avg_wins

    FROM gameweek_aggregated
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['rs.team_id', 'rs.gameweek_id']) }} AS team_gameweek_key,
        rs.team_id,
        rs.gameweek_id,
        ROUND(rs.three_week_team_roll_avg_goals_scored, 3)                       AS three_week_team_roll_avg_goals_scored,
        ROUND(rs.three_week_team_roll_avg_expected_goals, 3)                     AS three_week_team_roll_avg_expected_goals,
        ROUND(rs.three_week_team_roll_avg_opponent_score, 3)                     AS three_week_team_roll_avg_opponent_score,
        ROUND(rs.three_week_team_roll_avg_expected_goals_conceded, 3)            AS three_week_team_roll_avg_expected_goals_conceded,
        ROUND(rs.three_week_team_roll_avg_clean_sheets, 3)                       AS three_week_team_roll_avg_clean_sheets,
        ROUND(rs.three_week_team_roll_avg_wins, 3)                               AS three_week_team_roll_avg_wins

    FROM rolling_stats AS rs
)

SELECT *
FROM final