WITH base_data AS (
    SELECT
        team_id,
        gameweek_id,
        team_score,
        opponent_score,
        expected_goals,
        expected_goals_conceded,
        clean_sheets
        
    FROM {{ ref('fct_team_fixtures') }}
    WHERE is_finished = TRUE
),

rolling_stats AS (
    SELECT
        team_id,
        gameweek_id,
        team_score,
        opponent_score,

        -- offensive momentum
        AVG(team_score) OVER (
            PARTITION BY team_id
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                      AS three_week_team_roll_avg_goals_scored,
        AVG(expected_goals) OVER (
            PARTITION BY team_id
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                     AS three_week_team_roll_avg_expected_goals,
        -- defensive solidarity
        AVG(opponent_score) OVER (
            PARTITION BY team_id
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                      AS three_week_team_roll_avg_opponent_score,
        AVG(expected_goals_conceded) OVER (
            PARTITION BY team_id
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                      AS three_week_team_roll_avg_expected_goals_conceded,
        AVG(CASE WHEN clean_sheets > 0 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY team_id
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                      AS three_week_team_roll_avg_clean_sheets,
        AVG(CASE WHEN team_score > opponent_score THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY team_id
            ORDER BY gameweek_id DESC
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            )                                                                      AS three_week_team_roll_avg_wins

    FROM base_data
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