WITH base_data AS (
    SELECT
        fct.player_id,
        fct.gameweek_id,
        fct.total_points,
        fct.minutes_played
    FROM {{ ref('fct_players_gameweek') }}       AS fct
         INNER JOIN {{ ref('stg_gameweeks') }}   AS gw ON fct.gameweek_id = gw.gameweek_id
    WHERE gw.is_finished = TRUE
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
            )                                                           AS three_week_rolling_avg_points,
        AVG(total_points) OVER (
            PARTITION BY player_id 
            ORDER BY gameweek_id DESC 
            ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING
            )                                                           AS five_week_rolling_avg_points

    FROM base_data
),

games_played AS (
    SELECT
        player_id,
        gameweek_id,
        COALESCE(SUM(CASE WHEN minutes_played > 0 THEN 1 ELSE 0 END) OVER (
                    PARTITION BY player_id
                    ORDER BY gameweek_id DESC
                    ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                ), 0)                                                        AS total_games_played

    FROM base_data
),

final AS (
    SELECT
        rs.player_id,
        rs.gameweek_id,
        rs.total_points,
        rs.three_week_rolling_avg_points,
        rs.five_week_rolling_avg_points,
        gp.total_games_played

    FROM rolling_stats          AS rs
         LEFT JOIN games_played AS gp ON rs.player_id = gp.player_id
                                         AND rs.gameweek_id = gp.gameweek_id
)

SELECT *
FROM final