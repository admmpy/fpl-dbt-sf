WITH rolling_avg AS (
    SELECT
        player_id,
        gameweek_id,
        total_points,
        AVG(total_score) OVER (PARTITION BY player_id ORDER BY gameweek_id DESC ROWS BETWEEN 2 PRECEDING) AS 3_week_rolling_avg,
        AVG(total_score) OVER (PARTITION BY player_id ORDER BY gameweek_id DESC ROWS BETWEEN 4 PRECEDING) AS 5_week_rolling_avg

    FROM {{ ref('fct_players_gameweek') }}
    ORDER BY 1, 2 DESC
)

SELECT *
FROM rolling_avg



