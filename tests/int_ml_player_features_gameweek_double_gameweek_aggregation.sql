WITH double_gameweek_source AS (
    SELECT
        player_id,
        gameweek_id,
        SUM(total_points)                                                   AS expected_total_points,
        SUM(minutes_played)                                                 AS expected_minutes_played,
        COUNT(*)                                                            AS fixture_rows

    FROM {{ ref('fct_ml_player_features') }}
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
),

validation AS (
    SELECT
        src.player_id,
        src.gameweek_id,
        src.expected_total_points,
        agg.total_points,
        src.expected_minutes_played,
        agg.minutes_played,
        src.fixture_rows

    FROM double_gameweek_source                 AS src
         LEFT JOIN {{ ref('int_ml_player_features_gameweek') }} AS agg ON src.player_id = agg.player_id
                                                                          AND src.gameweek_id = agg.gameweek_id
    WHERE agg.player_id IS NULL
          OR src.expected_total_points != agg.total_points
          OR src.expected_minutes_played != agg.minutes_played
)

SELECT *
FROM validation
