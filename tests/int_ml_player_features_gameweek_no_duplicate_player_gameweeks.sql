SELECT
    player_id,
    gameweek_id,
    COUNT(*) AS row_count
FROM {{ ref('int_ml_player_features_gameweek') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
