SELECT
    player_id,
    COUNT(*) AS row_count
FROM {{ ref('fct_ml_player_features_latest') }}
GROUP BY 1
HAVING COUNT(*) > 1
