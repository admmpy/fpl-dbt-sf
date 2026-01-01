-- Find players in history but not in current players
SELECT DISTINCT
    h.player_id,
    h.value,
    h.total_points,
    h.goals_scored,
    h.assists
    -- Add any other fields you want to see
FROM {{ ref('stg_player_history') }} h
LEFT JOIN {{ ref('stg_players') }} p ON h.player_id = p.player_id
WHERE p.player_id IS NULL

SELECT MAX(player_id) FROM {{ ref('dim_players') }}
