/*
This model creates a dimension table for player positions in FPL.
It contains the four standard football positions: Goalkeeper, Defender, Midfielder, and Forward.
This is a static reference table for joining with player and performance data.
*/

WITH positions AS (
    SELECT 1 AS position_id, 'Goalkeeper' AS position_name
    UNION ALL
    SELECT 2, 'Defender'
    UNION ALL
    SELECT 3, 'Midfielder'
    UNION ALL
    SELECT 4, 'Forward'
),

final AS (
    SELECT *
    FROM positions
)

SELECT *
FROM final
