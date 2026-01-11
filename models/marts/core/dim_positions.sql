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
