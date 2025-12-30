WITH fixtures AS (
    SELECT *
    FROM {{ ref('stg_fixtures') }}
),

final AS (
    SELECT
        *
    FROM fixtures
)

SELECT * 
FROM final
