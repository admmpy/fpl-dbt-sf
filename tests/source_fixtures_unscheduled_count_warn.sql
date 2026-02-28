{{ config(severity='warn') }}

WITH stats AS (
    SELECT COUNT(*) AS unscheduled_cnt
    FROM {{ source('fpl_raw', 'fixtures') }}
    WHERE gameweek_id IS NULL
)
SELECT *
FROM stats
WHERE unscheduled_cnt > {{ var('max_unscheduled_fixtures', 2)}}