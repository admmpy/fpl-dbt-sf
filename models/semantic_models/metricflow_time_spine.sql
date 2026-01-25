/*
This model creates a daily time spine required by the dbt Semantic Layer.
The time spine provides a continuous series of dates that MetricFlow uses
for time-based joins and aggregations when querying metrics.
*/

{{
    config(
        materialized='table',
        tags=['semantic_layer']
    )
}}

WITH days AS (

    {{
        dbt.date_spine(
            'day',
            "to_date('01/01/2018', 'mm/dd/yyyy')",
            "to_date('01/01/2035', 'mm/dd/yyyy')"
        )
    }}

),

final AS (
    SELECT 
        CAST(date_day AS DATE) AS date_day
    FROM days
)

SELECT * 
FROM final
WHERE date_day > DATEADD(YEAR, -7, CURRENT_TIMESTAMP()) 
      AND date_day < DATEADD(DAY, 30, CURRENT_TIMESTAMP())
