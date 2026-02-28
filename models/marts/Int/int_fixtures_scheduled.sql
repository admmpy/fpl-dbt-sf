/*
This model is for downstream models that require stricter testing - gameweek_id is a nullable field due to fixtures being rescheduled, 
this model will exclude fixtures where gameweek_id is null so downstream models can keep not_nul testing without failing
*/

{{ 
    config(
        materialized='view',
        tags=['staging']
    )
}}

SELECT *
FROM {{ ref('stg_fixtures') }}
WHERE gameweek_id IS NOT NULL