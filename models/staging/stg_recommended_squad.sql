/*
This model transforms raw recommended squad data from the ML/Optimization pipeline into a clean, structured format.
It extracts squad recommendations and predicted points for each player in each gameweek.
The data is used to evaluate model accuracy and identify systematic bias.
*/

{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

WITH source AS (
    SELECT * 
    FROM {{ source('fpl_raw', 'recommended_squad') }}
),

final AS (
    SELECT 
        RECOMMENDATION_KEY AS recommendation_key,
        RECOMMENDED_AT AS recommended_at,
        PLAYER_ID AS player_id,
        POSITION_ID AS position_id,
        TEAM_ID AS team_id,
        NOW_COST AS now_cost,
        GAMEWEEK_ID AS gameweek_id,
        EXPECTED_POINTS_NEXT_GW AS expected_points_next_gw,
        EXPECTED_POINTS_5_GW AS expected_points_5_gw,
        IS_IN_SQUAD AS is_in_squad,
        IS_STARTER AS is_starter,
        IS_CAPTAIN AS is_captain,
        IS_VICE_CAPTAIN AS is_vice_captain
        
    FROM source
)

SELECT * 
FROM final