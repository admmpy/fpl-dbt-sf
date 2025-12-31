/*
This model transforms raw player history data from the FPL API into a clean, structured format.
It extracts player performance metrics for each fixture, including points scored,
goals, assists, and other key statistics. The data is flattened from JSON format
and cast to appropriate data types for analysis. */


WITH source AS (
    SELECT * 
    FROM {{ source('fpl_raw', 'raw_element_summary') }}
),

flattened AS (
    SELECT
        f.value:element::INTEGER                        AS player_id,
        f.value:round::INTEGER                          AS gameweek_id,
        f.value:fixture::INTEGER                        AS fixture_id,
        f.value:opponent_team::INTEGER                  AS opponent_team_id,
        f.value:total_points::INTEGER                   AS total_points,
        f.value:was_home::BOOLEAN                       AS was_home,
        f.value:kickoff_time::TIMESTAMP                 AS kickoff_at,
        f.value:team_h_score::INTEGER                   AS team_h_score,
        f.value:team_a_score::INTEGER                   AS team_a_score,
        f.value:minutes::INTEGER                        AS minutes_played,
        f.value:goals_scored::INTEGER                   AS goals_scored,
        f.value:assists::INTEGER                        AS assists,
        f.value:clean_sheets::INTEGER                   AS clean_sheets,
        f.value:goals_conceded::INTEGER                 AS goals_conceded,
        f.value:own_goals::INTEGER                      AS own_goals,
        f.value:penalties_saved::INTEGER                AS penalties_saved,
        f.value:penalties_missed::INTEGER               AS penalties_missed,
        f.value:yellow_cards::INTEGER                   AS yellow_cards,
        f.value:red_cards::INTEGER                      AS red_cards,
        f.value:saves::INTEGER                          AS saves,
        f.value:bonus::INTEGER                          AS bonus,
        f.value:bps::INTEGER                            AS bps,
        f.value:influence::FLOAT                        AS influence,
        f.value:creativity::FLOAT                       AS creativity,
        f.value:threat::FLOAT                           AS threat,
        f.value:ict_index::FLOAT                        AS ict_index,
        f.value:transfers_in::INTEGER                   AS transfers_in,
        f.value:transfers_out::INTEGER                  AS transfers_out,
        f.value:transfers_balance::INTEGER              AS transfers_balance,
        f.value:selected::INTEGER                       AS selected,
        f.value:expected_goals::FLOAT                   AS expected_goals,
        f.value:expected_assists::FLOAT                 AS expected_assists,
        f.value:expected_goal_involvements::FLOAT       AS expected_goal_involvements,
        f.value:expected_goals_conceded::FLOAT          AS expected_goals_conceded,
        f.value:value::INTEGER                          AS value
    FROM source,
        LATERAL flatten(input => source.data:history) AS f 
)

SELECT * 
FROM flattened