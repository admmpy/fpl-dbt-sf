/*
This model creates a fact table showing player performance metrics by gameweek.
It combines player statistics from stg_player_history with team strength data
from dim_teams to provide context about offensive and defensive capabilities
of both the player's team and their opponents. The model joins multiple
dimensions to enrich the performance data with team context and strength metrics.
*/

{{
    config(
        materialized='incremental',
        tags=['core', 'marts'],
        incremental_strategy='merge',
        unique_key='player_gameweek_key',
        on_schema_change='append_new_columns'
    )
}}

WITH performance AS (
    SELECT
        pl.web_name,
        ph.player_id,
        ph.gameweek_id,
        ph.fixture_id,
        CASE
            WHEN ph.was_home THEN fx.home_team_id
            ELSE fx.away_team_id
        END                                             AS team_id,
        ph.opponent_team_id,
        ph.total_points,
        ph.was_home,
        ph.minutes_played,
        ph.goals_scored,
        ph.expected_goals,
        ph.expected_goal_involvements,
        ph.assists,
        ph.expected_assists,
        ph.clean_sheets,
        ph.goals_conceded,
        ph.expected_goals_conceded,
        ph.yellow_cards,
        ph.red_cards,
        ph.saves,
        ph.bonus,
        ph.influence,
        ph.creativity,
        ph.threat,
        ph.ict_index,
        ph.value,
        ph.ingestion_at

    FROM {{ ref('stg_player_history') }}      AS ph
         INNER JOIN {{ ref('stg_fixtures') }} AS fx ON ph.fixture_id = fx.fixture_id
                                                       AND ph.gameweek_id = fx.gameweek_id
         INNER JOIN {{ ref('dim_players') }}  AS pl ON ph.player_id = pl.player_id
    {% if is_incremental() %}
    WHERE ph.ingestion_at >= (
            SELECT DATEADD(DAY, -7, MAX(target.ingestion_at)) 
            FROM {{ this }} AS target
    )
    {% endif %}
),

opponent_strength AS (
    SELECT 
        pe.*,
        -- This prevents NULL strength values which would break downstream analytics
        COALESCE(
            CASE 
                WHEN pe.was_home THEN tm.strength_defence_away
                ELSE  tm.strength_defence_home
            END,
            1000
        )                                                                        AS opponent_defence_strength

    FROM performance                        AS pe
         LEFT JOIN {{ ref('dim_teams') }}   AS tm ON pe.opponent_team_id = tm.team_id
),

team_context AS (
    SELECT 
        oc.*,
        -- This prevents NULL strength values which would break downstream analytics
        COALESCE(
            CASE 
                WHEN oc.was_home THEN tm.strength_attack_home
                ELSE tm.strength_attack_away
            END,
            1000
        )                                                                         AS team_attack_strength

    FROM opponent_strength                  AS oc      
         LEFT JOIN {{ ref('dim_teams') }}   AS tm ON oc.team_id = tm.team_id
),

final AS (
    SELECT
        -- Without this, multiple fixtures in same gameweek would cause duplicate key conflicts
        {{ dbt_utils.generate_surrogate_key(['tc.player_id', 'tc.gameweek_id', 'tc.fixture_id']) }} AS player_gameweek_key,
        web_name,
        player_id,
        gameweek_id,
        fixture_id,
        team_id,
        opponent_team_id,
        total_points,
        was_home,
        minutes_played,
        goals_scored,
        expected_goals,
        expected_goal_involvements,
        assists,
        expected_assists,
        clean_sheets,
        goals_conceded,
        expected_goals_conceded,
        yellow_cards,
        red_cards,
        saves,
        bonus,
        influence,
        creativity,
        threat,
        ict_index,
        value,
        opponent_defence_strength,
        team_attack_strength,
        ingestion_at,
        CURRENT_TIMESTAMP()                                                          AS updated_at

    FROM team_context                            AS tc
)

SELECT *
FROM final
