/*
This model collapses fixture-level ML features to player-gameweek grain.

GRAIN: One row per player per gameweek (player_id x gameweek_id)

LOGIC:
- Aggregates fixture-level scoring signals across all fixtures in the gameweek
- Preserves rolling/context features at gameweek grain for double gameweeks
- Retains deterministic latest snapshot fields using ingestion_at and fixture_id ordering

EXAMPLE:
  Player in GW15 (double gameweek): 12 pts (Fixture A) + 8 pts (Fixture B) = 20 pts total
  One row is produced for GW15 with summed scoring features and latest snapshot metadata
*/

{{
    config(
        materialized='table',
        tags=['intermediate'],
        cluster_by=['gameweek_id', 'player_id']
    )
}}

WITH base_data AS (
    SELECT
        web_name,
        player_id,
        gameweek_id,
        fixture_id,
        team_id,
        opponent_team_id,
        total_points,
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
        opponent_defence_strength,
        team_attack_strength,
        form,
        status,
        now_cost,
        ingestion_at,
        three_week_players_roll_avg_points,
        five_week_players_roll_avg_points,
        total_games_played,
        team_roll_avg_goals_scored,
        team_roll_avg_xg,
        team_roll_avg_clean_sheets,
        team_roll_avg_wins_pct,
        opponent_roll_avg_goals_conceded,
        opponent_roll_avg_xg,
        team_position,
        opponent_team_position,
        team_position_difference

    FROM {{ ref('fct_ml_player_features') }}
),

player_values AS (
    SELECT
        player_id,
        current_value

    FROM {{ ref('dim_players') }}
),

-- Aggregate double-gameweek fixture rows to player-gameweek grain
gameweek_aggregated AS (
    SELECT
        player_id,
        gameweek_id,
        SUM(total_points)                                               AS total_points,
        SUM(minutes_played)                                             AS minutes_played,
        SUM(goals_scored)                                               AS goals_scored,
        SUM(expected_goals)                                             AS expected_goals,
        SUM(expected_goal_involvements)                                 AS expected_goal_involvements,
        SUM(assists)                                                    AS assists,
        SUM(expected_assists)                                           AS expected_assists,
        SUM(clean_sheets)                                               AS clean_sheets,
        SUM(goals_conceded)                                             AS goals_conceded,
        SUM(expected_goals_conceded)                                    AS expected_goals_conceded,
        SUM(yellow_cards)                                               AS yellow_cards,
        SUM(red_cards)                                                  AS red_cards,
        SUM(saves)                                                      AS saves,
        SUM(bonus)                                                      AS bonus,
        SUM(influence)                                                  AS influence,
        SUM(creativity)                                                 AS creativity,
        SUM(threat)                                                     AS threat,
        SUM(ict_index)                                                  AS ict_index,
        AVG(opponent_defence_strength)                                  AS opponent_defence_strength,
        AVG(team_attack_strength)                                       AS team_attack_strength,
        AVG(three_week_players_roll_avg_points)                         AS three_week_players_roll_avg_points,
        AVG(five_week_players_roll_avg_points)                          AS five_week_players_roll_avg_points,
        AVG(team_roll_avg_goals_scored)                                 AS team_roll_avg_goals_scored,
        AVG(team_roll_avg_xg)                                           AS team_roll_avg_xg,
        AVG(team_roll_avg_clean_sheets)                                 AS team_roll_avg_clean_sheets,
        AVG(team_roll_avg_wins_pct)                                     AS team_roll_avg_wins_pct,
        AVG(opponent_roll_avg_goals_conceded)                           AS opponent_roll_avg_goals_conceded,
        AVG(opponent_roll_avg_xg)                                       AS opponent_roll_avg_xg,
        AVG(opponent_team_position)                                     AS opponent_team_position,
        AVG(team_position_difference)                                   AS team_position_difference

    FROM base_data
    GROUP BY 1, 2
),

latest_snapshot AS (
    SELECT
        web_name,
        player_id,
        gameweek_id,
        team_id,
        opponent_team_id,
        form,
        status,
        now_cost,
        ingestion_at,
        total_games_played,
        team_position

    FROM base_data
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY player_id, gameweek_id
        ORDER BY ingestion_at DESC, fixture_id DESC
    ) = 1
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ga.player_id', 'ga.gameweek_id']) }} AS player_gameweek_key,
        ls.web_name,
        ga.player_id,
        ga.gameweek_id,
        ls.team_id,
        ls.opponent_team_id,
        ga.total_points,
        ga.minutes_played,
        ga.goals_scored,
        ga.expected_goals,
        ga.expected_goal_involvements,
        ga.assists,
        ga.expected_assists,
        ga.clean_sheets,
        ga.goals_conceded,
        ga.expected_goals_conceded,
        ga.yellow_cards,
        ga.red_cards,
        ga.saves,
        ga.bonus,
        ga.influence,
        ga.creativity,
        ga.threat,
        ga.ict_index,
        ga.opponent_defence_strength,
        ga.team_attack_strength,
        ls.form,
        ls.status,
        COALESCE(ls.now_cost, pv.current_value)                                   AS now_cost,
        ls.ingestion_at,
        ga.three_week_players_roll_avg_points,
        ga.five_week_players_roll_avg_points,
        ls.total_games_played,
        ga.team_roll_avg_goals_scored,
        ga.team_roll_avg_xg,
        ga.team_roll_avg_clean_sheets,
        ga.team_roll_avg_wins_pct,
        ga.opponent_roll_avg_goals_conceded,
        ga.opponent_roll_avg_xg,
        ls.team_position,
        ga.opponent_team_position,
        ga.team_position_difference

    FROM gameweek_aggregated                  AS ga
         INNER JOIN latest_snapshot           AS ls ON ga.player_id = ls.player_id
                                                       AND ga.gameweek_id = ls.gameweek_id
         LEFT JOIN player_values              AS pv ON ga.player_id = pv.player_id
)

SELECT *
FROM final
