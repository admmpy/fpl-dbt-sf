/*
Description:
This model provides a fact table at the team-fixture level for Football Premier League analytics. 
Each row in the table represents a unique team's performance in a specific fixture and gameweek. 
Aggregations are performed at the (team_id, fixture_id, gameweek_id) level to enable downstream fixture-based analysis for both team and opponent, 
facilitating comprehensive performance reporting and advanced modeling.
*/

WITH player_history AS (
    SELECT *
    FROM {{ ref('stg_player_history') }}
),

player_teams AS (
    SELECT
        ph.player_id,
        ph.gameweek_id,
        ph.fixture_id,
        ph.opponent_team_id,
        CASE 
            WHEN ph.was_home = TRUE 
                THEN fx.home_team_id
            ELSE fx.away_team_id
        END                                             AS team_id,
        ph.goals_scored,
        ph.goals_conceded,
        ph.assists,
        ph.clean_sheets,
        ph.own_goals,
        ph.penalties_saved,
        ph.penalties_missed,
        ph.yellow_cards,
        ph.red_cards,
        ph.expected_goals,
        ph.expected_assists,
        ph.expected_goal_involvements,
        ph.expected_goals_conceded

    FROM player_history                         AS ph
         INNER JOIN {{ ref('stg_fixtures') }}   AS fx ON ph.fixture_id = fx.fixture_id
                                                         AND ph.gameweek_id = fx.gameweek_id
),

team_aggregated AS (
    SELECT
        team_id,
        gameweek_id,
        fixture_id,
        SUM(goals_scored)                               AS goals_scored,
        SUM(goals_conceded)                             AS goals_conceded,
        SUM(assists)                                    AS assists,
        SUM(clean_sheets)                               AS clean_sheets,
        SUM(own_goals)                                  AS own_goals,
        SUM(penalties_saved)                            AS penalties_saved,
        SUM(penalties_missed)                           AS penalties_missed,
        SUM(yellow_cards)                               AS yellow_cards,
        SUM(red_cards)                                  AS red_cards,
        SUM(expected_goals)                             AS expected_goals,
        SUM(expected_assists)                           AS expected_assists,
        SUM(expected_goal_involvements)                 AS expected_goal_involvements,
        SUM(expected_goals_conceded)                    AS expected_goals_conceded

    FROM player_teams
    GROUP BY 1, 2, 3
),

team_fixtures AS (
    SELECT
        fixture_id,
        gameweek_id,
        home_team_id                                    AS team_id,
        away_team_id                                    AS opponent_team_id,
        TRUE                                            AS was_home,
        home_team_score                                 AS team_score,
        away_team_score                                 AS opponent_score,
        is_finished

    FROM {{ref ('dim_fixtures') }}

    UNION ALL

    SELECT 
        fixture_id,
        gameweek_id,
        away_team_id                                    AS team_id,
        home_team_id                                    AS opponent_team_id,
        FALSE                                           AS was_home,
        away_team_score                                 AS team_score,
        home_team_score                                 AS opponent_score,
        is_finished

    FROM {{ref ('dim_fixtures') }}
),

teams AS (
    SELECT *
    FROM {{ ref('dim_teams') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['tf.team_id', 'tf.gameweek_id'])}} AS team_fixture_key,
        tf.team_id,
        tf.gameweek_id,
        tf.fixture_id,
        tf.opponent_team_id,
        tf.was_home,
        tf.team_score,
        tf.opponent_score,
        tf.is_finished,

        ta.assists,
        ta.clean_sheets,
        ta.own_goals,
        ta.penalties_saved,
        ta.penalties_missed,
        ta.yellow_cards,
        ta.red_cards,
        ta.expected_goals,
        ta.expected_assists,
        ta.expected_goal_involvements,
        ta.expected_goals_conceded,

        tm.team_points,
        tm.team_form,
        tm.team_position,
        tm.team_strength,
        tm.strength_attack_away,
        tm.strength_defence_away,
        tm.strength_attack_home,
        tm.strength_defence_home,
        tm.strength_overall_away,
        tm.strength_overall_home

    FROM team_fixtures                       AS tf
         LEFT JOIN team_aggregated           AS ta ON tf.team_id = ta.team_id 
                                                      AND tf.gameweek_id = ta.gameweek_id
                                                      AND tf.fixture_id = ta.fixture_id
         LEFT JOIN teams                     AS tm ON tm.team_id = tf.team_id
)

SELECT *
FROM final