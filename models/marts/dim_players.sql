WITH players AS (
    SELECT 
        player_id,
        position_id,
        team_id,
        first_name,
        second_name,
        web_name,
        now_cost,
        total_minutes,
        goals_scored,
        goals_conceded,
        assists,
        clean_sheets,
        bonus,
        next_gameweek_odds,
        total_points,
        form,
        selected_by_percent,
        status,
        current_gameweek

    FROM {{ ref('stg_players') }}
),

final AS (
    SELECT
        *
    FROM players
)

SELECT * 
FROM final
