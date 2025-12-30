WITH teams AS (
    SELECT
        team_id,
        team_name,
        short_name,
        points,
        form,
        position AS team_position,
        team_strength,
        strength_attack_away,
        strength_defence_away,
        strength_attack_home,
        strength_defence_home,
        strength_overall_away,
        strength_overall_home

    FROM {{ ref('stg_teams') }}
),

final AS (
    SELECT * 
    FROM teams
)

SELECT * 
FROM final
