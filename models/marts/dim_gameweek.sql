WITH gameweek AS (
    SELECT
        gameweek_id,
        is_current,
        deadline_time,
        is_finished,
        is_next,
        is_previous,
        average_entry_score,
        highest_score,
        most_selected,
        most_transferred_in,
        most_captained,
        most_vice_captained,

    FROM {{ ref('stg_gameweeks') }}
),

final AS (
    SELECT *
    FROM gameweek
)

SELECT * 
FROM final