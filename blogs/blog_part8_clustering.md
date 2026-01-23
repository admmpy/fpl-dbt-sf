# My FPL Project Part 8: Snowflake clustering (notes from a junior AE)

Part 7 got the ML loop working end-to-end. Part 8 is me being a bit more disciplined: if this runs weekly for years, I do not want queries getting slower (or more expensive) just because tables grew.

## What clustering is (in Snowflake terms)

Snowflake stores data in micro-partitions. If the rows I filter on are spread everywhere, Snowflake has to scan more partitions than it should. A clustering key helps keep “similar” rows closer together, so time-bound filters and common joins scan less.

This is not free: clustering adds maintenance overhead. So I only used it where query patterns are predictable and tables will get large.

## How I chose clustering keys

- **Start with the most common filter**: for this project it is almost always `gameweek_id`.
- **Add the entity second**: `player_id` or `team_id`, based on join keys.
- **Keep it small**: 2 columns per table unless there is a strong reason.
- **Do not bother on small tables**: the overhead is not worth it.

## Where I added clustering (8 models)

### Fact tables
- **`fct_players_gameweek`**: `gameweek_id, player_id`  
  Largest table; most queries slice by gameweek ranges and then drill into players.

- **`fct_team_fixtures`**: `gameweek_id, team_id`  
  Same pattern, but team-level.

- **`fct_model_analysis`**: `gameweek_id, player_id`  
  Model performance is analysed over time, then by player.

### ML features
- **`fct_ml_player_features`**: `gameweek_id, player_id`  
  Training extracts are almost always a gameweek range (temporal split), and the table is wide.

### Intermediate models
- **`int_player_rolling_stats`**: `gameweek_id, player_id`  
  This joins downstream on the same composite keys.

- **`int_team_rolling_stats`**: `gameweek_id, team_id`

### Reporting models
- **`rprt_squad_performance_comparison`**: `gameweek_id, player_id`  
  Dashboards tend to focus on “last N gameweeks”.

- **`rprt_squad_performance_summary`**: `gameweek_id, recommended_at`  
  This supports time-series views plus “recent recommendations”.

## What I did not cluster

- **Dimensions** (`dim_players`, `dim_teams`, etc.): they are small and usually joined by keys, so clustering is noise.
- **Staging views** (`stg_*`): views cannot be clustered.

## dbt implementation

In each model config:

```sql
{{
    config(
        materialized='incremental',
        cluster_by=['gameweek_id', 'player_id']
    )
}}
```

## How I plan to monitor it

I’ll keep an eye on clustering depth/overlap and adjust keys if query patterns change:

```sql
SELECT SYSTEM$CLUSTERING_DEPTH(
    'fpl_db.fpl_marts.fct_players_gameweek',
    '(gameweek_id, player_id)'
);

SELECT SYSTEM$CLUSTERING_INFORMATION(
    'fpl_db.fpl_marts.fct_players_gameweek',
    '(gameweek_id, player_id)'
);
```

## Quick takeaways

- **Clustering order matters**: put the thing you filter on most first (`gameweek_id` for me).
- **Match access patterns**: I clustered around `WHERE gameweek_id ...` and joins on `(player_id, gameweek_id)` / `(team_id, gameweek_id)`.
- **Document decisions**: future me needs to know why keys were chosen.

---

**Files modified (clustering added):**
- `fct_players_gameweek.sql`
- `fct_team_fixtures.sql`
- `fct_model_analysis.sql`
- `fct_ml_player_features.sql`
- `int_player_rolling_stats.sql`
- `int_team_rolling_stats.sql`
- `rprt_squad_performance_comparison.sql`
- `rprt_squad_performance_summary.sql`
