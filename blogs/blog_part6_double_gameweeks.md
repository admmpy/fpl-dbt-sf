# My FPL Project Part 6: Double gameweeks (bug fixes + grain lessons)

After building what I thought was a solid pipeline, I decided to do a thorough code review. That's when I discovered a series of critical bugs that would have caused major issues once double gameweeks started happening.

## What a double gameweek breaks

A player (or team) can have **two fixtures in one gameweek**. If my models assume “one row per player per gameweek”, I either get duplicates or wrong rolling windows.

## Fix 1: fact table primary key was not unique

In `fct_players_gameweek` my surrogate key only used `player_id` + `gameweek_id`. That fails the moment a player has two fixtures in the same gameweek.

- **Before**: `player_id, gameweek_id`
- **After**: `player_id, gameweek_id, fixture_id`

```sql
{{ dbt_utils.generate_surrogate_key(['player_id', 'gameweek_id', 'fixture_id']) }}
```

## Fix 2: rolling stats were calculated at the wrong grain

`int_player_rolling_stats` was trying to produce gameweek-level “form”, but it was fed fixture-level rows. The window frames were “rows”, not “gameweeks”, so double gameweeks effectively shortened the history.

What I changed:

- **Step 1**: aggregate fixture rows to **one row per player per gameweek**
- **Step 2**: run rolling windows on that aggregated dataset

```sql
gameweek_aggregated AS (
    SELECT
        player_id,
        gameweek_id,
        SUM(total_points)            AS total_points,
        SUM(minutes_played)          AS total_minutes_played,
        COUNT(DISTINCT fixture_id)   AS fixtures_played
    FROM base_data
    GROUP BY 1, 2
)
```

## Fix 3: team rolling stats (same grain issue + semantics)

`int_team_rolling_stats` had the same “rows vs gameweeks” problem, plus I tightened definitions:

- **Clean sheets**: treat as a fixture concept (opponent scored 0), not a sum of player flags.
- **Win percentage**: compute wins at fixture level, then average across fixtures (so a 1–1 split in a double gameweek becomes 0.5).


## NULL Handling

While reviewing, I also noticed that if `dim_teams` was missing a team (unlikely but possible), the strength metrics would be NULL. This would break any downstream analytics that assumed those fields were always populated.

**The Fix:**
```sql
COALESCE(
    CASE 
        WHEN was_home THEN strength_defence_away
        ELSE strength_defence_home
    END,
    1000  -- League average
) AS opponent_defence_strength
```

Now if a team is missing, we use 1000 (the FPL league average) as a sensible default.

## The Testing Strategy

I created comprehensive test queries to validate the fixes:

```sql
-- Verify no duplicate keys in rolling stats
SELECT 
    player_id,
    gameweek_id,
    COUNT(*) AS row_count
FROM analytics.int_player_rolling_stats
WHERE gameweek_id = <test_double_gameweek>
GROUP BY 1, 2
HAVING COUNT(*) > 1;  -- Should return 0 rows
```

## Documentation: Write It Down

I created detailed documentation of all bugs and fixes in technical summary documents.

**Why document bugs?**
- Future me will forget the reasoning
- Other developers can learn from the mistakes
- It forces you to think through edge cases

## Updated YML Files

I updated all the schema YML files to reflect the new grain:

```yaml
# fct_players_gameweek.yml
tests:
  - dbt_utils.unique_combination_of_columns:
      combination_of_columns:
        - player_id
        - gameweek_id
        - fixture_id  # Added this

columns:
  - name: player_gameweek_key
    description: Primary key generated from player_id, gameweek_id, 
                 and fixture_id to handle double gameweeks
```

Tests aren't just for finding bugs - they document your assumptions about the data.

## What I Learned

### 1. Always Think About Edge Cases
Double gameweeks are rare (maybe 3-4 per season), but they **will** happen. Designing for edge cases from the start prevents painful bugs later.

### 2. Grain Matters More Than You Think
The "grain" of your table (what does one row represent?) must be crystal clear. If you pull from fixture-level and output gameweek-level, aggregate explicitly.

### 3. Window Functions Are Tricky
`ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING` doesn't mean "3 gameweeks" - it means "3 rows". If you have multiple rows per gameweek, your calculation is wrong.

### 4. Simple > Clever
I could have built a complex fixture-level rolling system with dynamic joins. Instead, I aggregated to gameweek level first. The simpler solution is almost always better.

### 5. Test Your Assumptions
I thought my rolling stats were correct until I manually traced through a double gameweek scenario. Always validate with real examples.

### 6. Document Everything
Writing the bug summary forced me to think deeply about the fixes. Good documentation isn't just for others - it's for future you.

## The Results

After the fixes:
- 6 files modified
- 4 critical bugs fixed
- 107 lines changed
- All tests passing
- Zero duplicate key risks
- Mathematically correct rolling averages


## The Pre-Production Checklist

Before deploying:
```bash
# Full refresh the fact tables (breaking changes to keys)
dbt run --full-refresh --select fct_players_gameweek fct_team_fixtures

# Run intermediate and ML models
dbt run --select int_player_rolling_stats+ int_team_rolling_stats+ fct_ml_player_features

# Run all tests
dbt test --select fct_players_gameweek+ fct_team_fixtures+ int_player_rolling_stats+
```

## Next Steps

With the double gameweek bugs fixed, I'm now ready to build:
1. A fact table for model prediction analysis (tracking accuracy over time)
2. Reporting models for visualization in BI tools
---

**Files Changed:**
- `fct_players_gameweek.sql` - Fixed surrogate key, added NULL handling
- `fct_team_fixtures.sql` - Fixed surrogate key
- `int_player_rolling_stats.sql` - Added gameweek aggregation
- `int_team_rolling_stats.sql` - Added gameweek aggregation, fixed win/clean sheet logic
- All corresponding `.yml` files - Updated tests and documentation

**Lesson:** The best bugs are the ones you catch before they cause problems. A thorough code review saved me from lots of potential debugging during the season.
