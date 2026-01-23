# My FPL Project Part 2: Cleaning the Data and Fixing Bugs

Once the data was in Snowflake, I started using dbt to clean it. This is where I learned that messy data will break everything.

### File Organization
I created a proper folder structure with separate directories for `staging/`, `marts/core/`, `marts/Int/`, and `marts/ml/`. This keeps the project organized and makes it clear which models feed into which.

### Catching Data Errors
One bug I found was in my fixture data where it said Arsenal drew 5-5 with Leeds in Gameweek 1. Obviously wrong! I fixed my staging logic to properly pull home and away scores.

### The "Ghost Players" Problem
I had 10 player IDs (770-779) that had zero stats. These were probably players who transferred out or retired. They were causing all my `not_null` tests to fail.

**My fix:** I used an `INNER JOIN` instead of a `LEFT JOIN` so I only kept players that exist in my main `dim_players` table. This immediately fixed dozens of test failures.

### Handling Future Gameweeks
I also had to remove a `not_null` test on scoring fields because future gameweeks that haven't been played yet are legitimately `NULL`. You can't test for "not null" on data that doesn't exist yet!

### Deduplication
At one point, I had duplicate records in `stg_player_history` from old ingestion runs. I fixed this by adding a `QUALIFY ROW_NUMBER() = 1` filter to keep only the latest ingestion per player/gameweek.

### Small Consistency Fixes
*   Changed `has_finished` to `is_finished` for better consistency across all my boolean fields.
*   Divided player costs by 10 (from £100 to £10.0) to match how the FPL website displays prices.
*   Added a test to make sure player values can't be negative.
*   Removed trailing commas that were breaking SQL in some models.

### Rationale for Surrogate Keys
To uniquely identify each row, I created a "surrogate key" by hashing `player_id` and `gameweek_id` together using the `dbt_utils.generate_surrogate_key` macro. This became critical for my incremental models.

### The Big Win: Incremental Models
Finally, I converted `fct_players_gameweek` from a regular `table` to `incremental` with a `merge` strategy. 

**Why?** Rebuilding 15,000+ rows every run was getting expensive and slow. Now, dbt looks back 7 days, finds any changes (like late bonus points), and only updates those rows. 

I also added an `updated_at` timestamp so I always know when the data was last refreshed.

### Fixing Time-Series Logic
I adjusted the `PARTITION BY` clause in `stg_player_history` to include `gameweek_id`, not just `player_id`. This was necessary to properly handle the time-series nature of the data.
