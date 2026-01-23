# My FPL Project Part 3: Intermediate Models and Rolling Averages

Now that the data was clean, I needed to make it useful for Machine Learning. I created some "intermediate" models (`int_`) to handle the calculations. To preface this section, I have no experinece with ML in production but understand what is feature engineering and common supervised learning algorithms i.e. regression and classification.

### Rationale for Rolling Averages
I built models to calculate 3-week and 5-week "rolling averages" for player points. This captures "form" better than just looking at last week.

**The Data Leakage Trap:** At first, I accidentally included the current gameweek in the average. This is called "data leakage" because you're using today's result to predict today's result. 

**My Fix:** I used a window function with `ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING` (in descending order) so the average only looks at *previous* weeks.

### Learning from Mistakes
I created the `int_player_rolling_stats` model but forgot to create a `.yml` file for it at first. I caught this mistake when I ran `dbt test` and realized there were no tests running for that model. I went back and added proper documentation and tests.

I also created some empty intermediate models that I thought I'd need, but later realized they were unnecessary. I deleted them to keep the project clean.

### Historical Snapshots
The FPL API only shows you a player's *current* price and form. But for ML, I need to know what their form was *three weeks ago*.

I created `stg_players_gameweek_snapshot` that records:
*   `form` (the FPL "form" metric)
*   `status` (Available, Injured, Suspended, etc.)
*   `now_cost` (their price at that specific point in time)

This gives the ML model a "time machine." I used `QUALIFY ROW_NUMBER()` to keep only the latest ingestion per player/gameweek to avoid duplicates.

### Team-Level Analysis
I realized I needed to analyze teams, not just players. I created:
1.  **`fct_team_fixtures`:** Every team's fixture with their league position and whether they were home/away.
2.  **`int_team_rolling_stats`:** Rolling averages of goals scored, expected goals (xG), and clean sheets for each team.

This allows me to include "team context" in the final ML features.

### Adding Context: Team and Opponent Strength
In `fct_players_gameweek`, I joined with `dim_teams` to add:
*   **Team Attack Strength:** How good is the player's team at scoring?
*   **Opponent Defence Strength:** How good is the opponent at preventing goals?

I also had to determine the `team_id` dynamically based on whether the player was home or away in that fixture. This required a `CASE` statement checking `was_home`.

### Dimension for BI Reporting
I created `dim_positions` as a simple lookup table (GK, DEF, MID, FWD) with tests. This makes it easier for anyone using a BI tool to understand the data without needing to remember that `position_id = 3` means "Midfielder."
