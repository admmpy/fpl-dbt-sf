# My FPL Project Part 4: The Final ML Table and Data Quality

In the last part of my dbt work, I iteratively built `fct_ml_player_features`, adding more context with each version.

### Iteration 1: Basic Performance + Player Rolling Stats
I started simple. I joined `fct_players_gameweek` with `int_player_rolling_stats` to get:
*   Basic stats (goals, assists, minutes)
*   3 and 5-week rolling averages
*   Total games played

This gave me a baseline ML table.

### Iteration 2: Adding Team Context
Next, I realized that player performance depends heavily on their team. I joined `int_team_rolling_stats` to include:
*   Team's 3-week rolling average for goals scored
*   Team's 3-week rolling average for expected goals (xG)
*   Team's 3-week rolling average for clean sheets

I also pulled in the team's current league position to see if top-of-the-table teams behave differently.

### Iteration 3: Historical Snapshots
The breakthrough came when I added the player snapshot data. Now, for every gameweek, I have:
*   The player's **form** at that exact moment
*   Their **status** (was he injured 3 weeks ago?)
*   Their **cost** at that point in time

This is critical because you can't train a model on "current" data and expect it to work on historical patterns.

### Data Validation
I added "accepted range" tests to make sure the data makes sense:
*   `gameweek_id` must be between 1 and 38 (the length of a season)
*   `team_position` must be between 1 and 20 (the number of teams in the league)
*   Player `position_id` must be between 1 and 4 (GK, DEF, MID, FWD)

These tests catch bugs early before they get into the ML model.

### Rationale for Normalization (Z-Scores)
I learned about "Feature Drift." If the whole league starts scoring more goals (which happens some seasons), raw point totals become misleading. 

To fix this, I plan to use **Z-Scores** in my Python script. This compares a player's points to the average of just that one gameweek. I'll also only include players who actually played (`minutes > 0`) so bench players don't skew the average.

### Choosing an Optimizer
For actually picking the team, I learned that an ML model shouldn't directly output the "best 11." I have constraints:
*   Budget: £100m
*   Squad: 15 players (2 GK, 5 DEF, 5 MID, 3 FWD)
*   Max 3 players per team

I'm planning to use **`cvxpy`** to solve this as a Mixed-Integer Linear Programming (MILP) problem. This was suggested by a work friend who was also interested in project.

### The 5-Week Plan
My rationale is to optimize for the next **5 weeks**, not just one. This prevents the model from suggesting a player who has one easy game followed by four nightmares.

I'll use the ML model to predict "Baseline Form" and then apply a simple SQL multiplication for the fixture difficulty (based on opponent defence strength).

---

It's been a long journey from just pulling 10 players from an API to building this warehouse with 15 models, 255 tests, and proper incremental logic. The next step is connecting it all with Prefect so it runs automatically every gameweek!
