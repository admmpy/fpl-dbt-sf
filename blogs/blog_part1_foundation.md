# My FPL Project Part 1: Getting the Data into Snowflake

When I started this project, I just wanted a simple way to get FPL data. I used Python and a tool called Prefect to build a pipeline. 

### Starting Small
At first, I only pulled data for 10 players to see if it worked. I had to make sure my pipeline actually connected to Snowflake before I tried pulling thousands of records. Once I was confident, I updated the script to run the full pipeline for all players.

### Rationale for Typed Tables
Early on, I spent time making sure my Snowflake tables were "typed." This means I told Snowflake exactly what each column was (like INTEGER, VARCHAR, or TIMESTAMP) instead of just dumping everything into a VARIANT blob. 

I even wrote a special Python function that generates the SQL `CREATE TABLE` statements automatically. This saved me from having to write 50 lines of SQL for every new table.

### Learning to "Flatten" Data
The FPL API gives data in JSON format. I saved this as a "VARIANT" in Snowflake at first. But for dbt to properly query it, I had to learn how to use Snowflake's `FLATTEN` function. 

This was a breakthrough because it allowed me to get **historical data** for every player in every gameweek. Before this, I could only see a player's *current* season totals, which isn't useful for week-by-week predictions.

### Documentation
I also wrote a `GETTING_STARTED.md` and updated the `README.md` to document how the pipeline works. I added logs to `.gitignore` so they wouldn't clutter my git repository.

### Things I learned:
*   **Config Files:** I put all my API URLs in a `config.py` file so I didn't have to hardcode them.
*   **Overall League Standings:** I added an extra endpoint to pull league-wide data, not just player data.
