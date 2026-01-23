# My FPL Project Part 7: Completing the Loop - From ML Training to Squad Optimization

After fixing the critical double gameweek bugs in Part 6, the foundation was solid. But a foundation isn't a house. I still needed to connect the dots: training an actual Machine Learning model, generating predictions, optimizing a squad, and saving those results back to the database for analysis.

This was the final sprint to turn a "data pipeline" into an "intelligence system."

## 1. Cleaning House: Project Restructuring

Before diving into the complex ML logic, I realized my `pipeline/` directory was becoming a dumping ground for scripts, documentation, and random text files. To keep things production-ready, I reorganized the entire structure:

```
pipeline/
├── docs/                     # Documentation (Architecture, ML, etc.)
├── flows/                    # Prefect orchestration flows
├── logs/                     # Model artifacts (model.bin) and metrics
├── resources/                # Static resources (endpoints.txt)
├── scripts/                  # Entry points (train_model.py, run_once.py)
├── tasks/                    # Atomic pipeline operations
└── config.py                 # Core configuration
```

Moving `model.bin` and `model.metrics.txt` into a dedicated `logs/` folder was a small but crucial change. It keeps the root clean and separates code from artifacts.

## 2. Building the Brain: XGBoost Training

I created `pipeline/scripts/train_model.py` to handle the ML heavy lifting. Instead of a simple heuristic, I implemented a proper XGBoost regression model.

### Feature Engineering
The model learns from `fct_ml_player_features` (the dbt model we built earlier). I engineered 45+ features, including:
- **Rolling Stats:** 3-week and 5-week averages for points, goals, and xG.
- **Context:** Opponent strength, team form, and home/away advantage.
- **Z-Scores:** Normalized stats (e.g., how good is this player's form compared to the league average *this week*?).

```python
# Z-Score Normalization (relative to gameweek)
df[f'{feature}_z_score'] = (df[feature] - gw_mean) / gw_std
```

### Time Series Validation
Since FPL data is temporal, standard cross-validation fails (you can't train on Week 10 to predict Week 5). I used `TimeSeriesSplit` to respect the timeline.

```python
tscv = TimeSeriesSplit(n_splits=5)
cv_scores = cross_val_score(model, X, y, cv=tscv, ...)
```

The result? An MAE (Mean Absolute Error) of around ~2.1 points. Not magic, but better than random guessing.

## 3. The Missing Link: Closing the Loop to Snowflake

I had the model, and I had the optimizer. But when I ran the pipeline, it crashed trying to save the recommended squad.

### The Error
```
SQL compilation error: Object 'RECOMMENDED_SQUAD' does not exist.
```

I had forgotten the "Schema-First" rule. I was trying to load data into a table that I hadn't defined in my configuration.

### The Fix
I added the `RECOMMENDED_SQUAD_SCHEMA` to `config.py` and updated the orchestration flow to ensure the table exists before loading.

```python
# config.py
RECOMMENDED_SQUAD_SCHEMA = {
    "recommendation_key": "VARCHAR(100) NOT NULL",
    "recommended_at": "TIMESTAMP_NTZ",
    "player_id": "INTEGER",
    "expected_points_next_gw": "FLOAT",
    "is_in_squad": "BOOLEAN",
    "is_captain": "BOOLEAN",
    # ...
}
```

Now, the pipeline automatically creates the table if it's missing.

## 4. Analysis Layer: Checking My Homework

Saving the squad is useless if we don't track how well it performs. I built a new dbt mart: `fct_model_analysis`.

This model joins the `stg_recommended_squad` (predictions) with `fct_players_gameweek` (actuals) to calculate accuracy metrics.

```sql
SELECT
    fp.player_id,
    fp.gameweek_id,
    fp.predicted_points,
    ap.actual_points,
    ABS(ap.actual_points - fp.predicted_points) AS absolute_error,
    ap.actual_points - fp.predicted_points      AS error_bias
FROM final_predictions AS fp
INNER JOIN actual_performance AS ap ...
```

**Why this matters:**
- **absolute_error**: Tells me how "wrong" the model is on average.
- **error_bias**: Tells me if the model is too optimistic (positive bias) or pessimistic (negative bias).

## 5. The Full Workflow

Now, the system works in a complete loop:

1.  **Ingestion**: Fetch data from FPL API -> Snowflake.
2.  **Transformation**: dbt builds `fct_ml_player_features`.
3.  **Training**: `train_model.py` learns from history -> saves `model.bin`.
4.  **Inference**: Pipeline loads `model.bin`, predicts next week's points.
5.  **Optimization**: Linear programming picks the best 15 players (max points, valid formation, budget cap).
6.  **Loading**: Saves squad to `recommended_squad` in Snowflake.
7.  **Analysis**: dbt builds `fct_model_analysis` to grade the performance.

## What I Learned

### 1. Artifact Management is Key
Hardcoding paths like `model.bin` works for a script, but breaks in a pipeline. Moving artifacts to a dedicated `logs/` directory and using relative paths made the system robust.

### 2. Schema-First or Bust
It's tempting to just "dump JSON" and figure it out later. But defining the schema for `recommended_squad` upfront forced me to decide exactly what data I needed (e.g., including `now_cost` and `position_id` in the output saved me a join later).

### 3. The "Cold Start" Problem
The ML model needs history (rolling stats). I had to explicitly drop Gameweeks 1-3 from training because you can't calculate a 3-week rolling average in Week 1. Handling this edge case prevents the model from learning garbage noise.

### 4. Separate Training from Inference
I separated `train_model.py` (ad-hoc, heavy compute) from the daily pipeline. The pipeline just loads the saved model. This makes the daily run fast and lightweight.

## The Result

The pipeline is complete. It ingests data, transforms it, learns from it, makes decisions, and tracks its own performance.

The next step? 
- Building a dashboard to visualize these insights 
- Using the team to win my mini-league.

---

**Files Created/Modified:**
- `pipeline/scripts/train_model.py`: XGBoost training logic.
- `pipeline/tasks/ml_tasks.py`: Updated for inference using saved model.
- `pipeline/config.py`: Added `recommended_squad` schema.
- `fpl_development/models/marts/core/fct_model_analysis.sql`: Performance tracking.
- `pipeline/docs/ML_TRAINING.md`: Documentation for the new ML workflow.
