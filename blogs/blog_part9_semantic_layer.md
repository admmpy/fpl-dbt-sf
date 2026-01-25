# My FPL Project Part 9: Semantic Layer & BI Exposures

I really enjoyed this part as I got to implement a semantic layer. I knew about what a semantic layer was from my previous exposure as an analyst using applications such as Lightdash, but for this project, I was directly responsible for setting up not only the semantic layer but also connecting the data to BI tools (Looker Studio and Lightdash).

## Planning and Learning

Before starting, I went over the main benefits of implementing a semantic layer with the AI to make sure my understanding was correct. This was also a good chance to prompt and test the LLM on how it should be designed and built, using previous knowledge not only from work experience but from videos I've seen, such as segments from dbt Coalesce where others shared their processes and use cases.

The key motivations solidified into:
- **Centralised Definitions:** No more debating how "points per gameweek" is calculated—it's defined once in code.
- **Consistent Governance:** Whether I'm querying in Lightdash or asking an LLM for data, the underlying logic is identical.
- **AI Readiness:** The semantic layer translates raw table structures into business concepts an AI can reliably use.

## What Was Implemented

I started by structuring the layer logically, separating entities from metrics:

1.  **Entities (Dimensions):** Created `semantic_players.yml` and `semantic_teams.yml` to define the core "nouns" of the data model.
2.  **Performance Model:** Built `semantic_player_performance.yml` on top of `fct_players_gameweek`. This houses the "verbs" (measures and metrics).
3.  **Metrics:** Implemented governed definitions for:
    - `total_points`
    - `points_per_gameweek`
    - `goals_per_90`

I also added `exposures.yml` to explicitly link these dbt models to the downstream Lightdash and Looker Studio dashboards, improving impact analysis.

## Challenges and Adjustments

It wasn't all smooth sailing. I hit a few technical blockers that forced me to refine the implementation:

-   **Time Spine Requirement:** I initially missed that MetricFlow (the engine behind the semantic layer) effectively *requires* a continuous time spine for time-series operations. I had to implement a `metricflow_time_spine` model (daily granularity) to support this.
-   **Strict Dimension Typing:** I learned the hard way that the semantic layer is strict about types. I initially tried to define numeric columns (like team strength) as `numeric` dimensions, but dbt requires them to be `categorical` if they are used for slicing/grouping.
-   **Metric Logic Nuances:** Defining `points_per_gameweek` required care. Simply counting rows would double-count gameweeks for players with two fixtures in a Double Gameweek. I had to ensure the metric used `count_distinct` on the gameweek ID to align with how FPL actually works.
-   **dbt 1.11 Syntax:** I encountered some deprecated syntax warnings regarding the time spine configuration and had to update the YAML structure to align with the latest dbt 1.11 standards.

## Conclusion

Connecting Lightdash to this new layer was a "lightbulb moment"—metrics just appeared, pre-defined and correct. It validated the effort of moving logic out of the BI tool and into the codebase. Looking to implement AI capabilities (outside of Lightdash's built in AI features) to query data with natural language either through dbt-MCP or LangChain.

---

**Files modified:**
- `models/semantic_models/semantic_players.yml`
- `models/semantic_models/semantic_teams.yml`
- `models/semantic_models/semantic_player_performance.yml`
- `models/semantic_models/metricflow_time_spine.sql`
- `models/semantic_models/metricflow_time_spine.yml`
- `models/exposures.yml`
