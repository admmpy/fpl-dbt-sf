## Fixing Reschedules + Double Gameweeks Without Weakening Data Contracts

  I spent this branch (fix/double-gameweek-troubles) fixing a reliability gap that shows up when FPL scheduling gets messy:

  - Double gameweeks create valid duplicate player_id + gameweek_id pairs.
  - Reschedules can temporarily leave fixtures with gameweek_id = null.

  The hard part was keeping standards high without silencing useful tests.

  ### What I changed

  1. Aligned uniqueness tests with true grain

  - Updated uniqueness logic to include fixture_id where the model is fixture-grain.
  - Most importantly, ML feature testing now reflects that one player can have multiple fixtures in one gameweek.

  2. Stopped treating upstream schedule volatility as a hard failure

  - Replaced upstream handling with a warn-only monitor:
      - Added tests/source_fixtures_unscheduled_count_warn.sql
      - Added threshold vars in dbt_project.yml
  - This keeps visibility when unscheduled fixtures spike, without breaking the whole build for valid real-world behavior.

  3. Introduced a scheduled-fixture contract model

  - Created a scheduled-only fixture model and then changed it to int_* (int_fixtures_scheduled) instead of stg_*.
  - Updated downstream references (for example in fct_team_fixtures) to use this curated model.

  4. Kept strict integrity where it matters

  - I did not relax strict not-null/FK expectations in ML and reporting contracts.
  - I moved the contract boundary earlier so downstream models can remain strict and trustworthy.

  ### Challenges I faced

  - Grain confusion surfaced in tests: a uniqueness test at gameweek grain was applied to fixture-grain data.
  - Naming/contract semantics: I initially used stg_fixtures_scheduled, then corrected to int_fixtures_scheduled because it is curated logic, not raw-shaped staging.
  - dbt1005 warning: forgot to uupdate the name of the model in the yml to reflect change (stg_* vs int_*).

  ### My rationale

  I wanted to avoid two bad outcomes:

  - fragile pipelines that fail on valid reschedules
  - weak downstream quality checks that hide real problems

  So I separated concerns:

  - Upstream/source-truth layers can reflect nullable gameweek_id.
  - Curated model + downstream analytical layers enforce strict not_null and relationships.

  ### Data quality checks I ran (Snowflake)

  I validated that filtering logic was precise, not over-filtering:

  1. Count parity checks between non-null scheduled source and scheduled boundary.
  2. Anti-joins to confirm:

  - no scheduled fixtures missing in fct_team_fixtures
  - no unscheduled fixtures leaking into fct_team_fixtures

  3. Grain check: each scheduled fixture expands to exactly two team rows in fct_team_fixtures.
  4. Equivalent anti-join reconciliation for fct_players_gameweek vs stg_player_history ∩ int_fixtures_scheduled.

  All reconciliation queries returned the expected outcomes (no unexpected rows).

  ### What I learned

  - Test policies must follow data contracts, not assumptions.
  - “One model to do everything” creates contradictions when source reality is volatile.
  - The clean pattern is:
      - raw/staging truth
      - explicit contract boundary
      - strict downstream enforcement

  ### End result

  I now have a pipeline that:

  - handles double gameweeks correctly,
  - tolerates unpredictable rescheduling upstream,
  - preserves strict downstream integrity for ML and reporting,
  - and surfaces schedule anomalies through monitoring instead of noisy hard-fail behavior.