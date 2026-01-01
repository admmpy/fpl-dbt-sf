# FPL Team Optimizer

A Prefect + dbt + Snowflake pipeline that predicts Fantasy Premier League player points and recommends optimal team selections.

## What This Project Does

- **Ingests** live FPL data from the official API into snowflake using scheduled python script (Prefect)
- **Transforms** raw data through staging → intermediate → marts layers within dbt core
- **Engineers** 20+ features including rolling averages, form indicators, and fixture difficulty
- **Trains** a ML model to predict next-gameweek player points
- **Recommends** an optimised 15-player squad within FPL constraints

## Background

Created a previous project using BigQuery to achieve this with the help of Cusor. Working in an AE space, I used best practices for dbt and GCP which was my previous tech stack. The aim of this project is to once again set up a working pipeline that can ingest data into snowflake, be transformed within dbt and applied to a ML model to generate a team optimised for the upcoming gameweek. My approach for this projects differs with an ELT approach (previous was ETL) allowing me to take more resposibility with the transformation stage within dbt and Snowflake. Cursor is still being used (Actually Windsurf, since im trialing it - but Cursor is becoming synonimous these days 'IDE with AI models') but only in ASK mode and with a custom instruction set tailored to scaffolding and guidance.

**For some more context around this repo, refer to the fpl-dbt-bq repo**
