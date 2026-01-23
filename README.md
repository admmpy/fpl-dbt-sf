# FPL Team Optimizer 

A Prefect + dbt + Snowflake pipeline that predicts Fantasy Premier League player points and recommends optimal team selections.

## What This Project Does

- **Ingests** live FPL data from the official API into snowflake using scheduled python script (Prefect)
- **Transforms** raw data through staging → intermediate → marts layers within dbt core
- **Engineers** 20+ features including rolling averages, form indicators, and fixture difficulty
- **Trains** a ML model to predict next-gameweek player points
- **Recommends** an optimised 15-player squad within FPL constraints

## Background

Created a previous project using BigQuery to achieve this with the help of Cursor. Working within an AE space, I wanted to implement best practices from dbt and GCP which was my previous tech stack to help build this project.
BQ repo: https://github.com/admmpy/fpl-dbt-bq

The aim of this project is to once again set up a working pipeline (The files for this are within fpl-pipeline) that can ingest data into snowflake, be transformed within dbt and applied to a ML model to generate a team optimised for the upcoming gameweeks. 

My approach for this projects differs with an ELT approach (previous was ETL) allowing me to take more resposibility with the transformation stage within dbt and Snowflake. Cursor is still being used (Actually Windsurf, since im trialing it - but Cursor is becoming synonimous these days 'IDE with AI models') but only in ASK mode and with a custom instruction set tailored to scaffolding and guidance.

**For some more context around the pipeline for the project, refer to the fpl-pipeline ([https://github.com/admmpy/fpl-dbt-bq](https://github.com/admmpy/fpl-pipeline))***

**To understand the process behind this project better, refer to the `blog` directory**

# Next steps

- Connecting to a BI tool
- Implementing cloud scheduler for pipeline
