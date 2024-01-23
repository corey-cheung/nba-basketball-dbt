# nba-basketball-dbt-dbt

**The transformation part of an end-to-end data analytics pipeline using basketball data from the NBA.**

Repository for the ingestion of NBA basketball data from the public [Ball don't lie API.](https://www.balldontlie.io/home.html#introduction) This repo is focussed on data transformation using [dbt-core] (https://docs.getdbt.com/docs/introduction) and [DuckDb.] (https://duckdb.org/)

In the `nba-basketball-ingestion` repo, data is retrieved from a public API and ingested into a local Postgres database using Python scripting. Tables from Postgres are then ingested into DuckDb using [Airbyte.] (https://airbyte.com) Further downstream, the transformed data will be used for analysis & visualisation in the `nba-basketball-analytics` repo.
