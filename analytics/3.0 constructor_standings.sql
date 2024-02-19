-- Databricks notebook source
CREATE TABLE IF NOT EXISTS formula1_dev.analytics.constructor_standings
(
  year INT,
  team STRING,
  total_points INT,
  wins INT,
  rank INT
)
USING DELTA

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW update_constructor_standings
AS 
WITH agg_results AS
(
  SELECT 
    race_year year,
    team,
    sum(points) total_points,
    count(CASE WHEN position = 1 THEN 1 END) wins
  FROM
    formula1_dev.analytics.race_results
  GROUP BY
    race_year, team
    )
SELECT 
  *, 
  dense_rank() OVER (PARTITION BY year ORDER BY total_points DESC, Wins DESC) rank
FROM
  agg_results

-- COMMAND ----------

MERGE INTO formula1_dev.analytics.constructor_standings tgt
USING update_constructor_standings src
  ON (tgt.year = src.year AND tgt.team = src.team)
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")