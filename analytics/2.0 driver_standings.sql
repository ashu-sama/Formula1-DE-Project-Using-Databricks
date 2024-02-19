-- Databricks notebook source
CREATE TABLE IF NOT EXISTS formula1_dev.analytics.driver_standings
(
  year INT,
  driver STRING,
  nationality STRING,
  total_points INT,
  wins INT,
  rank INT
)
USING DELTA

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW update_driver_standings
AS 
WITH agg_results AS
(
  SELECT 
    race_year year,
    driver_name driver,
    driver_nationality nationality,
    sum(points) total_points,
    count(CASE WHEN position = 1 THEN 1 END) wins
  FROM
    formula1_dev.analytics.race_results
  GROUP BY
    race_year, driver_name, driver_nationality
    )
SELECT 
  *, 
  dense_rank() OVER (PARTITION BY Year ORDER BY total_points DESC, Wins DESC) rank
FROM
  agg_results

-- COMMAND ----------

MERGE INTO formula1_dev.analytics.driver_standings tgt
USING update_driver_standings src
  ON (tgt.year = src.year AND tgt.driver = src.driver AND tgt.nationality = src.nationality)
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")