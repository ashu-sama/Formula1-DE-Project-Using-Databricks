-- Databricks notebook source
CREATE TABLE IF NOT EXISTS formula1_dev.analytics.race_results
(
  race_id INT,
  race_year INT,
  race_name STRING,
  race_date DATE,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  race_time STRING,
  points INT,
  position INT
)
USING DELTA

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW update_race_results
AS
(
  SELECT
    rac.race_id, rac.race_year, rac.race_name, rac.race_date,
    concat(cir.city, ", ",cir.country) AS circuit_location,
    dri.driver_name, dri.driver_number, dri.driver_nationality,
    con.team,
    res.race_time, res.points, res.position
  FROM
    formula1_dev.gold.fct_results res
    JOIN formula1_dev.gold.dim_races rac ON res.race_id = rac.race_id
    JOIN formula1_dev.gold.dim_circuits cir ON cir.circuit_id = rac.circuit_id
    JOIN formula1_dev.gold.dim_drivers dri ON dri.driver_id = res.driver_id
    JOIN formula1_dev.gold.dim_constructors con ON con.constructor_id = res.constructor_id
)

-- COMMAND ----------

MERGE INTO formula1_dev.analytics.race_results tgt
USING update_race_results src
ON (tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name)
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")