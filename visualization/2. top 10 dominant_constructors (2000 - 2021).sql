-- Databricks notebook source
SELECT 
  team,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  ROUND(AVG(calculated_points), 3) AS avg_points
FROM 
  formula1_dev.analytics.calculated_race_results
WHERE
  race_year BETWEEN 2000 AND 2021
GROUP BY 
  team
HAVING 
  COUNT(1) >= 100
ORDER BY 
  avg_points DESC

-- COMMAND ----------

