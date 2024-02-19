# Databricks notebook source
v_result = dbutils.notebook.run("1.1 race_results", 0, {})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("1.2 calculated_race_results", 0, {})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.0 driver_standings", 0, {})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.0 constructor_standings", 0, {})
v_result