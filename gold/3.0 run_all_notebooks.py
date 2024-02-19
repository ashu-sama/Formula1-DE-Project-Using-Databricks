# Databricks notebook source
v_result = dbutils.notebook.run("1.1 dim_circuits", 0, {})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("1.2 dim_drivers", 0, {})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("1.3 dim_constructors", 0, {})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("1.4 dim_races", 0, {})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.1 fct_results", 0, {})
v_result