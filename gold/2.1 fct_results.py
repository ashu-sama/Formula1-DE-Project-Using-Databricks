# Databricks notebook source
# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.functions import col, to_date
input_df = (spark
               .read
               .table("formula1_dev.silver.results")
               .select(col("result_id"), col("race_id"), col("driver_id"), col("constructor_id"), 
                       col("time").alias("race_time"), col("position"), col("points"), col("laps"))
               )
tbl_name = "formula1_dev.gold.fct_results"
merge_condition = "tgt.result_id = src.result_id"
merge_delta_tbl(input_df, tbl_name, merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")