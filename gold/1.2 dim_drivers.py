# Databricks notebook source
# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.functions import col
input_df = (spark
               .read
               .table("formula1_dev.silver.drivers")
               .select(col("driver_id"), col("driver_ref"), col("driver_name"), col("number").alias("driver_number"),
                       col("nationality").alias("driver_nationality"))
               )
tbl_name = "formula1_dev.gold.dim_drivers"
merge_condition = "tgt.driver_id = src.driver_id AND tgt.driver_ref = src.driver_ref"
merge_delta_tbl(input_df, tbl_name, merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")