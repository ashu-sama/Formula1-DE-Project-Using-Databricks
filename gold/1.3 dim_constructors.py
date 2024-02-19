# Databricks notebook source
# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.functions import col
input_df = (spark
               .read
               .table("formula1_dev.silver.constructors")
               .select(col("constructor_id"), col("constructor_ref"), col("name").alias("team"))
               )
tbl_name = "formula1_dev.gold.dim_constructors"
merge_condition = "tgt.constructor_id = src.constructor_id"
merge_delta_tbl(input_df, tbl_name, merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")