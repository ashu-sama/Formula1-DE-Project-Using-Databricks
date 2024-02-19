# Databricks notebook source
# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.functions import col
input_df = (spark
               .read
               .table("formula1_dev.silver.circuits")
               .select(col("circuit_id"), col("name").alias("circuit_name"), 
                       col("location").alias("city"), col("country"))
               )
tbl_name = "formula1_dev.gold.dim_circuits"
merge_condition = "tgt.circuit_id = src.circuit_id"
merge_delta_tbl(input_df, tbl_name, merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")