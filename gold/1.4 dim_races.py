# Databricks notebook source
# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.functions import col, to_date
input_df = (spark
               .read
               .table("formula1_dev.silver.races")
               .select(col("race_id"), col("year").alias("race_year"), col("name").alias("race_name"),
                       to_date(col("race_timestamp"), "yyyy-MM-dd").alias("race_date"), col("circuit_id"))
               )
tbl_name = "formula1_dev.gold.dim_races"
merge_condition = "tgt.race_id = src.race_id AND tgt.race_name = src.race_name"
merge_delta_tbl(input_df, tbl_name, merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

