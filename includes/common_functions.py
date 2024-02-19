# Databricks notebook source
def merge_delta_tbl(input_df, tbl_name, merge_condition):

    from delta.tables import DeltaTable
    if spark.catalog.tableExists(f"{tbl_name}"):     
        delta_table = DeltaTable.forName(spark, f"{tbl_name}")
        delta_table.alias("tgt") \
            .merge(input_df.alias("src"), merge_condition)\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        input_df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(f"{tbl_name}")

# COMMAND ----------

