# Databricks notebook source
# Clean-up
# spark.sql("DROP TABLE IF EXISTS formula1_dev.silver.qualifying")

# COMMAND ----------

# Define variables used in code below
file_path = f"/Volumes/formula1_dev/bronze/raw/file_date=*/qualifying"
table_name = "formula1_dev.silver.qualifying"
checkpoint_path = "abfss://silver@f1adlsext.dfs.core.windows.net/checkpoint/qualifying_cp"

# Configure Auto Loader to ingest json data to a Delta table

qualifying_df = (spark
                    .readStream
                    .format('cloudFiles')
                    .option('cloudFiles.format', 'json')
                    .option('multiLine', True)
                    .option('cloudFiles.inferColumnTypes', True)
                    .option('cloudFiles.schemaLocation', checkpoint_path)
                    .load(file_path))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, when, expr

# COMMAND ----------

qualifying_final = (qualifying_df
                            .withColumn('ingestion_date', current_timestamp())
                            .withColumn('q1', when(col('q1')=="\\N", None).otherwise(col('q1')))
                            .withColumn('q2', when(col('q2')=="\\N", None).otherwise(col('q2')))
                            .withColumn('q3', when(col('q3')=="\\N", None).otherwise(col('q3')))
                            .select(col('qualifyId').alias('qualify_id'), col('raceId').alias('race_id'), col('driverId').alias('driver_id'),
                                    col('constructorId').alias('constructor_id'), 'number', 'position', 'q1', 'q2', 'q3',
                                    'file_date', 'ingestion_date', '_rescued_data')
)


# COMMAND ----------

(qualifying_final.writeStream
        .option('checkpointLocation', checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)
        )

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_dev.silver.qualifying

# COMMAND ----------

