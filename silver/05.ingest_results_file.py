# Databricks notebook source
# Clean-up
# spark.sql("DROP TABLE IF EXISTS formula1_dev.silver.results")

# COMMAND ----------

# Define variables used in code below
file_path = f"/Volumes/formula1_dev/bronze/raw/file_date=*/results.json"
table_name = "formula1_dev.silver.results"
checkpoint_path = "abfss://silver@f1adlsext.dfs.core.windows.net/checkpoint/results_cp"

# Configure Auto Loader to ingest json data to a Delta table

results_df = (spark
                .readStream
                .format('cloudFiles')
                .option('cloudFiles.format', 'json')
                .option('cloudFiles.inferColumnTypes', True)
                .option('cloudFiles.schemaLocation', checkpoint_path)
                .load(file_path)
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, when, col

# COMMAND ----------

results_cleaned = (results_df
                        .withColumnsRenamed({"resultId": "result_id", "raceId": "race_id", 
                                             "driverId": "driver_id", "constructorId": "constructor_id",
                                             "positionText": "position_text","positionOrder": "position_order", "fastestLap": "fastest_lap", "fastestLapTime": "fastest_lap_time", "fastestLapSpeed": "fastest_lap_speed"})
                        .withColumn('ingestion_date', current_timestamp())
                        .dropDuplicates(['race_id', 'driver_id'])
)

# COMMAND ----------

# replacing \\N in dataframe with null
results_trans = results_cleaned\
    .select([when(col(c)=="\\N", None).otherwise(col(c)).alias(c) for c in results_cleaned.columns])

# COMMAND ----------

results_final = results_trans\
    .select('result_id', 'race_id', 'driver_id', 'constructor_id',
            'number', 'grid','position', 'position_text', 'position_order',
            'points', 'laps', 'time', 'milliseconds',
            'fastest_lap', 'fastest_lap_speed', 'fastest_lap_time', 'rank',
            'file_date', 'ingestion_date', '_rescued_data')

# COMMAND ----------

(results_final
    .writeStream
    .option('checkpointLocation', checkpoint_path)
    .trigger(availableNow=True)
    .toTable(table_name)
)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * From formula1_dev.silver.results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM formula1_dev.silver.results

# COMMAND ----------

