# Databricks notebook source
# Clean-up
# spark.sql("DROP TABLE IF EXISTS formula1_dev.silver.pit_stops")

# COMMAND ----------

# Define variables used in code below
file_path = f"/Volumes/formula1_dev/bronze/raw/file_date=*/pit_stops.json"
table_name = "formula1_dev.silver.pit_stops"
checkpoint_path = "abfss://silver@f1adlsext.dfs.core.windows.net/checkpoint/pit_stops_cp"

# Configure Auto Loader to ingest csv data to a Delta table

pit_stops_df = (spark
                    .readStream
                    .format('cloudFiles')
                    .option('cloudFiles.format', 'json')
                    .option("multiLine", True)
                    .option('cloudFiles.inferColumnTypes', True)
                    .option('cloudFiles.schemaLocation', checkpoint_path)
                    .load(file_path)
    )

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_cleaned = pit_stops_df\
    .withColumnsRenamed({'driverId': 'driver_id',
                         'raceId': 'race_id'})\
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

pit_stops_final = pit_stops_cleaned\
    .select('race_id', 'driver_id', 'duration', 'stop', 'lap', 'time', 'milliseconds',
            'file_date', 'ingestion_date', '_rescued_data')

# COMMAND ----------

(pit_stops_final.writeStream
        .option('checkpointLocation', checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)
    )

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_dev.silver.pit_stops

# COMMAND ----------

