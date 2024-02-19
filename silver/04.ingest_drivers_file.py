# Databricks notebook source
# Clean-up
spark.sql("DROP TABLE IF EXISTS formula1_dev.silver.drivers")

# COMMAND ----------

# Define variables used in code below
file_path = f"/Volumes/formula1_dev/bronze/raw/file_date=*/drivers.json"
table_name = "formula1_dev.silver.drivers"
checkpoint_path = "abfss://silver@f1adlsext.dfs.core.windows.net/checkpoint/drivers_cp"

# Configure Auto Loader to ingest json data to a Delta table
drivers_df = (spark.readStream
                    .format('cloudFiles')
                    .option('cloudFiles.format', 'json')
                    .option('cloudFiles.inferColumnTypes', True)
                    .option('cloudFiles.schemaLocation', checkpoint_path)
                    .load(file_path)
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, when, col, lit, concat

# COMMAND ----------

drivers_cleaned = (drivers_df
                        .withColumn('number', when(drivers_df.number == '\\N', None).otherwise(drivers_df.number))
                        .withColumn('code', when(drivers_df.code == '\\N', None).otherwise(drivers_df.code))
                        .withColumn('driver_name', concat(col("name.forename"), lit(" "), col("name.surname")))
                        .withColumn('ingestion_timestamp', current_timestamp())
                        .withColumnsRenamed({"driverId": "driver_id", 
                                            "driverRef": "driver_ref"})
                        .dropna(how='all')
                        .dropDuplicates()
)

# COMMAND ----------

drivers_final = (drivers_cleaned
                        .select('driver_id', 'driver_ref', 'driver_name', 'nationality', 'number',
                                'code', 'dob', 'ingestion_timestamp', 'file_date', '_rescued_data')
                        )

# COMMAND ----------

(drivers_final.writeStream
            .option('checkpointLocation', checkpoint_path)
            .trigger(availableNow=True)
            .toTable(table_name)
        )

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_dev.silver.drivers

# COMMAND ----------

