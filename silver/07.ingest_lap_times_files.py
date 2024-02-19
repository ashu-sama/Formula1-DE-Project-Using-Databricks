# Databricks notebook source
# Clean-up
# spark.sql("DROP TABLE IF EXISTS formula1_dev.silver.lap_times")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
# Define variables used in code below
file_path = f"/Volumes/formula1_dev/bronze/raw/file_date=*/lap_times"
table_name = "formula1_dev.silver.lap_times"
checkpoint_path = "abfss://silver@f1adlsext.dfs.core.windows.net/checkpoint/lap_times_cp"

# Configure Auto Loader to ingest csv data to a Delta table
lap_times_schema = StructType(fields=[StructField("race_id", IntegerType(), False),
                                      StructField("driver_id", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                      StructField("file_date", DateType(), True)
                                     ])

lap_times_df = (spark
                    .readStream
                    .format('cloudFiles')
                    .option('cloudFiles.format', 'csv')
                    .schema(lap_times_schema)
                    .option('rescuedDataColumn', '_rescued_data')
                    .option('cloudFiles.schemaLocation', checkpoint_path)
                    .load(file_path)
    )

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

lap_times_cleaned = lap_times_df\
    .withColumn('ingestion_date', current_timestamp())\
    .dropDuplicates()

# COMMAND ----------

lap_times_final = lap_times_cleaned\
    .select('race_id', 'driver_id', 'lap', 'position', 'time', 'milliseconds',
            'file_date', 'ingestion_date', '_rescued_data'
 )

# COMMAND ----------

(lap_times_final
        .writeStream
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)    
)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_dev.silver.lap_times

# COMMAND ----------

