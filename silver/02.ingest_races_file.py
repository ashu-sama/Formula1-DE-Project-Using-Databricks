# Databricks notebook source
# Clean-up
# spark.sql("DROP TABLE IF EXISTS formula1_dev.silver.races")

# COMMAND ----------

# Define variables used in code below
file_path = f"/Volumes/formula1_dev/bronze/raw/file_date=*/races.csv"
table_name = "formula1_dev.silver.races"
checkpoint_path = "abfss://silver@f1adlsext.dfs.core.windows.net/checkpoint/races_cp"

races_df =(spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", True)
                .option("cloudFiles.inferColumnTypes", True)
                .option("cloudFiles.schemaLocation", checkpoint_path)
                .load(file_path))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit, regexp_replace

# COMMAND ----------

races_cleaned = (races_df
                    .withColumn('time', regexp_replace(col('time'), r"\\N", "00:00:00"))
                    .withColumn('ingestion_timestamp', current_timestamp())
                    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
                    .withColumnsRenamed({'raceId': 'race_id',
                                         'circuitId': 'circuit_id'})
                    .drop("url")  
                    )

# COMMAND ----------

races_final = (races_cleaned
                        .select('race_id', 'year', 'round', 'name', 'circuit_id', 'race_timestamp',
                                'ingestion_timestamp', 'file_date', '_rescued_data')
                )

# COMMAND ----------

(races_final.writeStream
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .toTable(table_name)
)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_dev.silver.races

# COMMAND ----------

