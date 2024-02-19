# Databricks notebook source
# Clean-up
# spark.sql("DROP TABLE IF EXISTS formula1_dev.silver.circuits")

# COMMAND ----------

# Define variables used in code below
file_path = f"/Volumes/formula1_dev/bronze/raw/file_date=*/circuits.csv"
table_name = "formula1_dev.silver.circuits"
checkpoint_path = "abfss://silver@f1adlsext.dfs.core.windows.net/checkpoint/circuits_cp"

circuits_df =(spark.readStream
                    .format('cloudFiles')
                    .option('cloudFiles.format', 'csv')
                    .option('header', True)
                    .option('cloudFiles.inferColumnTypes', True)
                    .option('cloudFiles.schemaLocation', checkpoint_path)
                    .load(file_path)
            )


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_cleaned = (circuits_df
                        .withColumn('ingestion_timestamp', current_timestamp())
                        .withColumnsRenamed({"circuitId": "circuit_id", 
                                            "circuitRef": "circuit_ref",
                                            "lat": "latitude",
                                            "lng": "longitude",
                                            "alt": "altitude"})
                        .drop("url")
                        .dropna(subset=['circuit_id', 'circuit_ref', 'name', 'location', 'country'])
                        .dropDuplicates(subset=['circuit_ref', 'name'])
)

# COMMAND ----------

circuits_final = circuits_cleaned.select('circuit_id', 'circuit_ref', 'name', 'location', 'country', 
                                         'latitude', 'longitude', 'altitude', 
                                         'ingestion_timestamp', 'file_date', '_rescued_data')

# COMMAND ----------

(circuits_final.writeStream
        .option('checkpointLocation', checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)
        )

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *From formula1_dev.silver.circuits

# COMMAND ----------

