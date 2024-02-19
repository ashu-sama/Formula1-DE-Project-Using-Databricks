# Databricks notebook source
# Clean-up
# spark.sql("DROP TABLE IF EXISTS formula1_dev.silver.constructors")

# COMMAND ----------

# Define variables used in code below
file_path = f"/Volumes/formula1_dev/bronze/raw/file_date=*/constructors.json"
table_name = "formula1_dev.silver.constructors"
checkpoint_path = "abfss://silver@f1adlsext.dfs.core.windows.net/checkpoint/constructors_cp"

# Configure Auto Loader to ingest csv data to a Delta table
constructors_df = (spark.readStream
                        .format('cloudFiles')
                        .option('cloudFiles.format', 'json')
                        .option('cloudFiles.inferColumnTypes', True)
                        .option('cloudFiles.schemaLocation', checkpoint_path)
                        .load(file_path))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_cleaned = (constructors_df
                            .withColumn('ingestion_timestamp', current_timestamp())
                            .withColumnsRenamed({'constructorId': 'constructor_id',
                                                'constructorRef': 'constructor_ref'})
                            .drop("url")
                            .dropna(subset=['constructor_ref', 'name', 'nationality'])
                            .dropDuplicates(subset=['constructor_ref', 'name', 'nationality'])
)

# COMMAND ----------

constructors_final = (constructors_cleaned
                        .select('constructor_id', 'constructor_ref', 'name', 'nationality',
                                'ingestion_timestamp', 'file_date', '_rescued_data')
)

# COMMAND ----------

(constructors_final.writeStream
                    .option('checkpointLocation', checkpoint_path)
                    .trigger(availableNow=True)
                    .toTable(table_name)
        )

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_dev.silver.constructors

# COMMAND ----------

