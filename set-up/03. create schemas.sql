-- Databricks notebook source
-- create bronze schema
CREATE SCHEMA IF NOT EXISTS formula1_dev.bronze

-- COMMAND ----------

-- creating volume for accessing raw data
CREATE External VOLUME formula1_dev.bronze.raw
LOCATION 'abfss://bronze@f1adlsext.dfs.core.windows.net/raw';

-- COMMAND ----------

-- create silver schema with managed location
CREATE SCHEMA IF NOT EXISTS formula1_dev.silver
MANAGED LOCATION 'abfss://silver@f1adlsext.dfs.core.windows.net/'

-- COMMAND ----------

-- create gold schema with managed location
CREATE SCHEMA IF NOT EXISTS formula1_dev.gold
MANAGED LOCATION 'abfss://gold@f1adlsext.dfs.core.windows.net/'

-- COMMAND ----------

