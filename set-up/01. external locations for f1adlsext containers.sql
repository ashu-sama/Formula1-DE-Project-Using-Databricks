-- Databricks notebook source
CREATE EXTERNAL LOCATION IF NOT EXISTS f1adlsext_raw
URL  "abfss://raw@f1adlsext.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `f1adlsext_sc`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS f1adlsext_bronze
URL  "abfss://bronze@f1adlsext.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `f1adlsext_sc`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS f1adlsext_silver
URL  "abfss://silver@f1adlsext.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `f1adlsext_sc`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS f1adlsext_gold
URL  "abfss://gold@f1adlsext.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `f1adlsext_sc`);