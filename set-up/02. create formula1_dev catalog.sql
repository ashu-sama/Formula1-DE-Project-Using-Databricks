-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS formula1_dev;

-- COMMAND ----------

USE CATALOG formula1_dev;
SELECT current_catalog();