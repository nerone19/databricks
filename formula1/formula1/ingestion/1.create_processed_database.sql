-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula19/processed"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula19/presentation"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

CREATE TABLES 