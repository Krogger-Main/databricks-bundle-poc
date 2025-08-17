# Databricks notebook source
dbutils.widgets.text("catalog", "hive_metastore")
dbutils.widgets.text("schema", "demo_dev")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------
# final transform step (can be pure SQL via %sql)
# Using a Python cell for portability; switch to %sql if you like:
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.sales_transformed AS
SELECT
  id,
  amount,
  amount * 1.1 AS amount_with_tax
FROM {catalog}.{schema}.sales_raw
""")
