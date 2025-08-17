# Databricks notebook source
# Widgets to receive job parameters
dbutils.widgets.text("catalog", "hive_metastore")
dbutils.widgets.text("schema", "demo_dev")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------
# ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------
# create a demo raw table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.sales_raw (
  id INT,
  amount DOUBLE
)
""")

# COMMAND ----------
# seed some data (idempotent-ish sample)
spark.sql(f"INSERT INTO {catalog}.{schema}.sales_raw VALUES (1, 100.0), (2, 200.0)")
