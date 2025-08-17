# Databricks notebook source
dbutils.widgets.text("catalog", "hive_metastore")
dbutils.widgets.text("schema", "demo_dev")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------
# simple transform step to clean/prepare data
df = spark.table(f"{catalog}.{schema}.sales_raw")
df = df.withColumn("amount_norm", df.amount * 1.0)  # placeholder transform
df.createOrReplaceTempView("sales_prepared")
