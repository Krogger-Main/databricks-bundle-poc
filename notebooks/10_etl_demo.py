# Databricks notebook
dbutils.widgets.text("catalog", "hive_metastore")
dbutils.widgets.text("schema", "demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Read raw
df = spark.table(f"{catalog}.{schema}.sales_raw")

# Simple transform: add total_amount
from pyspark.sql.functions import col, round as sql_round
df_tr = df.withColumn("total_amount", sql_round(col("qty") * col("price"), 2))

# Write
df_tr.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.sales_curated")

print("Wrote table:", f"{catalog}.{schema}.sales_curated")
