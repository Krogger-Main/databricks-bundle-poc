import dlt
import pyspark.sql.functions as F

# -----------------------------
# Step 1: Create Demo Table
# -----------------------------
@dlt.table(
    name="demo_sales",
    comment="Demo sales table with base data"
)
def create_demo_table():
    return spark.createDataFrame([
        (1, "productA", 100),
        (2, "productB", 200)
    ], ["id", "product", "amount"]) \
    .withColumn("amount_with_tax", F.col("amount") * 1.1)

# -----------------------------
# Step 2: ETL Demo
# -----------------------------
@dlt.table(
    name="etl_demo_output",
    comment="ETL output table with adjusted tax"
)
def etl_demo():
    df = dlt.read("demo_sales")
    return df.withColumn("amount_with_tax", F.col("amount") * 1.2)

# -----------------------------
# Step 3: Transform Sales
# -----------------------------
@dlt.table(
    name="sales_transformed",
    comment="Filtered sales data for analytics"
)
def transform_sales():
    df = dlt.read("etl_demo_output")
    return df.filter(df.amount_with_tax > 150) \
             .withColumn("amount_with_tax", F.col("amount_with_tax").cast("double"))
