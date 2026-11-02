# ==============================================================================
# PIPELINE: Silver to Gold Star Schema Modeling (Delta Lake Star Schema)
# AUTHOR: Abhishek Das (Senior Data & Analytics Engineer)
# DESCRIPTION: Constructs Fact_Sales and Dim_Product for Direct Lake Power BI Consumption
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hash, current_timestamp

# 1. Read Refined Silver Delta Tables
silver_df = spark.read.format("delta").table("retail_lakehouse.silver_pos_transactions")

# 2. Build Gold Dimension Table: Dim_Product
dim_product_df = (
    silver_df
    .select("product_id", "product_name", "product_category")
    .distinct()
    .withColumn("product_key", hash(col("product_id")))
)

dim_product_df.write.format("delta").mode("overwrite").saveAsTable("retail_lakehouse.gold_dim_product")

# 3. Build Gold Fact Table: Fact_Sales
fact_sales_df = (
    silver_df
    .select(
        col("transaction_id"),
        col("transaction_date"),
        col("store_id"),
        col("product_id"),
        hash(col("product_id")).alias("product_key"),
        col("quantity"),
        col("unit_price"),
        col("total_amount")
    )
    .withColumn("gold_loaded_at", current_timestamp())
)

fact_sales_df.write.format("delta").mode("overwrite").saveAsTable("retail_lakehouse.gold_fact_sales")

print("Gold Layer Star Schema (Fact_Sales, Dim_Product) successfully published to OneLake.")
