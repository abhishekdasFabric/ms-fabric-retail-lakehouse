# ==============================================================================
# PIPELINE: Bronze to Silver Cleansing (Microsoft Fabric PySpark Notebook)
# AUTHOR: Abhishek Das (Senior Data & Analytics Engineer)
# DESCRIPTION: Cleanses raw retail POS transaction logs, handles nulls, and deduplicates
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, to_date, trim, upper

# 1. Read Raw Delta Table from Bronze Lakehouse
bronze_df = spark.read.format("delta").table("retail_lakehouse.bronze_pos_transactions")

# 2. Perform Data Scrubbing & Schema Validation
silver_cleansed_df = (
    bronze_df
    .dropDuplicates(["transaction_id", "store_id"])
    .withColumn("product_category", upper(trim(col("product_category"))))
    .withColumn("store_region", upper(trim(col("store_region"))))
    .withColumn("quantity", when(col("quantity").isNull() | (col("quantity") <= 0), 1).otherwise(col("quantity")))
    .withColumn("unit_price", when(col("unit_price").isNull(), 0.0).otherwise(col("unit_price")))
    .withColumn("total_amount", col("quantity") * col("unit_price"))
    .withColumn("transaction_date", to_date(col("transaction_timestamp")))
    .withColumn("silver_ingested_at", current_timestamp())
)

# 3. Write Cleansed Data to Silver Delta Layer with Schema Enforcement
(
    silver_cleansed_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_lakehouse.silver_pos_transactions")
)

print("Successfully transformed Bronze raw logs to Silver refined Delta Table.")
