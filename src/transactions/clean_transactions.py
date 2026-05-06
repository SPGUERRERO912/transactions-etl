from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, lower, ltrim, col, regexp_replace

spark = SparkSession.builder.getOrCreate()

ROOT_PATH = "s3://amzn-3-transactions-etl"

def clean_transactions():
    # Read from bronze table
    bronze_df = spark.table("transactions_etl.bronze.transactions")
    
    # Apply transformations
    clean_df = bronze_df.select(
        col("transaction_id"),
        to_date(col("transaction_date")).alias("transaction_date"),
        col("customer_id"),
        col("product_id"),
        regexp_replace(
            ltrim(lower(col("product_name"))),
            r"\s+",
            " "
        ).alias("product_name"),
        col("category"),
        col("quantity").cast("int").alias("quantity"),
        col("unit_price").cast("double").alias("unit_price"),
        col("total_amount").cast("double").alias("total_amount"),
        col("store_localtion"),
        col("payment_method")
    )

    # Write to silver table
    (
        clean_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "path",
            f"{ROOT_PATH}/silver/transactions/"
        )
        .saveAsTable("transactions_etl.silver.transactions")
    )

if __name__ == "__main__":
    clean_transactions()
