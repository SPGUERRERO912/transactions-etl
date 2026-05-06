from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, initcap, col

spark = SparkSession.builder.getOrCreate()

ROOT_PATH = "s3://amzn-3-transactions-etl"

def create_gold_transactions():
    # Read from silver table
    silver_df = spark.table("transactions_etl.silver.transactions")
    
    # Apply transformations for gold layer
    gold_df = silver_df.select(
        col("transaction_id"),
        col("transaction_date"),
        col("customer_id"),
        col("product_id"),
        initcap(col("product_name")).alias("product_name"),
        col("category"),
        col("quantity"),
        col("unit_price"),
        col("total_amount"),
        col("store_localtion"),
        col("payment_method"),
        current_timestamp().alias("inserted_at")
    )
    
    # Write to gold table
    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "path",
            f"{ROOT_PATH}/gold/transactions/"
        )
        .saveAsTable("transactions_etl.gold.transactions")
    )

if __name__ == "__main__":
    create_gold_transactions()
