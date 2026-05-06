from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

ROOT_PATH = "s3://amzn-3-transactions-etl"

def ingest_transactions():
    raw_df = (
        spark.read
        .option("header", True)
        .schema("""
            transaction_id STRING,
            transaction_date STRING,
            customer_id STRING,
            product_id STRING,
            product_name STRING,
            category STRING,
            quantity STRING,
            unit_price STRING,
            total_amount STRING,
            store_localtion STRING,
            payment_method STRING
        """)
        .csv(f"{ROOT_PATH}/landing/transactions/")
    )

    (
        raw_df.write
        .format("delta")
        .mode("append")
        .option(
            "path",
            f"{ROOT_PATH}/bronze/transactions/"
        )
        .saveAsTable("transactions_etl.bronze.transactions")
    )

if __name__ == "__main__":
    ingest_transactions()