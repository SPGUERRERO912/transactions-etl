from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

ROOT_PATH = "s3://amzn-3-transactions-etl"

def ingest_users():
    raw_df = (
        spark.read
        .option("header", True)
        .schema("""
            user_id STRING,
            first_name STRING,
            last_name STRING,
            email STRING,
            signup_date STRING,
            country STRING,
            referral_source STRING
        """)
        .csv(f"{ROOT_PATH}/landing/users/")
    )

    (
        raw_df.write
        .format("delta")
        .mode("append")
        .option(
            "path",
            f"{ROOT_PATH}/bronze/users/"
        )
        .saveAsTable("transactions_etl.bronze.users")
    )

if __name__ == "__main__":
    ingest_users()