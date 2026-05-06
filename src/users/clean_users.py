from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace

spark = SparkSession.builder.getOrCreate()

ROOT_PATH = "s3://amzn-3-transactions-etl"

def clean_users():
    bronze_df = spark.table("transactions_etl.bronze.users")

    clean_df = (
        bronze_df
        .select(
            col("user_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            to_date(
                regexp_replace(
                    col("signup_date"),
                    r"\.",
                    "/"
                ),
                "M/d/yy"
            ).alias("signup_date"),
            col("country"),
            col("referral_source")
        )
        .dropDuplicates(["user_id"])
        .filter(col("user_id").isNotNull())
    )

    (
        clean_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "path",
            f"{ROOT_PATH}/silver/users/"
        )
        .saveAsTable("transactions_etl.silver.users")
    )

if __name__ == "__main__":
    clean_users()