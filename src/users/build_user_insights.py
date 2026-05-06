from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    date_format,
    count,
    countDistinct,
    col
)

spark = SparkSession.builder.getOrCreate()

ROOT_PATH = "s3://amzn-3-transactions-etl"

def build_gold():
    silver_df = spark.table("transactions_etl.silver.users")

    insights_df = (
        silver_df
        .withColumn(
            "day_name",
            date_format(
                col("signup_date"),
                "EEEE"
            )
        )
        .groupBy(
            "day_name",
            "referral_source"
        )
        .agg(
            count("*").alias("signups"),
            countDistinct("country").alias(
                "unique_countries"
            )
        )
    )

    (
        insights_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "path",
            f"{ROOT_PATH}/gold/insights/"
        )
        .saveAsTable(
            "transactions_etl.gold.insights"
        )
    )

if __name__ == "__main__":
    build_gold()