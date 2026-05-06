# DLT pipeline would typically materialize all tables into one target schema.
import dlt
from pyspark.sql.functions import (
    col,
    to_date,
    regexp_replace,
    date_format,
    count,
    countDistinct
)

ROOT_PATH = "s3://amzn-3-transactions-etl"


# Bronze
@dlt.table(
    name="bronze_users"
)
def bronze_users():
    return (
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


# Silver
@dlt.table(
    name="silver_users"
)
def silver_users():
    return (
        dlt.read("bronze_users")
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


# Gold
@dlt.table(
    name="gold_user_insights"
)
def gold_user_insights():
    return (
        dlt.read("silver_users")
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