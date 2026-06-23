"""Batch job: read all clean events from Kafka, write daily distinct-user agg.

Reads the whole `events.clean` topic from earliest, computes the daily
distinct-user aggregate, and writes Parquet + a console preview."""
import os

from pyspark.sql import SparkSession, functions as F

from eightball.aggregations.daily import daily_distinct_users

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# Clean-event schema as seen on events.clean (post-DQ): init carries the resolved
# country_name and the uppercased platform.
_SCHEMA = ("`event-type` STRING, `time` LONG, `user-id` STRING, "
           "country_name STRING, platform STRING")


def main():
    spark = SparkSession.builder.appName("daily-batch").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", "events.clean")
           .option("startingOffsets", "earliest").load())
    events = raw.select(
        F.from_json(F.col("value").cast("string"), _SCHEMA).alias("e")
    ).select("e.*")
    out = daily_distinct_users(events)
    out.show(truncate=False)
    out.write.mode("overwrite").parquet("/output/daily_distinct_users")


if __name__ == "__main__":
    main()
