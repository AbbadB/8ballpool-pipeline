"""Daily distinct users by country and platform (Beginner tier).

Pure transform: a DataFrame of clean init events -> daily aggregate. Event time
is epoch millis (ADR-0006)."""
from pyspark.sql import DataFrame, functions as F


def daily_distinct_users(events: DataFrame) -> DataFrame:
    init = events.filter(F.col("event-type") == "init")
    return (init
            .withColumn("event_date",
                        F.to_date(F.timestamp_millis(F.col("time"))))
            .groupBy("event_date", "country_name", "platform")
            .agg(F.countDistinct("user-id").alias("distinct_users")))
