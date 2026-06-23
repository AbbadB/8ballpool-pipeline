"""Structured Streaming minute aggregator (Pro tier).

Reads `events.clean`, and per micro-batch (foreachBatch, ADR-0008):
  1. updates the user dimension (parquet) from any init events in the batch,
  2. reads the full dimension, enriches match/purchase,
  3. computes minute aggregates for the batch and merges into output parquet,
  4. prints current minute aggregates to the console.
At demo scale read-modify-write on parquet is fine; at scale -> Delta merge.
"""
import os

from pyspark.sql import SparkSession, functions as F

from eightball.aggregations.minute import (
    build_user_dim, enrich, minute_purchase_metrics,
    minute_revenue_by_country, minute_matches_by_country)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DIM = "/output/user_dim"
SCHEMA = ("`event-type` STRING, `time` LONG, `user-id` STRING, "
          "purchase_value DOUBLE, `product-id` STRING, country_name STRING, "
          "platform STRING, `user-a` STRING, `user-b` STRING")


def _merge(df, path):
    """Append df to the parquet at path (read-modify-write; demo scale)."""
    try:
        existing = df.sparkSession.read.parquet(path)
        df = existing.unionByName(df, allowMissingColumns=True)
    except Exception:
        pass  # first write: nothing to merge with
    df.write.mode("overwrite").parquet(path)


def process_batch(batch_df, _epoch):
    spark = batch_df.sparkSession
    events = batch_df.select(
        F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")
    ).select("e.*")

    # 1. update user dim from init events in this batch
    new_dim = build_user_dim(events)
    if new_dim.head(1):
        _merge(new_dim, DIM)

    # 2. read the full dimension (skip the batch until we have one)
    try:
        dim = spark.read.parquet(DIM).dropDuplicates(["uid"])
    except Exception:
        return

    # 3. enrich + aggregate this batch, merge outputs
    enriched = enrich(events, dim)
    _merge(minute_purchase_metrics(enriched), "/output/minute_purchase_metrics")
    _merge(minute_revenue_by_country(enriched), "/output/minute_revenue_by_country")
    _merge(minute_matches_by_country(enriched), "/output/minute_matches_by_country")

    # 4. console visibility
    minute_purchase_metrics(enriched).show(truncate=False)


def main():
    spark = SparkSession.builder.appName("minute-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", "events.clean")
              .option("startingOffsets", "earliest").load())
    (stream.writeStream.foreachBatch(process_batch)
     .option("checkpointLocation", "/output/_chk").start().awaitTermination())


if __name__ == "__main__":
    main()
