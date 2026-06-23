"""Structured Streaming minute aggregator (Pro tier).

Reads `events.clean`, and per micro-batch (foreachBatch, ADR-0008):
  1. updates the user dimension (parquet) from any init events in the batch,
  2. enriches this batch's match/purchase against the full dimension and APPENDS
     the enriched events to accumulating stores,
  3. RECOMPUTES the minute aggregates from the full accumulated enriched stores,
  4. prints current minute aggregates to the console.

Recomputing from accumulated *enriched events* (not from per-batch partials) is
what makes the per-minute totals correct across batches -- including distinct
users, which cannot be summed from partials (ADR-0008). It is O(n) per batch; the
production upgrade is native stateful streaming aggregation with watermarks.
"""
import os

from pyspark.sql import SparkSession, functions as F

from eightball.aggregations.minute import (
    build_user_dim, enrich, minute_purchase_metrics,
    minute_revenue_by_country, minute_matches_by_country)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
OUT = "/output"
DIM = f"{OUT}/user_dim"
ENR_PURCH = f"{OUT}/_enriched_purchases"
ENR_PLAYERS = f"{OUT}/_enriched_players"
SCHEMA = ("`event-type` STRING, `time` LONG, `user-id` STRING, "
          "purchase_value DOUBLE, `product-id` STRING, country_name STRING, "
          "platform STRING, `user-a` STRING, `user-b` STRING")


def _read(spark, path):
    try:
        return spark.read.parquet(path)
    except Exception:
        return None


def process_batch(batch_df, _epoch):
    spark = batch_df.sparkSession
    events = batch_df.select(
        F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")
    ).select("e.*")

    # 1. accumulate the user dimension from init events in this batch
    new_dim = build_user_dim(events)
    if new_dim.head(1):
        new_dim.write.mode("append").parquet(DIM)

    dim = _read(spark, DIM)
    if dim is None:
        return                       # no init seen yet; nothing to enrich against
    dim = dim.dropDuplicates(["uid"])

    # 2. enrich this batch and APPEND enriched events to accumulating stores
    purchases, players = enrich(events, dim)
    if purchases.head(1):
        purchases.write.mode("append").parquet(ENR_PURCH)
    if players.head(1):
        players.write.mode("append").parquet(ENR_PLAYERS)

    # 3. RECOMPUTE minute aggregates from the full accumulated enriched stores
    all_purch = _read(spark, ENR_PURCH)
    if all_purch is not None:
        minute_purchase_metrics((all_purch, None)).write.mode("overwrite").parquet(
            f"{OUT}/minute_purchase_metrics")
        minute_revenue_by_country((all_purch, None)).write.mode("overwrite").parquet(
            f"{OUT}/minute_revenue_by_country")
        minute_purchase_metrics((all_purch, None)).show(truncate=False)  # console

    all_players = _read(spark, ENR_PLAYERS)
    if all_players is not None:
        minute_matches_by_country((None, all_players)).write.mode("overwrite").parquet(
            f"{OUT}/minute_matches_by_country")


def main():
    spark = SparkSession.builder.appName("minute-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", "events.clean")
              .option("startingOffsets", "earliest").load())
    (stream.writeStream.foreachBatch(process_batch)
     .option("checkpointLocation", f"{OUT}/_chk").start().awaitTermination())


if __name__ == "__main__":
    main()
