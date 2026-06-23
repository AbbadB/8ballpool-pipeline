"""Per-minute aggregates (Pro tier). All functions are pure DataFrame transforms.

Country enrichment (ADR-0001): match/purchase carry no country, so we join them
to a user dimension built from init events. Matches are attributed to BOTH
players' countries (ADR-0004). Windows are 1-minute tumbling on event-time
(epoch millis -> timestamp, ADR-0006)."""
from pyspark.sql import DataFrame, functions as F


def _minute():
    # Built lazily inside transforms: a Spark column expression cannot be created
    # at import time (it needs an active SparkContext).
    return F.window(F.col("ts"), "1 minute")


def _with_ts(df: DataFrame) -> DataFrame:
    return df.withColumn("ts", F.timestamp_millis(F.col("time")))


def build_user_dim(events: DataFrame) -> DataFrame:
    """user -> (country_name, platform), one row per user, from init events."""
    return (events.filter(F.col("event-type") == "init")
            .select(F.col("user-id").alias("uid"), "country_name", "platform")
            .dropDuplicates(["uid"]))


def enrich(events: DataFrame, user_dim: DataFrame):
    """Attach country_name to purchase (by user-id) and match (by user-a/user-b).

    Returns a (purchases, players) tuple. `players` has one row per participating
    player so a match counts under BOTH players' countries (ADR-0004). A user with
    no known init enriches to country_name = 'UNKNOWN' rather than being dropped.
    """
    e = _with_ts(events)
    dim = user_dim
    # Project only the columns each subset needs BEFORE the join, so the dim's
    # country_name is the single, unambiguous source of country.
    purchase_src = (e.filter(F.col("event-type") == "in-app-purchase")
                    .select("ts", "purchase_value",
                            F.col("user-id").alias("uid")))
    purchases = (purchase_src.join(dim, "uid", "left")
                 .select("ts", "purchase_value",
                         F.col("uid").alias("user-id"),
                         F.coalesce("country_name", F.lit("UNKNOWN")).alias("country_name")))
    m = e.filter(F.col("event-type") == "match")
    players = (m.select("ts", F.col("user-a").alias("uid"))
               .unionByName(m.select("ts", F.col("user-b").alias("uid")))
               .join(dim, "uid", "left")
               .select("ts",
                       F.coalesce("country_name", F.lit("UNKNOWN")).alias("country_name")))
    return purchases, players


def minute_purchase_metrics(enriched) -> DataFrame:
    purchases, _ = enriched
    return (purchases.groupBy(_minute())
            .agg(F.count("*").alias("purchase_count"),
                 F.sum("purchase_value").alias("revenue"),
                 F.countDistinct("user-id").alias("distinct_users")))


def minute_revenue_by_country(enriched) -> DataFrame:
    purchases, _ = enriched
    return (purchases.groupBy(_minute(), "country_name")
            .agg(F.sum("purchase_value").alias("revenue")))


def minute_matches_by_country(enriched) -> DataFrame:
    _, players = enriched
    return (players.groupBy(_minute(), "country_name")
            .agg(F.count("*").alias("matches")))
