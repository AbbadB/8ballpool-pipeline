import pytest
from pyspark.sql import SparkSession

from eightball.aggregations.minute import (
    build_user_dim, enrich, minute_purchase_metrics,
    minute_revenue_by_country, minute_matches_by_country)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("t")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _events(spark):
    rows = [
        ("init", 1719100000000, "u1", None, None, "Portugal", "IOS", None, None),
        ("init", 1719100000000, "u2", None, None, "Brazil",   "IOS", None, None),
        ("in-app-purchase", 1719100001000, "u1", 4.99, "p1", None, None, None, None),
        ("in-app-purchase", 1719100002000, "u2", 1.00, "p2", None, None, None, None),
        ("match", 1719100003000, None, None, None, None, None, "u1", "u2"),
    ]
    cols = ["event-type", "time", "user-id", "purchase_value", "product-id",
            "country_name", "platform", "user-a", "user-b"]
    return spark.createDataFrame(rows, cols)


def test_build_user_dim(spark):
    dim = {r["uid"]: r["country_name"]
           for r in build_user_dim(_events(spark)).collect()}
    assert dim == {"u1": "Portugal", "u2": "Brazil"}


def test_purchase_metrics_per_minute(spark):
    df = _events(spark)
    dim = build_user_dim(df)
    m = minute_purchase_metrics(enrich(df, dim)).collect()
    assert len(m) == 1                          # all in one minute
    row = m[0]
    assert row["purchase_count"] == 2
    assert abs(row["revenue"] - 5.99) < 1e-6
    assert row["distinct_users"] == 2


def test_revenue_by_country(spark):
    df = _events(spark)
    dim = build_user_dim(df)
    rev = {r["country_name"]: r["revenue"]
           for r in minute_revenue_by_country(enrich(df, dim)).collect()}
    assert abs(rev["Portugal"] - 4.99) < 1e-6
    assert abs(rev["Brazil"] - 1.00) < 1e-6


def test_matches_by_country_counts_both_players(spark):
    df = _events(spark)
    dim = build_user_dim(df)
    mc = {r["country_name"]: r["matches"]
          for r in minute_matches_by_country(enrich(df, dim)).collect()}
    assert mc["Portugal"] == 1 and mc["Brazil"] == 1   # both players (ADR-0004)


def test_distinct_users_correct_across_accumulated_batches(spark):
    # Pins the Option-C correctness guarantee (ADR-0008): recomputing metrics over
    # ACCUMULATED enriched events yields a correct distinct count, which summing
    # per-batch partials could not. Batch 1 sees {u1,u2}, batch 2 sees {u2,u3};
    # true distinct over the minute is 3, not 4.
    import datetime
    t = datetime.datetime(2026, 6, 23, 16, 43, 0)
    cols = ["ts", "purchase_value", "user-id", "country_name"]
    accumulated = [(t, 1.0, "u1", "PT"), (t, 1.0, "u2", "PT"),   # "batch 1"
                   (t, 1.0, "u2", "PT"), (t, 1.0, "u3", "PT")]    # "batch 2"
    purchases = spark.createDataFrame(accumulated, cols)
    m = minute_purchase_metrics((purchases, None)).collect()[0]
    assert m["purchase_count"] == 4
    assert m["distinct_users"] == 3
