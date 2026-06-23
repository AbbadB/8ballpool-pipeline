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
