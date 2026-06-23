import pytest
from eightball.aggregations.daily import daily_distinct_users
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("test")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def test_daily_distinct_users(spark):
    # two events same user same day -> distinct count 1
    rows = [
        ("init", 1719100000000, "u1", "Portugal", "IOS"),
        ("init", 1719100050000, "u1", "Portugal", "IOS"),
        ("init", 1719100060000, "u2", "Portugal", "IOS"),
        ("init", 1719100070000, "u3", "Brazil",   "ANDROID"),
    ]
    df = spark.createDataFrame(
        rows, ["event-type", "time", "user-id", "country_name", "platform"])
    out = {(r["country_name"], r["platform"], r["distinct_users"])
           for r in daily_distinct_users(df).collect()}
    assert ("Portugal", "IOS", 2) in out
    assert ("Brazil", "ANDROID", 1) in out
