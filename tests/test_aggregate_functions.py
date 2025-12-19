import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestAggFunctions").getOrCreate()


def test_agg_basic(spark):
    data = [{"grp": "A", "val": 10}, {"grp": "A", "val": 20}, {"grp": "B", "val": 30}]
    df = spark.createDataFrame(data)

    # Simple aggregations
    result = df.select(
        F.count("val").alias("cnt"),
        F.sum("val").alias("sum"),
        F.avg("val").alias("avg"),
        F.min("val").alias("min"),
        F.max("val").alias("max"),
    ).collect()

    assert result[0]["cnt"] == 3
    assert result[0]["sum"] == 60
    assert result[0]["avg"] == 20.0
    assert result[0]["min"] == 10
    assert result[0]["max"] == 30


def test_agg_count_distinct(spark):
    data = [{"val": 10}, {"val": 10}, {"val": 20}]
    df = spark.createDataFrame(data)
    result = df.select(F.countDistinct("val").alias("uniq")).collect()
    assert result[0]["uniq"] == 2


def test_agg_first_last(spark):
    data = [{"val": 1}, {"val": 2}, {"val": 3}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.first("val").alias("first"), F.last("val").alias("last")
    ).collect()
    assert result[0]["first"] == 1
    assert result[0]["last"] == 3
