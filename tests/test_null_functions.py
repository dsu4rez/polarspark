import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestNullFunctions").getOrCreate()


def test_isnull_isnotnull(spark):
    data = [{"val": 10}, {"val": None}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.isnull(F.col("val")).alias("is_null"),
        F.isnotnull(F.col("val")).alias("is_not_null"),
    ).collect()

    assert result[0]["is_null"] == False
    assert result[0]["is_not_null"] == True
    assert result[1]["is_null"] == True
    assert result[1]["is_not_null"] == False


def test_coalesce(spark):
    data = [
        {"a": None, "b": None, "c": 3},
        {"a": None, "b": 2, "c": 3},
        {"a": 1, "b": 2, "c": 3},
    ]
    df = spark.createDataFrame(data)
    result = df.select(
        F.coalesce(F.col("a"), F.col("b"), F.col("c")).alias("coalesced")
    ).collect()

    assert result[0]["coalesced"] == 3
    assert result[1]["coalesced"] == 2
    assert result[2]["coalesced"] == 1


def test_nan(spark):
    import math

    data = [{"val": float("nan")}, {"val": 1.0}]
    df = spark.createDataFrame(data)
    result = df.select(F.isnan(F.col("val")).alias("is_nan")).collect()
    assert result[0]["is_nan"] == True
    assert result[1]["is_nan"] == False
