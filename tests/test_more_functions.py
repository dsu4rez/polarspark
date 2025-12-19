import pytest
import math
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestMoreFunctions").getOrCreate()


def test_math_v2(spark):
    data = [{"val": 1.0}, {"val": 10.0}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.sqrt(F.col("val")).alias("sqrt"),
        F.exp(F.col("val")).alias("exp"),
        F.log(F.col("val")).alias("log"),
    ).collect()

    assert result[0]["sqrt"] == 1.0
    assert result[1]["log"] == math.log(10.0)


def test_string_v3(spark):
    data = [{"text": "apple", "pad": "x"}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.lpad(F.col("text"), 10, "#").alias("lpad"),
        F.rpad(F.col("text"), 10, "#").alias("rpad"),
        F.reverse(F.col("text")).alias("rev"),
        F.initcap(F.lit("hello world")).alias("cap"),
    ).collect()

    assert result[0]["lpad"] == "#####apple"
    assert result[0]["rpad"] == "apple#####"
    assert result[0]["rev"] == "elppa"
    assert result[0]["cap"] == "Hello World"


def test_collection_v2(spark):
    data = [{"arr": [3, 1, 2], "val": 1}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.array_contains(F.col("arr"), 1).alias("has_1"),
        F.array_contains(F.col("arr"), 5).alias("has_5"),
        F.sort_array(F.col("arr")).alias("sorted"),
    ).collect()

    assert result[0]["has_1"] == True
    assert result[0]["has_5"] == False
    assert list(result[0]["sorted"]) == [1, 2, 3]


def test_type_conversion(spark):
    data = [{"val": "123"}]
    df = spark.createDataFrame(data)
    # Spark format for to_date is optional, but let's test basic cast first
    result = df.select(
        F.to_date(F.lit("2023-01-01")).alias("dt"),
        F.to_timestamp(F.lit("2023-01-01 12:00:00")).alias("ts"),
    ).collect()

    from datetime import date, datetime

    assert result[0]["dt"] == date(2023, 1, 1)
    assert result[0]["ts"] == datetime(2023, 1, 1, 12, 0, 0)
