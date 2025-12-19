import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestFunctions").getOrCreate()


def test_math_abs(spark):
    data = [{"val": -10}, {"val": 20}]
    df = spark.createDataFrame(data)
    result = df.select(F.abs(F.col("val")).alias("abs_val")).collect()
    assert result[0]["abs_val"] == 10
    assert result[1]["abs_val"] == 20


def test_math_ceil_floor(spark):
    data = [{"val": 1.5}, {"val": -1.5}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.ceil(F.col("val")).alias("ceil"), F.floor(F.col("val")).alias("floor")
    ).collect()
    assert result[0]["ceil"] == 2
    assert result[0]["floor"] == 1
    assert result[1]["ceil"] == -1
    assert result[1]["floor"] == -2


def test_string_upper_lower(spark):
    data = [{"name": "alice"}, {"name": "BOB"}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.upper(F.col("name")).alias("upper"), F.lower(F.col("name")).alias("lower")
    ).collect()
    assert result[0]["upper"] == "ALICE"
    assert result[0]["lower"] == "alice"
    assert result[1]["upper"] == "BOB"
    assert result[1]["lower"] == "bob"


def test_string_length(spark):
    data = [{"name": "alice"}, {"name": "bob"}]
    df = spark.createDataFrame(data)
    result = df.select(F.length(F.col("name")).alias("len")).collect()
    assert result[0]["len"] == 5
    assert result[1]["len"] == 3


def test_string_concat(spark):
    data = [{"first": "John", "last": "Doe"}]
    df = spark.createDataFrame(data)
    # Spark's concat takes multiple columns/literals
    result = df.select(
        F.concat(F.col("first"), F.lit(" "), F.col("last")).alias("full")
    ).collect()
    assert result[0]["full"] == "John Doe"
