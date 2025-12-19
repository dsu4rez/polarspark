import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestAdvancedFunctions").getOrCreate()


def test_string_logic(spark):
    data = [{"text": "hello world", "val": 123.4567}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.instr(F.col("text"), "world").alias("instr"),
        F.locate("o", F.col("text")).alias("locate"),
        F.format_number(F.col("val"), 2).alias("fmt"),
    ).collect()

    # instr is 1-indexed in Spark. "world" starts at 7 (1-indexed)
    assert result[0]["instr"] == 7
    # locate is 1-indexed. First "o" is at 5
    assert result[0]["locate"] == 5
    assert result[0]["fmt"] == "123.46"


def test_agg_v2(spark):
    data = [{"val": i} for i in range(1, 101)]
    df = spark.createDataFrame(data)
    result = df.select(
        F.skewness("val").alias("skew"),
        F.kurtosis("val").alias("kurt"),
        F.stddev("val").alias("std"),
        F.variance("val").alias("var"),
    ).collect()

    assert result[0]["std"] > 0
    assert result[0]["var"] > 0
