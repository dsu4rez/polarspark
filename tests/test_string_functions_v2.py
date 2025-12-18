import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestStringFunctionsV2").getOrCreate()

def test_regex_ops(spark):
    data = [{"text": "apple 123", "repl": "fruit"}]
    df = spark.createDataFrame(data)
    # regexp_replace(col, pattern, replacement)
    result = df.select(
        F.regexp_replace(F.col("text"), r"\d+", "number").alias("replaced"),
        F.regexp_extract(F.col("text"), r"(\d+)", 1).alias("extracted")
    ).collect()
    
    assert result[0]["replaced"] == "apple number"
    assert result[0]["extracted"] == "123"

def test_split_substring(spark):
    data = [{"text": "a,b,c"}]
    df = spark.createDataFrame(data)
    # substring(col, pos, len) - Spark is 1-indexed
    result = df.select(
        F.split(F.col("text"), ",").alias("split"),
        F.substring(F.col("text"), 1, 3).alias("sub")
    ).collect()
    
    assert list(result[0]["split"]) == ["a", "b", "c"]
    assert result[0]["sub"] == "a,b"

def test_trim_ops(spark):
    data = [{"text": "  hello  "}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.trim(F.col("text")).alias("trim"),
        F.ltrim(F.col("text")).alias("ltrim"),
        F.rtrim(F.col("text")).alias("rtrim")
    ).collect()
    
    assert result[0]["trim"] == "hello"
    assert result[0]["ltrim"] == "hello  "
    assert result[0]["rtrim"] == "  hello"
