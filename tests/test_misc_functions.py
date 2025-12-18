import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestMiscFunctions").getOrCreate()

def test_math_v3(spark):
    data = [{"a": 3, "b": 4}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.pow(F.col("a"), 2).alias("pow"),
        F.hypot(F.col("a"), F.col("b")).alias("hypot")
    ).collect()
    
    assert result[0]["pow"] == 9
    assert result[0]["hypot"] == 5.0

def test_ids(spark):
    data = [{"a": 1}, {"a": 2}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.monotonically_increasing_id().alias("id"),
        F.spark_partition_id().alias("pid")
    ).collect()
    
    assert result[0]["id"] < result[1]["id"]
    assert result[0]["pid"] == 0

def test_binary_string(spark):
    data = [{"text": "Alice"}]
    df = spark.createDataFrame(data)
    # base64(col)
    result = df.select(
        F.base64(F.col("text")).alias("b64")
    ).select(
        F.col("b64"),
        F.unbase64(F.col("b64")).alias("raw")
    ).collect()
    
    import base64
    expected_b64 = base64.b64encode(b"Alice").decode()
    assert result[0]["b64"] == expected_b64
    # unbase64 in Spark returns binary. In our case, let's see. 
    # Usually we want strings back if it was a string.
    assert result[0]["raw"] == b"Alice"
