import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestHigherOrderV2").getOrCreate()

def test_zip_with(spark):
    data = [{"a": [1, 2], "b": [10, 20]}]
    df = spark.createDataFrame(data)
    
    # zip_with(a, b, lambda x, y: x + y)
    result = df.select(F.zip_with("a", "b", lambda x, y: x + y).alias("res")).collect()
    assert list(result[0]["res"]) == [11, 22]

def test_transform_keys_values(spark):
    # Maps are lists of structs {"key":..., "value":...}
    data = [{"m": [{"key": "a", "value": 1}, {"key": "b", "value": 2}]}]
    df = spark.createDataFrame(data)
    
    # transform_keys(m, lambda k, v: k.upper())
    # transform_values(m, lambda k, v: v * 10)
    result = df.select(
        F.transform_keys("m", lambda k, v: F.upper(k)).alias("k"),
        F.transform_values("m", lambda k, v: v * 10).alias("v")
    ).collect()
    
    # transform_keys in Spark returns a map with new keys
    # In our case, list of structs with new keys
    assert result[0]["k"][0]["key"] == "A"
    assert result[0]["v"][0]["value"] == 10
