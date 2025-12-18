import pytest
import json
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestJsonFunctions").getOrCreate()

def test_json_ops(spark):
    data = [{"json_str": '{"name": "Alice", "age": 30}'}]
    df = spark.createDataFrame(data)
    
    # get_json_object(col, path)
    result = df.select(
        F.get_json_object(F.col("json_str"), "$.name").alias("name"),
        F.get_json_object(F.col("json_str"), "$.age").alias("age")
    ).collect()
    
    assert result[0]["name"] == "Alice"
    assert result[0]["age"] == "30" # Note: get_json_object usually returns strings in Spark

def test_to_json(spark):
    data = [{"name": "Alice", "meta": {"city": "New York"}}]
    df = spark.createDataFrame(data)
    
    # In Polars, structs are already dict-like. 
    # F.struct() can be used to create one if needed.
    result = df.select(F.to_json(F.col("meta")).alias("json")).collect()
    
    # We expect a JSON string
    parsed = json.loads(result[0]["json"])
    assert parsed["city"] == "New York"
