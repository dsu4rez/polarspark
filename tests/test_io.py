import pytest
import os
import shutil
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestIO").getOrCreate()

def test_csv_read_write(spark):
    df = spark.createDataFrame([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
    path = "tests/temp_test.csv"
    
    # Write with options
    df.write.option("header", "true").option("sep", "|").csv(path)
    
    assert os.path.exists(path)
    
    # Read with options
    df2 = spark.read.option("header", "true").option("sep", "|").csv(path)
    
    res = df2.collect()
    assert len(res) == 2
    assert res[0]["name"] == "Alice"
    assert res[1]["id"] == 2
    
    os.remove(path)

def test_parquet_read_write(spark):
    df = spark.createDataFrame([{"id": 1, "val": 10.5}, {"id": 2, "val": 20.0}])
    path = "tests/temp_test.parquet"
    
    df.write.parquet(path)
    assert os.path.exists(path)
    
    df2 = spark.read.parquet(path)
    res = df2.collect()
    assert len(res) == 2
    assert res[0]["val"] == 10.5
    
    os.remove(path)

def test_json_read_write(spark):
    df = spark.createDataFrame([{"id": 1, "data": [1, 2]}, {"id": 2, "data": [3, 4]}])
    path = "tests/temp_test.json"
    
    df.write.json(path)
    assert os.path.exists(path)
    
    df2 = spark.read.json(path)
    res = df2.orderBy("id").collect()
    assert len(res) == 2
    assert res[0]["data"] == [1, 2]
    
    os.remove(path)

def test_format_load_save(spark):
    df = spark.createDataFrame([{"a": 1, "b": 2}])
    path = "tests/temp_test_format.parquet"
    
    # Save using format
    df.write.format("parquet").save(path)
    assert os.path.exists(path)
    
    # Load using format
    df2 = spark.read.format("parquet").load(path)
    res = df2.collect()
    assert res[0]["a"] == 1
    
    os.remove(path)
