import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestBinaryBitwiseFunctions").getOrCreate()

def test_bitwise(spark):
    data = [{"a": 1, "b": 2}]
    df = spark.createDataFrame(data)
    # 1 is 01, 2 is 10. NOT 1 is ... (depends on bit width)
    # Spark bitwiseNOT typically works on longs.
    result = df.select(
        F.shiftLeft(F.col("a"), 1).alias("left"),
        F.shiftRight(F.col("b"), 1).alias("right")
    ).collect()
    
    assert result[0]["left"] == 2
    assert result[0]["right"] == 1

def test_binary_hex(spark):
    data = [{"val": 255}]
    df = spark.createDataFrame(data)
    result = df.select(F.hex(F.col("val")).alias("hex")).collect()
    assert result[0]["hex"].lower() == "ff"

def test_array_ext(spark):
    data = [{"arr1": [1, 2, 3], "arr2": [3, 4, 5]}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.array_union(F.col("arr1"), F.col("arr2")).alias("union"),
        F.array_remove(F.col("arr1"), 1).alias("remove")
    ).collect()
    
    # union of [1,2,3] and [3,4,5] is usually [1,2,3,4,5] (distinct)
    assert set(result[0]["union"]) == {1, 2, 3, 4, 5}
    assert list(result[0]["remove"]) == [2, 3]
