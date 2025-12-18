import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F
from polarspark.sql.window import Window

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestWindowFunctions").getOrCreate()

def test_row_number(spark):
    data = [
        {"grp": "A", "val": 10},
        {"grp": "A", "val": 20},
        {"grp": "B", "val": 5}
    ]
    df = spark.createDataFrame(data)
    
    window = Window.partitionBy("grp").orderBy("val")
    result = df.select(
        F.col("grp"),
        F.col("val"),
        F.row_number().over(window).alias("rn")
    ).orderBy("grp", "rn").collect()
    
    assert result[0]["rn"] == 1
    assert result[1]["rn"] == 2
    assert result[2]["rn"] == 1

def test_window_agg(spark):
    data = [
        {"grp": "A", "val": 10},
        {"grp": "A", "val": 20}
    ]
    df = spark.createDataFrame(data)
    
    window = Window.partitionBy("grp")
    result = df.select(
        F.sum("val").over(window).alias("sum_val")
    ).collect()
    
    assert result[0]["sum_val"] == 30
    assert result[1]["sum_val"] == 30
