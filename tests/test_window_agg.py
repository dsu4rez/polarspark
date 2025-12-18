import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F
from polarspark.sql.window import Window

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestWindowFraming").getOrCreate()

def test_collect_aggregates(spark):
    data = [{"id": 1, "v": "a"}, {"id": 1, "v": "b"}, {"id": 2, "v": "c"}]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("id").agg(
        F.collect_list("v").alias("list"),
        F.collect_set("v").alias("set")
    ).orderBy("id").collect()
    
    assert list(result[0]["list"]) == ["a", "b"]
    assert len(result[0]["set"]) == 2

def test_window_ranking_extended(spark):
    data = [{"id": 1, "v": 10}, {"id": 2, "v": 20}, {"id": 3, "v": 20}, {"id": 4, "v": 30}]
    df = spark.createDataFrame(data)
    w = Window.orderBy("v")
    
    result = df.select(
        "id",
        F.percent_rank().over(w).alias("pr"),
        F.cume_dist().over(w).alias("cd")
    ).orderBy("id").collect()
    
    # percent_rank = (rank - 1) / (n - 1)
    # v=10: rank=1 -> (1-1)/3 = 0.0
    # v=20: rank=2 -> (2-1)/3 = 0.333...
    assert result[0]["pr"] == 0.0
    assert abs(result[1]["pr"] - 0.3333) < 0.01

def test_ntile(spark):
    data = [{"v": i} for i in range(4)]
    df = spark.createDataFrame(data)
    result = df.select(F.ntile(2).over(Window.orderBy("v")).alias("n")).collect()
    # 0, 1 -> n=1; 2, 3 -> n=2
    assert [r["n"] for r in result] == [1, 1, 2, 2]
