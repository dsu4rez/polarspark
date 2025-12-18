import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestJoinsGroupBy").getOrCreate()

def test_groupby_agg(spark):
    data = [
        {"grp": "A", "val": 10},
        {"grp": "A", "val": 20},
        {"grp": "B", "val": 30}
    ]
    df = spark.createDataFrame(data)
    
    result = df.groupBy("grp").agg(F.sum("val").alias("total")).orderBy("grp").collect()
    
    assert result[0]["grp"] == "A"
    assert result[0]["total"] == 30
    assert result[1]["grp"] == "B"
    assert result[1]["total"] == 30

def test_joins(spark):
    df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
    df2 = spark.createDataFrame([{"id": 1, "age": 30}, {"id": 3, "age": 40}])
    
    # Inner join
    result = df1.join(df2, on="id", how="inner").orderBy("id").collect()
    assert len(result) == 1
    assert result[0]["name"] == "Alice"
    assert result[0]["age"] == 30
    
    # Left join
    result = df1.join(df2, on="id", how="left").orderBy("id").collect()
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2
    assert result[1]["age"] is None
