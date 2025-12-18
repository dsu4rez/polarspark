import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestAmbiguousJoins").getOrCreate()

def test_ambiguous_join(spark):
    df1 = spark.createDataFrame([{"id": 1, "a": 10}, {"id": 2, "a": 20}])
    df2 = spark.createDataFrame([{"id": 1, "b": 100}, {"id": 3, "b": 300}])
    
    # In Spark, this is ambiguous if not handled.
    # PolarSpark needs a way to distinguish.
    # For now, let's see if we can just assume list of strings for equi-joins is better.
    # But if user uses Column:
    try:
        result = df1.join(df2, F.col("id") == F.col("id")).collect()
        # In Polars, pl.col("id") == pl.col("id") is always true. 
        # result would be a cross join if it just evaluated the tautology.
        print(f"Result len: {len(result)}")
    except Exception as e:
        print(f"Error: {e}")

    # The real way to solve this in Polars shim without deep changes is to 
    # detect equality of two same-named columns and convert to 'on=["id"]'
    
def test_inequality_join(spark):
    df1 = spark.createDataFrame([{"id": 1, "t": 10}, {"id": 1, "t": 20}])
    df2 = spark.createDataFrame([{"id": 1, "start": 5, "end": 15}])
    
    # Join on id and t between start and end
    # Spark: df1.join(df2, (df1.id == df2.id) & (df1.t >= df2.start) & (df1.t <= df2.end))
    cond = (F.col("id") == F.col("id")) & (F.col("t") >= F.col("start")) & (F.col("t") <= F.col("end"))
    
    # Since we have ambiguity in 'id', this is still hard.
    # But if we use DIFFERENT names:
    df1 = spark.createDataFrame([{"id1": 1, "t": 10}, {"id1": 1, "t": 20}])
    df2 = spark.createDataFrame([{"id2": 1, "start": 5, "end": 15}])
    cond = (F.col("id1") == F.col("id2")) & (F.col("t") >= F.col("start")) & (F.col("t") <= F.col("end"))
    
    result = df1.join(df2, cond).collect()
    assert len(result) == 1
    assert result[0]["t"] == 10
