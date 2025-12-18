import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestAliases").getOrCreate()

def test_dataframe_alias_join(spark):
    df1 = spark.createDataFrame([{"id": 1, "a": 10}, {"id": 2, "a": 20}]).alias("df1")
    df2 = spark.createDataFrame([{"id": 1, "b": 100}, {"id": 3, "b": 300}]).alias("df2")
    
    # Selecting from specific alias
    # In Spark: res = df1.join(df2, df1.id == df2.id).select("df1.id", "a", "b")
    # Our current impl of .alias() needs to be checked.
    
    # This is the "real" test of alias support in expressions
    res = df1.join(df2, F.col("df1.id") == F.col("df2.id")).select("df1.id", "a", "b").collect()
    
    assert len(res) == 1
    assert res[0]["id"] == 1
    assert res[0]["a"] == 10
    assert res[0]["b"] == 100


def test_alias_string_selection(spark):
    df = spark.createDataFrame([{"id": 1, "val": 10}]).alias("t1")
    
    # Should be able to select via "t1.id"
    res = df.select("t1.id").collect()
    assert res[0]["id"] == 1

def test_self_join_aliases(spark):
    df = spark.createDataFrame([{"id": 1, "val": "A"}, {"id": 2, "val": "B"}])
    
    # Self-join to find pairs
    t1 = df.alias("t1")
    t2 = df.alias("t2")
    
    # In Spark: t1.join(t2, t1.id < t2.id).select(t1.id.alias("id1"), t2.id.alias("id2"))
    # 1 < 2 -> Correct
    res = t1.join(t2, t1.id < t2.id).select(t1.id.alias("id1"), t2.id.alias("id2")).collect()
    
    assert len(res) == 1
    assert res[0]["id1"] == 1
    assert res[0]["id2"] == 2

