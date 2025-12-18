import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestJoinsV2").getOrCreate()

def test_join_types(spark):
    df1 = spark.createDataFrame([{"id": 1, "a": "A"}, {"id": 2, "a": "B"}])
    df2 = spark.createDataFrame([{"id": 1, "b": "X"}, {"id": 3, "b": "Y"}])
    
    # Left join
    left = df1.join(df2, "id", "left").orderBy("id").collect()
    assert len(left) == 2
    assert left[0]["b"] == "X"
    assert left[1]["b"] is None
    
    # Right join
    right = df1.join(df2, "id", "right").orderBy("id").collect()
    assert len(right) == 2
    assert right[0]["a"] == "A"
    assert right[1]["a"] is None
    
    # Full join
    full = df1.join(df2, "id", "outer").orderBy("id").collect()
    assert len(full) == 3
    
    # Semi join (returns only cols from df1 where there's a match)
    semi = df1.join(df2, "id", "semi").collect()
    assert len(semi) == 1
    assert semi[0]["a"] == "A"
    assert "b" not in semi[0]
    
    # Anti join (returns only cols from df1 where there's NO match)
    anti = df1.join(df2, "id", "anti").collect()
    assert len(anti) == 1
    assert anti[0]["a"] == "B"
