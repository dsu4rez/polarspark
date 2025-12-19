import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestComplexJoins").getOrCreate()


def test_equi_join_expression(spark):
    df1 = spark.createDataFrame(
        [{"id1": 1, "name": "Alice"}, {"id1": 2, "name": "Bob"}]
    )
    df2 = spark.createDataFrame([{"id2": 1, "age": 30}, {"id2": 3, "age": 40}])

    # Join on df1.id1 == df2.id2
    result = df1.join(df2, F.col("id1") == F.col("id2")).orderBy("id1").collect()

    assert len(result) == 1
    assert result[0]["name"] == "Alice"
    assert result[0]["age"] == 30


def test_non_equi_join(spark):
    df1 = spark.createDataFrame([{"val1": 10}, {"val1": 20}])
    df2 = spark.createDataFrame([{"val2": 5}, {"val2": 15}, {"val2": 25}])

    # Left join where val1 > val2
    # In Spark: df1.join(df2, df1.val1 > df2.val2, "left")
    result = (
        df1.join(df2, F.col("val1") > F.col("val2"), "inner")
        .orderBy("val1", "val2")
        .collect()
    )

    # (10 > 5) -> True
    # (20 > 5) -> True
    # (20 > 15) -> True
    assert len(result) == 3
    assert result[0]["val1"] == 10 and result[0]["val2"] == 5
    assert result[1]["val1"] == 20 and result[1]["val2"] == 5
    assert result[2]["val1"] == 20 and result[2]["val2"] == 15


def test_complex_condition_join(spark):
    df1 = spark.createDataFrame([{"id": 1, "a": 10}, {"id": 2, "a": 20}])
    df2 = spark.createDataFrame([{"id": 1, "b": 10}, {"id": 1, "b": 5}])

    # Join on id and a == b
    cond = (F.col("df1.id") == F.col("df2.id")) & (F.col("a") == F.col("b"))
    # Note: Column names might need prefixes if there's ambiguity, but here we keep it simple.
    result = df1.join(
        df2, (F.col("id") == F.col("id")) & (F.col("a") == F.col("b"))
    ).collect()
    # In Spark, this would cause "ambiguous column" error if not prefixed, but our simple impl might just pick one.
    # For now let's just use different names to avoid ambiguity issues in the test itself.

    df1 = spark.createDataFrame([{"id1": 1, "a": 10}, {"id1": 2, "a": 20}])
    df2 = spark.createDataFrame([{"id2": 1, "b": 10}, {"id2": 1, "b": 5}])
    result = df1.join(
        df2, (F.col("id1") == F.col("id2")) & (F.col("a") == F.col("b"))
    ).collect()

    assert len(result) == 1
    assert result[0]["id1"] == 1
    assert result[0]["a"] == 10
    assert result[0]["b"] == 10
