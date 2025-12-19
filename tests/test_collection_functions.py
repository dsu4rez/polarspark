import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestCollectionFunctions").getOrCreate()


def test_array_size(spark):
    data = [{"a": 1, "b": 2}]
    df = spark.createDataFrame(data)
    result = (
        df.select(
            F.array(F.col("a"), F.col("b")).alias("arr"),
        )
        .select(F.col("arr"), F.size(F.col("arr")).alias("len"))
        .collect()
    )

    # In Polars, it returns it as a list/Series-like object
    assert list(result[0]["arr"]) == [1, 2]
    assert result[0]["len"] == 2


def test_explode(spark):
    # This might require some changes in DataFrame because explode usually expands rows
    # Spark's explode(col) is used in select.
    # In Polars, it's df.explode(col_name)
    data = [{"id": 1, "vals": [10, 20]}]
    df = spark.createDataFrame(data)

    # We need to see if our Column can handle being used in a way that generates multiple rows.
    # Spark: df.select(F.explode("vals"))
    result = df.select(F.explode(F.col("vals")).alias("val")).collect()

    assert len(result) == 2
    assert result[0]["val"] == 10
    assert result[1]["val"] == 20


def test_struct(spark):
    data = [{"a": 1, "b": "x"}]
    df = spark.createDataFrame(data)
    result = df.select(F.struct("a", "b").alias("s")).collect()

    # Polars returns structs as dicts in to_dicts()
    assert result[0]["s"] == {"a": 1, "b": "x"}
