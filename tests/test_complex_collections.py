import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestComplexCollections").getOrCreate()


def test_struct_access(spark):
    data = [{"user": {"name": "Alice", "age": 30}}]
    df = spark.createDataFrame(data)

    # Access field using getField
    result = df.select(
        F.col("user").getField("name").alias("user_name"),
        F.col("user").getField("age").alias("user_age"),
    ).collect()

    assert result[0]["user_name"] == "Alice"
    assert result[0]["user_age"] == 30


def test_arrays_zip(spark):
    data = [{"ids": [1, 2], "names": ["A", "B"]}]
    df = spark.createDataFrame(data)

    # arrays_zip(ids, names) -> list of structs [{ids: 1, names: A}, {ids: 2, names: B}]
    result = df.select(
        F.arrays_zip(F.col("ids"), F.col("names")).alias("zipped")
    ).collect()

    zipped = result[0]["zipped"]
    assert len(zipped) == 2
    assert zipped[0]["ids"] == 1
    assert zipped[0]["names"] == "A"


def test_posexplode(spark):
    data = [{"items": ["apple", "banana"]}]
    df = spark.createDataFrame(data)

    # posexplode is tricky in select if it returns multiple cols.
    # Usually in Spark: select(posexplode(col))
    # For now, let's implement it to return a list of structs with 'pos' and 'col'
    # so it can be exploded.
    # Actually, let's try to match Spark closely.

    result = df.select(F.posexplode(F.col("items"))).collect()
    # If we return a struct, we might need to alias it or it might auto-flatten if possible.
    # In Spark it results in 'pos' and 'col' columns.
    assert "pos" in result[0]
    assert "col" in result[0]
    assert result[0]["pos"] == 0
    assert result[0]["col"] == "apple"
    assert result[1]["pos"] == 1
    assert result[1]["col"] == "banana"


def test_named_struct(spark):
    df = spark.createDataFrame([{"a": 1, "b": 2}])
    result = df.select(
        F.named_struct(F.lit("x"), F.col("a"), F.lit("y"), F.col("b")).alias("s")
    ).collect()

    assert result[0]["s"]["x"] == 1
    assert result[0]["s"]["y"] == 2
