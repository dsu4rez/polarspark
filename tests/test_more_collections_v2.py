import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestMoreCollections").getOrCreate()


def test_map_functions(spark):
    data = [{"ks": ["k1", "k2"], "vs": [1, 2]}]
    df = spark.createDataFrame(data)

    # map_from_arrays(keys, values)
    # result is list of structs [{"key": "k1", "value": 1}, ...]
    df_map = df.select(F.map_from_arrays(F.col("ks"), F.col("vs")).alias("m"))
    result = df_map.collect()

    m = result[0]["m"]
    assert len(m) == 2
    assert m[0]["key"] == "k1"
    assert m[0]["value"] == 1

    # map_keys, map_values
    res_kv = df_map.select(
        F.map_keys("m").alias("k"), F.map_values("m").alias("v")
    ).collect()
    assert list(res_kv[0]["k"]) == ["k1", "k2"]
    assert list(res_kv[0]["v"]) == [1, 2]


def test_array_ops_v3(spark):
    data = [{"a1": [1, 2, 2, 3], "a2": [3, 4]}]
    df = spark.createDataFrame(data)

    result = df.select(
        F.array_distinct("a1").alias("dist"),
        F.array_intersect("a1", "a2").alias("inter"),
        F.array_except("a1", "a2").alias("exc"),
        F.element_at("a1", 1).alias("first"),  # 1-indexed in Spark
        F.element_at("a1", -1).alias("last"),
    ).collect()

    assert list(result[0]["dist"]) == [1, 2, 3]
    assert list(result[0]["inter"]) == [3]
    assert list(result[0]["exc"]) == [1, 2]
    assert result[0]["first"] == 1
    assert result[0]["last"] == 3


def test_slice(spark):
    data = [{"a": [1, 2, 3, 4, 5]}]
    df = spark.createDataFrame(data)
    # slice(col, start, length) - 1-indexed
    result = df.select(F.slice("a", 2, 3).alias("s")).collect()
    assert list(result[0]["s"]) == [2, 3, 4]
