import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestHigherOrder").getOrCreate()


def test_transform_simple(spark):
    data = [{"arr": [1, 2, 3]}]
    df = spark.createDataFrame(data)

    # transform(col, lambda x: x * 2)
    result = df.select(
        F.transform(F.col("arr"), lambda x: x * 2).alias("res")
    ).collect()
    assert list(result[0]["res"]) == [2, 4, 6]


def test_transform_with_index(spark):
    data = [{"arr": [1, 2, 3]}]
    df = spark.createDataFrame(data)

    # transform(col, lambda x, i: x + i)
    result = df.select(
        F.transform(F.col("arr"), lambda x, i: x + i).alias("res")
    ).collect()
    assert list(result[0]["res"]) == [1, 3, 5]  # 1+0, 2+1, 3+2


def test_filter_array(spark):
    data = [{"arr": [1, 2, 3, 4]}]
    df = spark.createDataFrame(data)

    # filter(col, lambda x: x % 2 == 0)
    # Note: in Spark this is 'filter', but we already have df.filter.
    # We should name it 'filter' in functions.py but it might clash if not careful.
    # Spark calls it 'filter' or 'exists'.
    result = df.select(F.filter("arr", lambda x: x % 2 == 0).alias("res")).collect()
    assert list(result[0]["res"]) == [2, 4]


def test_exists_array(spark):
    data = [{"arr": [1, 2, 3]}]
    df = spark.createDataFrame(data)

    result = df.select(
        F.exists("arr", lambda x: x > 2).alias("e1"),
        F.exists("arr", lambda x: x > 5).alias("e2"),
    ).collect()

    assert result[0]["e1"] == True
    assert result[0]["e2"] == False


def test_forall_array(spark):
    data = [{"arr": [2, 4, 6]}, {"arr": [2, 5, 6]}]
    df = spark.createDataFrame(data)

    result = df.select(F.forall("arr", lambda x: x % 2 == 0).alias("res")).collect()
    assert result[0]["res"] == True
    assert result[1]["res"] == False


def test_aggregate_array(spark):
    data = [{"arr": [1, 2, 3, 4]}]
    df = spark.createDataFrame(data)

    # aggregate(col, initialValue, merge, finish)
    # sum of squares: 0 + 1^2 + 2^2 + 3^2 + 4^2 = 1+4+9+16 = 30
    result = df.select(
        F.aggregate("arr", F.lit(0), lambda acc, x: acc + (x * x)).alias("sum_sq")
    ).collect()

    assert result[0]["sum_sq"] == 30
