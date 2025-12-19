import pytest
from polarspark.sql import SparkSession, StringType, IntegerType
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestUDF").getOrCreate()


def test_basic_udf(spark):
    df = spark.createDataFrame([{"name": "alice"}, {"name": "bob"}])

    upper_udf = F.udf(lambda x: x.upper(), StringType())
    res = df.withColumn("upper_name", upper_udf(F.col("name"))).collect()

    assert res[0]["upper_name"] == "ALICE"
    assert res[1]["upper_name"] == "BOB"


def test_udf_decorator(spark):
    df = spark.createDataFrame([{"age": 20}, {"age": 30}])

    @F.udf(returnType=IntegerType())
    def add_ten(x):
        return x + 10

    res = df.withColumn("age_plus_ten", add_ten(F.col("age"))).collect()
    assert res[0]["age_plus_ten"] == 30
    assert res[1]["age_plus_ten"] == 40


def test_multi_col_udf(spark):
    df = spark.createDataFrame([{"a": 1, "b": 2}, {"a": 10, "b": 20}])

    sum_udf = F.udf(lambda x, y: x + y, IntegerType())
    res = df.withColumn("sum", sum_udf(F.col("a"), F.col("b"))).collect()

    assert res[0]["sum"] == 3
    assert res[1]["sum"] == 30


def test_udf_null_handling(spark):
    df = spark.createDataFrame([{"val": "hello"}, {"val": None}])

    len_udf = F.udf(lambda x: len(x) if x is not None else -1, IntegerType())
    res = df.withColumn("len", len_udf(F.col("val"))).collect()

    assert res[0]["len"] == 5
    assert res[1]["len"] == -1
