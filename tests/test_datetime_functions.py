import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F
from datetime import date, datetime


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestDateTimeFunctions").getOrCreate()


def test_current_date(spark):
    df = spark.createDataFrame([{"id": 1}])
    result = df.select(F.current_date().alias("today")).collect()
    assert isinstance(result[0]["today"], date)
    assert result[0]["today"] == date.today()


def test_date_arithmetic(spark):
    data = [{"dt": date(2023, 1, 1)}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.date_add(F.col("dt"), 5).alias("add"), F.date_sub(F.col("dt"), 5).alias("sub")
    ).collect()
    assert result[0]["add"] == date(2023, 1, 6)
    assert result[0]["sub"] == date(2022, 12, 27)


def test_date_extract(spark):
    data = [{"dt": date(2023, 5, 15)}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.year(F.col("dt")).alias("y"),
        F.month(F.col("dt")).alias("m"),
        F.dayofmonth(F.col("dt")).alias("d"),
    ).collect()
    assert result[0]["y"] == 2023
    assert result[0]["m"] == 5
    assert result[0]["d"] == 15


def test_datediff(spark):
    data = [{"start": date(2023, 1, 1), "end": date(2023, 1, 10)}]
    df = spark.createDataFrame(data)
    result = df.select(F.datediff(F.col("end"), F.col("start")).alias("diff")).collect()
    # Spark datediff usually returns an integer (number of days)
    assert result[0]["diff"] == 9
