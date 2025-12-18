import pytest
from datetime import date
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestLogicDateFunctions").getOrCreate()

def test_logical_functions(spark):
    data = [{"a": 1, "b": 5, "c": 3}, {"a": 10, "b": 2, "c": 8}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.greatest("a", "b", "c").alias("max_val"),
        F.least("a", "b", "c").alias("min_val")
    ).collect()
    
    assert result[0]["max_val"] == 5
    assert result[0]["min_val"] == 1
    assert result[1]["max_val"] == 10
    assert result[1]["min_val"] == 2

def test_nanvl(spark):
    data = [{"a": float('nan'), "b": 1.0}, {"a": 2.0, "b": 3.0}]
    df = spark.createDataFrame(data)
    result = df.select(F.nanvl(F.col("a"), F.col("b")).alias("val")).collect()
    
    assert result[0]["val"] == 1.0
    assert result[1]["val"] == 2.0

def test_date_advanced(spark):
    data = [{"dt": date(2023, 1, 15)}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.add_months(F.col("dt"), 1).alias("next_month"),
        F.add_months(F.col("dt"), -1).alias("prev_month"),
        F.last_day(F.col("dt")).alias("last_day")
    ).collect()
    
    assert result[0]["next_month"] == date(2023, 2, 15)
    assert result[0]["prev_month"] == date(2022, 12, 15)
    assert result[0]["last_day"] == date(2023, 1, 31)

def test_months_between(spark):
    data = [
        {"d1": date(2023, 3, 31), "d2": date(2023, 1, 31)}
    ]
    df = spark.createDataFrame(data)
    result = df.select(F.months_between(F.col("d1"), F.col("d2")).alias("diff")).collect()
    
    # Spark months_between can return floats, but let's test integer months for now
    assert result[0]["diff"] == 2.0
