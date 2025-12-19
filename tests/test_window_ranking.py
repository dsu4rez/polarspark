import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F
from polarspark.sql.window import Window


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestWindowRankingFunctions").getOrCreate()


def test_ranking_functions(spark):
    data = [
        {"grp": "A", "val": 10},
        {"grp": "A", "val": 20},
        {"grp": "A", "val": 20},
        {"grp": "A", "val": 30},
    ]
    df = spark.createDataFrame(data)

    window = Window.partitionBy("grp").orderBy("val")
    result = (
        df.select(
            F.col("val"),
            F.rank().over(window).alias("rank"),
            F.dense_rank().over(window).alias("dense_rank"),
        )
        .orderBy("val")
        .collect()
    )

    # Val 10
    assert result[0]["rank"] == 1
    assert result[0]["dense_rank"] == 1
    # Val 20 (tie)
    assert result[1]["rank"] == 2
    assert result[1]["dense_rank"] == 2
    assert result[2]["rank"] == 2
    assert result[2]["dense_rank"] == 2
    # Val 30
    assert result[3]["rank"] == 4  # Rank skips
    assert result[3]["dense_rank"] == 3  # Dense rank doesn't skip


def test_lag_lead(spark):
    data = [{"id": 1, "val": 10}, {"id": 2, "val": 20}, {"id": 3, "val": 30}]
    df = spark.createDataFrame(data)

    window = Window.orderBy("id")
    result = (
        df.select(
            F.col("id"),
            F.lag("val", 1).over(window).alias("prev"),
            F.lead("val", 1).over(window).alias("next"),
        )
        .orderBy("id")
        .collect()
    )

    assert result[0]["prev"] is None
    assert result[0]["next"] == 20
    assert result[1]["prev"] == 10
    assert result[1]["next"] == 30
    assert result[2]["prev"] == 20
    assert result[2]["next"] is None
