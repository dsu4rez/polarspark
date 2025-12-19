import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestNonEquiJoins").getOrCreate()


def test_left_non_equi_join(spark):
    df1 = spark.createDataFrame([{"id1": 1, "val1": 10}, {"id1": 2, "val1": 20}])
    df2 = spark.createDataFrame([{"id2": 1, "val2": 5}, {"id2": 1, "val2": 15}])

    # Left join where val1 > val2
    # Row 1 (val1=10) matches val2=5.
    # Row 2 (val1=20) matches val2=5 and val2=15.
    # If we added a row in df1 that matches NOTHING:
    df1 = spark.createDataFrame(
        [{"id1": 1, "val1": 10}, {"id1": 2, "val1": 20}, {"id1": 3, "val1": 2}]
    )

    result = (
        df1.join(df2, F.col("val1") > F.col("val2"), "left")
        .orderBy("id1", "val2")
        .collect()
    )

    # Expected:
    # id1=1, val1=10 matches val2=5
    # id1=2, val1=20 matches val2=5, 15
    # id1=3, val1=2 matches NOTHING -> should have nulls for df2 cols

    assert len(result) == 4
    assert result[0]["id1"] == 1 and result[0]["val2"] == 5
    assert result[1]["id1"] == 2 and result[1]["val2"] == 5
    assert result[2]["id1"] == 2 and result[2]["val2"] == 15
    assert result[3]["id1"] == 3 and result[3]["val2"] is None


def test_semi_anti_non_equi(spark):
    df1 = spark.createDataFrame([{"id": 1, "v": 10}, {"id": 2, "v": 20}])
    df2 = spark.createDataFrame([{"v": 15}])

    # Semi: df1 where v > df2.v (v=20)
    semi = df1.join(df2, df1.v > df2.v, "semi").collect()
    assert len(semi) == 1
    assert semi[0]["id"] == 2

    # Anti: df1 where v NOT > df2.v (v=10)
    anti = df1.join(df2, df1.v > df2.v, "anti").collect()
    assert len(anti) == 1
    assert anti[0]["id"] == 1
