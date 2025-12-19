import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestStructJoins").getOrCreate()


def test_struct_field_join(spark):
    # Data with structs
    df1 = spark.createDataFrame(
        [
            {"info": {"id": 1, "name": "A"}, "val": 10},
            {"info": {"id": 2, "name": "B"}, "val": 20},
        ]
    )

    df2 = spark.createDataFrame(
        [
            {"info": {"id": 1, "extra": "X"}, "score": 100},
            {"info": {"id": 3, "extra": "Z"}, "score": 300},
        ]
    )

    res = df1.join(df2, df1.info.getField("id") == df2.info.getField("id")).collect()
    assert len(res) == 1
    assert res[0]["val"] == 10

    # Test attribute style: df1.info.id
    res2 = df1.join(df2, df1.info.id == df2.info.id).collect()
    assert len(res2) == 1
    assert res2[0]["score"] == 100

    # Test aliased attribute style
    t1 = df1.alias("t1")
    t2 = df2.alias("t2")
    res3 = (
        t1.join(t2, t1.info.id == t2.info.id)
        .select(t1.info.id.alias("id1"), t2.score)
        .collect()
    )
    assert len(res3) == 1
    assert res3[0]["id1"] == 1
    assert res3[0]["score"] == 100

    # Test F.col style with deep dots: t1.info.id
    res4 = t1.join(t2, F.col("t1.info.id") == F.col("t2.info.id")).collect()
    assert len(res4) == 1
    assert res4[0]["val"] == 10
