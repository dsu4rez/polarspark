import pytest
from datetime import datetime
from polarspark.sql import SparkSession
import polarspark.sql.functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestDateTimeEncryption").getOrCreate()


def test_date_format(spark):
    data = [{"dt": datetime(2023, 1, 1, 12, 0, 0)}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.date_format(F.col("dt"), "yyyy-MM-dd").alias("date"),
        F.unix_timestamp(F.col("dt")).alias("ts"),
    ).collect()

    assert result[0]["date"] == "2023-01-01"
    assert result[0]["ts"] > 0


def test_encryption(spark):
    data = [{"text": "hello"}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.md5(F.col("text")).alias("md5"),
        F.sha1(F.col("text")).alias("sha1"),
        F.sha2(F.col("text"), 256).alias("sha2"),
    ).collect()

    import hashlib

    assert result[0]["md5"] == hashlib.md5(b"hello").hexdigest()
    assert result[0]["sha1"] == hashlib.sha1(b"hello").hexdigest()
    assert result[0]["sha2"] == hashlib.sha256(b"hello").hexdigest()
