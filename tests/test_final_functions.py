import pytest
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

@pytest.fixture
def spark():
    return SparkSession.builder.appName("TestFinalFunctions").getOrCreate()

def test_agg_v3(spark):
    data = [{"a": 1, "b": 10}, {"a": 2, "b": 20}]
    df = spark.createDataFrame(data)
    result = df.select(
        F.corr("a", "b").alias("corr"),
        F.covar_samp("a", "b").alias("cov")
    ).collect()
    
    assert result[0]["corr"] == 1.0
    # covar_samp for (1,2) and (10,20) is ((1-1.5)(10-15) + (2-1.5)(20-15)) / 1 
    # = (-0.5*-5 + 0.5*5) / 1 = (2.5 + 2.5) / 1 = 5.0
    assert result[0]["cov"] == 5.0

def test_string_v6(spark):
    data = [{"text": "Hello"}]
    df = spark.createDataFrame(data)
    # translate(col, matching, replacement)
    result = df.select(
        F.translate(F.col("text"), "el", "12").alias("trans")
    ).collect()
    
    # H e l l o -> H 1 2 2 o
    assert result[0]["trans"] == "H122o"
