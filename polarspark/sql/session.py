import polars as pl
from .dataframe import DataFrame
from typing import List, Any, Optional
from .readwriter import DataFrameReader


class SparkSession:
    class Builder:
        def __init__(self):
            self._app_name = "PolarSpark"

        def appName(self, name: str) -> "SparkSession.Builder":
            self._app_name = name
            return self

        def config(self, key: str, value: Any) -> "SparkSession.Builder":
            # Ignored for now
            return self

        def getOrCreate(self) -> "SparkSession":
            return SparkSession()

    builder = Builder()

    def createDataFrame(self, data: Any, schema: Optional[Any] = None) -> DataFrame:
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            df = pl.from_dicts(data)
        else:
            df = pl.from_records(data, schema=schema)
        return DataFrame(df)

    @property
    def read(self) -> DataFrameReader:
        return DataFrameReader()
