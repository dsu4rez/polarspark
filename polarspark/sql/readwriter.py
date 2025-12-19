import polars as pl
from typing import Any, Dict, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .dataframe import DataFrame


class DataFrameReader:
    def __init__(self):
        self._options: Dict[str, Any] = {}
        self._format: Optional[str] = None

    def option(self, key: str, value: Any) -> "DataFrameReader":
        self._options[key] = value
        return self

    def options(self, **options) -> "DataFrameReader":
        self._options.update(options)
        return self

    def format(self, source: str) -> "DataFrameReader":
        self._format = source
        return self

    def load(self, path: Optional[str] = None) -> "DataFrame":
        from .dataframe import DataFrame

        fmt = self._format or "parquet"
        if fmt == "csv":
            return self.csv(path)
        elif fmt == "parquet":
            return self.parquet(path)
        elif fmt == "json":
            return self.json(path)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

    def csv(self, path: str) -> "DataFrame":
        from .dataframe import DataFrame

        # Map Spark options to Polars options
        sep = self._options.get("sep", self._options.get("delimiter", ","))
        header = str(self._options.get("header", "false")).lower() == "true"
        inferSchema = str(self._options.get("inferSchema", "false")).lower() == "true"

        return DataFrame(
            pl.read_csv(
                path, separator=sep, has_header=header, try_parse_dates=inferSchema
            )
        )

    def parquet(self, path: str) -> "DataFrame":
        from .dataframe import DataFrame

        return DataFrame(pl.read_parquet(path))

    def json(self, path: str) -> "DataFrame":
        from .dataframe import DataFrame

        return DataFrame(pl.read_json(path))


class DataFrameWriter:
    def __init__(self, df: "DataFrame"):
        self._df = df
        self._options: Dict[str, Any] = {}
        self._format: Optional[str] = None
        self._mode: str = "error"  # Spark default is error

    def option(self, key: str, value: Any) -> "DataFrameWriter":
        self._options[key] = value
        return self

    def options(self, **options) -> "DataFrameWriter":
        self._options.update(options)
        return self

    def format(self, source: str) -> "DataFrameWriter":
        self._format = source
        return self

    def mode(self, saveMode: str) -> "DataFrameWriter":
        self._mode = saveMode.lower()
        return self

    def save(self, path: Optional[str] = None) -> None:
        fmt = self._format or "parquet"
        if fmt == "csv":
            self.csv(path)
        elif fmt == "parquet":
            self.parquet(path)
        elif fmt == "json":
            self.json(path)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

    def csv(self, path: str) -> None:
        sep = self._options.get("sep", self._options.get("delimiter", ","))
        header = str(self._options.get("header", "true")).lower() == "true"
        self._df._df.write_csv(path, separator=sep, include_header=header)

    def parquet(self, path: str) -> None:
        self._df._df.write_parquet(path)

    def json(self, path: str) -> None:
        self._df._df.write_json(path)
