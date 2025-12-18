# PolarSpark 🚀

**PolarSpark** is a high-performance, PySpark-compatible API wrapper built on top of the [Polars](https://github.com/pola-rs/polars) DataFrame library. It allows you to write Spark-style code while enjoying the lightning-fast execution speeds and low memory footprint of Polars.

## ✨ Key Features

- **Familiar API**: Mirroring the `pyspark.sql` structure (`SparkSession`, `DataFrame`, `functions`, `Column`, `Window`).
- **Blazing Performance**: Powered by the Polars query engine.
- **Extensive Function Library**: 70+ compatible functions across:
    - **String Manipulation**: `regexp_replace`, `split`, `translate`, `initcap`, etc.
    - **Date & Time**: `add_months`, `datediff`, `unix_timestamp`, `date_format`.
    - **Collections**: `array_union`, `arrays_zip`, `posexplode`, `map_from_arrays`.
    - **Higher-Order Functions**: `transform`, `filter`, `exists`, `aggregate`, `zip_with`.
    - **Aggregates & Math**: `approx_count_distinct`, `corr`, `skewness`, `hypot`, `pow`.
- **Advanced Joins**: Support for `inner`, `left`, `right`, `full`, `semi`, `anti`, and `cross` joins, including expression-based join conditions.
- **Window Functions**: Robust support for `partitionBy`, `orderBy`, and ranking functions like `rank()`, `dense_rank()`, `ntile()`.

## 🚀 Quick Start

```python
from polarspark.sql import SparkSession
import polarspark.sql.functions as F

# Initialize Session
spark = SparkSession.builder.appName("PolarSparkExample").getOrCreate()

# Create DataFrame
data = [
    {"name": "Alice", "age": 30, "tags": ["tech", "lead"]},
    {"name": "Bob", "age": 25, "tags": ["design"]}
]
df = spark.createDataFrame(data)

# Transform using familiar Spark syntax
result = df.withColumn("is_senior", F.col("age") >= 30) \
           .withColumn("upper_name", F.upper(F.col("name"))) \
           .select("upper_name", "is_senior", F.size("tags").alias("tag_count"))

result.show()
```

## 🧪 Testing

PolarSpark is built with a test-driven approach. We currently have a robust suite of unit tests verifying compatibility.

To run the tests:
```bash
export PYTHONPATH=$PYTHONPATH:.
pytest tests/
```

## 🏗️ Project Structure

- `polarspark/sql/`: Core implementation of the Spark API.
    - `session.py`: `SparkSession` and `DataFrame` creation.
    - `dataframe.py`: `DataFrame` operations (select, join, groupBy).
    - `functions.py`: The massive library of Spark-equivalent functions.
    - `column.py`: Logical column operations and expressions.
    - `window.py`: Window specification and framing.
- `tests/`: Extensive unit test suite.

## ⚠️ Current Status & Limitations

PolarSpark is an active project aiming for maximum PySpark API coverage. 
- **Inner Joins**: Expression-based joins (`df1.id == df2.id + 1`) are fully supported for inner joins.
- **Non-Inner Joins**: Join expressions for left/right/full joins are currently being refined.
- **UDFs**: Basic Python UDF support is planned.
- **IO**: Currently supports direct creation from Python objects; Parquet/CSV readers are under development.

---
Built by [dsu4rez] as a high-performance bridge between Spark and Polars.
