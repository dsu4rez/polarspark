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
- **Advanced Joins**: Support for all join types (`inner`, `left`, `right`, `full`, `semi`, `anti`, `cross`). Includes full support for expression-based join conditions (e.g., `df1.id == df2.id`), self-joins, and non-equi joins.
- **Ambiguity & Aliasing**: Smart `DataFrame.alias()` support and automatic column resolution for joined results, including disambiguation between table aliases and nested struct fields.
- **Window Functions**: Robust support for `partitionBy`, `orderBy`, and ranking functions like `rank()`, `dense_rank()`, `ntile()`, and framing.
- **Struct & Deep Nesting**: Full support for struct field access via `getField`, dot-notation (`df.user.id`), and recursive resolution in join conditions.


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

# Complex Joins & Aliases Example
t1 = df.alias("t1")
t2 = df.alias("t2")
# Self-join to find pairs, disambiguating 't1.id' and 't2.id' automatically
pairs = t1.join(t2, t1.id < t2.id).select(
    F.col("t1.name").alias("user1"),
    F.col("t2.name").alias("user2")
)
pairs.show()

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
- **Expression Joins**: All standard Sparks join types are fully supported with complex expressions.
- **UDFs**: Basic Python UDF support is planned.
- **IO**: Currently supports direct creation from Python objects; Parquet/CSV readers are under development.


---
Built by [dsu4rez] as a high-performance bridge between Spark and Polars.
