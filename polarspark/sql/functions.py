from typing import Any, Union, Optional
from datetime import datetime
import polars as pl
from .column import Column
from .types import StringType, IntegerType, LongType, FloatType, DoubleType, BooleanType


class UserDefinedFunction:
    def __init__(self, func, returnType):
        self.func = func
        self.returnType = returnType

    def __call__(self, *cols) -> Column:
        pl_type = self._spark_type_to_polars(self.returnType)

        if len(cols) == 1:
            expr = cols[0]._expr if isinstance(cols[0], Column) else pl.col(cols[0])
            return Column(
                expr.map_elements(self.func, return_dtype=pl_type, skip_nulls=False)
            )
        else:
            # Multi-column UDF: use struct to pass multiple values as a dict
            col_exprs = []
            for c in cols:
                if isinstance(c, Column):
                    col_exprs.append(c._expr)
                else:
                    col_exprs.append(pl.col(c))

            struct_expr = pl.struct(col_exprs)

            def wrapper(val_dict):
                if val_dict is None:
                    # Try to call func with None for each arg
                    try:
                        return self.func(*([None] * len(cols)))
                    except:
                        return None
                return self.func(*val_dict.values())

            return Column(
                struct_expr.map_elements(
                    wrapper, return_dtype=pl_type, skip_nulls=False
                )
            )

    def _spark_type_to_polars(self, spark_type):
        s = str(spark_type)
        if "StringType" in s:
            return pl.Utf8
        if "IntegerType" in s:
            return pl.Int64
        if "LongType" in s:
            return pl.Int64
        if "FloatType" in s:
            return pl.Float32
        if "DoubleType" in s:
            return pl.Float64
        if "BooleanType" in s:
            return pl.Boolean
        return None


def udf(f=None, returnType=None):
    if f is None or not callable(f):
        # Decorator: @udf(returnType=StringType())
        return lambda func: UserDefinedFunction(func, f or returnType)
    # Function: udf(lambda x: x, StringType())
    return UserDefinedFunction(f, returnType)


# Encryption & Hashing


def md5(col_name: Union[str, Column]) -> Column:
    import hashlib

    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(
        expr.map_elements(
            lambda x: hashlib.md5(x.encode()).hexdigest() if x is not None else None,
            return_dtype=pl.Utf8,
        )
    )


def sha1(col_name: Union[str, Column]) -> Column:
    import hashlib

    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(
        expr.map_elements(
            lambda x: hashlib.sha1(x.encode()).hexdigest() if x is not None else None,
            return_dtype=pl.Utf8,
        )
    )


def sha2(col_name: Union[str, Column], num_bits: int) -> Column:
    import hashlib

    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)

    def hash_fn(x):
        if x is None:
            return None
        if num_bits == 256:
            return hashlib.sha256(x.encode()).hexdigest()
        if num_bits == 512:
            return hashlib.sha512(x.encode()).hexdigest()
        return hashlib.sha256(x.encode()).hexdigest()

    return Column(expr.map_elements(hash_fn, return_dtype=pl.Utf8))


# Date V3
def date_format(col_name: Union[str, Column], format: str) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # Basic mapping of Spark -> Python strftime
    py_format = format.replace("yyyy", "%Y").replace("MM", "%m").replace("dd", "%d")
    py_format = py_format.replace("HH", "%H").replace("mm", "%M").replace("ss", "%S")
    return Column(expr.dt.to_string(py_format))


def unix_timestamp(
    col_name: Optional[Union[str, Column]] = None, format: str = "yyyy-MM-dd HH:mm:ss"
) -> Column:
    if col_name is None:
        return Column(pl.lit(datetime.now().timestamp()).cast(pl.Int64))

    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # If it's already a date/datetime
    # return Column(expr.dt.timestamp().floordiv(1000000)) # micro to sec
    return Column((expr.dt.timestamp() // 1000000).cast(pl.Int64))


def transform(col_name: Union[str, Column], f: Any) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    import inspect

    sig = inspect.signature(f)
    if len(sig.parameters) == 1:
        # Standard: 1 arg
        res_col = f(Column(pl.element()))
        return Column(expr.list.eval(res_col._expr))
    else:
        # Two args: (element, index)
        def transform_2_fn(arr):
            if arr is None:
                return None
            res = []
            for i, v in enumerate(arr):
                out = f(v, i)
                if isinstance(out, Column):
                    # Attempt to extract literal value if possible
                    try:
                        out = pl.select(out._expr).to_series()[0]
                    except:
                        pass
                res.append(out)
            return res

        return Column(expr.map_elements(transform_2_fn, return_dtype=None))


def filter(col_name: Union[str, Column], f: Any) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # Inside eval, pl.element() is each item
    res_col = f(Column(pl.element()))
    return Column(expr.list.eval(pl.element().filter(res_col._expr)))


def exists(col_name: Union[str, Column], f: Any) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    res_col = f(Column(pl.element()))
    return Column(expr.list.eval(res_col._expr).list.any())


def forall(col_name: Union[str, Column], f: Any) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    res_col = f(Column(pl.element()))
    return Column(expr.list.eval(res_col._expr).list.all())


def aggregate(
    col_name: Union[str, Column], initialValue: Any, merge: Any, finish: Any = None
) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)

    def get_val(v):
        if isinstance(v, Column):
            try:
                return pl.select(v._expr).to_series()[0]
            except:
                return v
        return v

    init_val = get_val(initialValue)

    def agg_fn(arr):
        if arr is None:
            return None
        from functools import reduce

        res = reduce(lambda acc, x: merge(acc, x), arr, init_val)
        if finish:
            res = finish(res)
        if isinstance(res, Column):
            try:
                res = pl.select(res._expr).to_series()[0]
            except:
                pass
        return res

    return Column(expr.map_elements(agg_fn, return_dtype=None))


def zip_with(left: Union[str, Column], right: Union[str, Column], f: Any) -> Column:
    l_expr = left._expr if isinstance(left, Column) else pl.col(left)
    r_expr = right._expr if isinstance(right, Column) else pl.col(right)

    def zip_fn(s):
        if s is None:
            return None
        l_arr, r_arr = s["l"], s["r"]
        if l_arr is None or r_arr is None:
            return None
        res = []
        for x, y in zip(l_arr, r_arr):
            out = f(x, y)
            if isinstance(out, Column):
                try:
                    out = pl.select(out._expr).to_series()[0]
                except:
                    pass
            res.append(out)
        return res

    return Column(
        pl.struct([l_expr.alias("l"), r_expr.alias("r")]).map_elements(
            zip_fn, return_dtype=None
        )
    )


def transform_keys(col_name: Union[str, Column], f: Any) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # Inside eval, pl.element() is the struct {"key":..., "value":...}
    k = Column(pl.element().struct.field("key"))
    v = Column(pl.element().struct.field("value"))
    new_k = f(k, v)

    res_struct = pl.struct(
        [new_k._expr.alias("key"), pl.element().struct.field("value").alias("value")]
    )
    return Column(expr.list.eval(res_struct))


def transform_values(col_name: Union[str, Column], f: Any) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    k = Column(pl.element().struct.field("key"))
    v = Column(pl.element().struct.field("value"))
    new_v = f(k, v)

    res_struct = pl.struct(
        [pl.element().struct.field("key").alias("key"), new_v._expr.alias("value")]
    )
    return Column(expr.list.eval(res_struct))


def row_number() -> Column:
    c = Column(pl.lit("_ROW_NUMBER_"))
    c._is_row_number = True
    return c


def percent_rank() -> Column:
    c = Column(pl.lit("_PERCENT_RANK_"))
    c._is_rank = True
    c._rank_method = "percent_rank"
    return c


def cume_dist() -> Column:
    c = Column(pl.lit("_CUME_DIST_"))
    c._is_rank = True
    c._rank_method = "cume_dist"
    return c


def ntile(n: int) -> Column:
    c = Column(pl.lit("_NTILE_"))
    c._is_rank = True
    c._rank_method = "ntile"
    c._ntile_n = n
    return c


# Aggregates
def collect_list(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.implode())


def collect_set(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.implode().list.unique())


def monotonically_increasing_id() -> Column:
    return Column(pl.int_range(0, pl.len(), dtype=pl.Int64))


def spark_partition_id() -> Column:
    return Column(pl.lit(0, dtype=pl.Int32))


def pow(col_name: Union[str, Column], power: Union[float, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    p = power._expr if isinstance(power, Column) else pl.lit(power)
    return Column(expr.pow(p))


def hypot(col1: Union[str, Column], col2: Union[str, Column]) -> Column:
    expr1 = col1._expr if isinstance(col1, Column) else pl.col(col1)
    expr2 = col2._expr if isinstance(col2, Column) else pl.col(col2)
    return Column((expr1.pow(2) + expr2.pow(2)).sqrt())


# Binary / String
def base64(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # Polars base64_encode recently added
    return Column(expr.cast(pl.Binary).bin.encode("base64"))


def unbase64(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.decode("base64"))


def rank() -> Column:
    c = Column(pl.lit("_RANK_"))
    c._is_rank = True
    c._rank_method = "min"
    return c


def dense_rank() -> Column:
    c = Column(pl.lit("_DENSE_RANK_"))
    c._is_rank = True
    c._rank_method = "dense"
    return c


def lag(col_name: Union[str, Column], n: int = 1, default: Any = None) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.shift(n, fill_value=default))


def lead(col_name: Union[str, Column], n: int = 1, default: Any = None) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.shift(-n, fill_value=default))


# Logical & Conditional
def greatest(*cols: Union[str, Column]) -> Column:
    exprs = [c._expr if isinstance(c, Column) else pl.col(c) for c in cols]
    return Column(pl.max_horizontal(exprs))


def least(*cols: Union[str, Column]) -> Column:
    exprs = [c._expr if isinstance(c, Column) else pl.col(c) for c in cols]
    return Column(pl.min_horizontal(exprs))


# JSON
def get_json_object(col_name: Union[str, Column], path: str) -> Column:
    import json

    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # Simplified JSONPath: $.field -> field
    clean_path = path.lstrip("$.").replace("[", ".").replace("]", "")

    def extract(s):
        if not s:
            return None
        try:
            d = json.loads(s)
            # Basic path traversal
            for part in clean_path.split("."):
                if not part:
                    continue
                if isinstance(d, dict):
                    d = d.get(part)
                elif isinstance(d, list):
                    d = d[int(part)]
                else:
                    return None
            return str(d) if d is not None else None
        except:
            return None

    return Column(expr.map_elements(extract, return_dtype=pl.Utf8))


def to_json(col_name: Union[str, Column]) -> Column:
    import json

    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(
        expr.map_elements(
            lambda x: json.dumps(x) if x is not None else None, return_dtype=pl.Utf8
        )
    )


# Bitwise


def shiftLeft(col_name: Union[str, Column], n: int) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(
        expr.map_elements(
            lambda x: x << n if x is not None else None, return_dtype=pl.Int64
        )
    )


def shiftRight(col_name: Union[str, Column], n: int) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(
        expr.map_elements(
            lambda x: x >> n if x is not None else None, return_dtype=pl.Int64
        )
    )


def hex(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # Using map_elements for hex as there's no direct expression in Polars for all types
    return Column(
        expr.map_elements(
            lambda x: format(x, "X") if x is not None else None, return_dtype=pl.Utf8
        )
    )


def nanvl(col1: Union[str, Column], col2: Union[str, Column]) -> Column:
    expr1 = col1._expr if isinstance(col1, Column) else pl.col(col1)
    expr2 = col2._expr if isinstance(col2, Column) else pl.col(col2)
    return Column(pl.when(expr1.is_nan()).then(expr2).otherwise(expr1))


def col(col_name: str) -> Column:
    # Handle Spark-style df.col or table.col.field by parsing dots
    table_name = None
    if isinstance(col_name, str) and "." in col_name:
        parts = col_name.split(".")
        table_name = parts[0]
        # The base column is the second part
        base_col_name = parts[1]
        c = Column(pl.col(base_col_name))
        c._is_simple_col = True
        c._col_name = base_col_name
        c._table_name = table_name

        # Any subsequent parts are struct fields
        for field in parts[2:]:
            c = c.getField(field)
        return c

    c = Column(pl.col(col_name))
    c._is_simple_col = True
    c._col_name = col_name
    return c


def lit(value: Any) -> Column:
    return Column(pl.lit(value))


# Null handling
def isnull(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.is_null())


def isnotnull(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.is_not_null())


def isnan(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.is_nan())


def coalesce(*cols: Union[str, Column]) -> Column:
    exprs = [c._expr if isinstance(c, Column) else pl.col(c) for c in cols]
    return Column(pl.coalesce(exprs))


# String Functions V2
def regexp_replace(
    col_name: Union[str, Column], pattern: str, replacement: str
) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.replace_all(pattern, replacement))


def regexp_extract(
    col_name: Union[str, Column], pattern: str, group: int = 1
) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.extract(pattern, group_index=group))


def split(col_name: Union[str, Column], pattern: str) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.split(pattern))


def substring(col_name: Union[str, Column], pos: int, length: int) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # Spark is 1-indexed, Polars is 0-indexed
    return Column(expr.str.slice(pos - 1, length))


def trim(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.strip_chars())


def ltrim(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.strip_chars_start())


def rtrim(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.strip_chars_end())


# Collection Functions
def countDistinct(*cols: Union[str, Column]) -> Column:
    exprs = [c._expr if isinstance(c, Column) else pl.col(c) for c in cols]
    return Column(pl.concat_list(exprs).list.n_unique())


def approx_count_distinct(col_name: Union[str, Column], rsd: float = 0.05) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(
        expr.n_unique()
    )  # Polars is already very fast, n_unique is usually fine


def first(col_name: Union[str, Column], ignorenulls: bool = False) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    if ignorenulls:
        return Column(expr.drop_nulls().first())
    return Column(expr.first())


def last(col_name: Union[str, Column], ignorenulls: bool = False) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    if ignorenulls:
        return Column(expr.drop_nulls().last())
    return Column(expr.last())


def array(*cols: Union[str, Column]) -> Column:
    exprs = [c._expr if isinstance(c, Column) else pl.col(c) for c in cols]
    return Column(pl.concat_list(exprs))


def explode(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.explode())


def size(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.list.len())


def struct(*cols: Union[str, Column]) -> Column:
    exprs = [c._expr if isinstance(c, Column) else pl.col(c) for c in cols]
    return Column(pl.struct(exprs))


def named_struct(*args: Any) -> Column:
    # args are name1, val1, name2, val2...
    exprs = []
    for i in range(0, len(args), 2):
        name = args[i]
        val = args[i + 1]

        # If name is a Column literal, it's hard to get the value.
        # For now, let's support strings directly or Column(pl.lit(...))
        name_str = None
        if isinstance(name, str):
            name_str = name
        elif isinstance(name, Column):
            # Try a hacky way to see if it's a literal string
            # We can't easily get it.
            # Let's just use its string representation if we have to,
            # but usually users pass strings.
            # In my test I passed F.lit("x").
            # Let's try to evaluate it if it's a single literal.
            try:
                # evaluate the literal to get the name
                name_str = pl.select(name._expr).to_series()[0]
            except:
                name_str = str(name)
        else:
            name_str = str(name)

        v_expr = val._expr if isinstance(val, Column) else pl.lit(val)
        exprs.append(v_expr.alias(name_str))

    return Column(pl.struct(exprs))


def arrays_zip(*cols: Union[str, Column]) -> Column:
    exprs = []
    names = []
    for c in cols:
        if isinstance(c, Column):
            exprs.append(c._expr)
            names.append("col")
        else:
            exprs.append(pl.col(c))
            names.append(c)

    # We use a struct to group columns and map_elements to zip them
    def zip_fn(s):
        if s is None:
            return None
        # s is a dict of lists
        lists = [s[k] for k in s]
        keys = list(s.keys())
        return [dict(zip(keys, vals)) for vals in zip(*lists)]

    return Column(pl.struct(exprs).map_elements(zip_fn))


def posexplode(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)

    def pos_fn(val):
        if val is None:
            return None
        return [{"pos": i, "col": v} for i, v in enumerate(val)]

    c = Column(expr.map_elements(pos_fn).explode())
    c._is_generator = True
    return c


# Map-like operations (using list of structs)
def map_from_arrays(keys: Union[str, Column], values: Union[str, Column]) -> Column:
    k_expr = keys._expr if isinstance(keys, Column) else pl.col(keys)
    v_expr = values._expr if isinstance(values, Column) else pl.col(values)

    def zip_map(s):
        if s is None:
            return None
        # s is a dict with keys 'k' and 'v'
        return [{"key": k, "value": v} for k, v in zip(s["k"], s["v"])]

    return Column(
        pl.struct([k_expr.alias("k"), v_expr.alias("v")]).map_elements(zip_map)
    )


def map_keys(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.list.eval(pl.element().struct.field("key")))


def map_values(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.list.eval(pl.element().struct.field("value")))


# Advanced Array Operations
def element_at(col_name: Union[str, Column], index: int) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    if index > 0:
        return Column(expr.list.get(index - 1))
    else:
        return Column(expr.list.get(index))


def array_distinct(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.list.unique(maintain_order=True))


def array_intersect(col1: Union[str, Column], col2: Union[str, Column]) -> Column:
    expr1 = col1._expr if isinstance(col1, Column) else pl.col(col1)
    expr2 = col2._expr if isinstance(col2, Column) else pl.col(col2)
    return Column(expr1.list.set_intersection(expr2))


def array_except(col1: Union[str, Column], col2: Union[str, Column]) -> Column:
    expr1 = col1._expr if isinstance(col1, Column) else pl.col(col1)
    expr2 = col2._expr if isinstance(col2, Column) else pl.col(col2)
    return Column(expr1.list.set_difference(expr2))


def slice(col_name: Union[str, Column], start: int, length: int) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.list.slice(start - 1, length))


def array_union(col1: Union[str, Column], col2: Union[str, Column]) -> Column:
    expr1 = col1._expr if isinstance(col1, Column) else pl.col(col1)
    expr2 = col2._expr if isinstance(col2, Column) else pl.col(col2)
    return Column(pl.concat_list([expr1, expr2]).list.unique())


def array_remove(col_name: Union[str, Column], value: Any) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # Filter elements that are not equal to value
    return Column(expr.list.eval(pl.element().filter(pl.element() != value)))


# Phase 3: Date & Time Functions


def current_date() -> Column:
    # Polars lit(datetime.today().date()) works, or we can use pl.repeat
    from datetime import date

    return Column(pl.lit(date.today()))


def date_add(col_name: Union[str, Column], days: int) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # Polars uses .dt for datetime ops
    return Column(expr.dt.offset_by(f"{days}d"))


def date_sub(col_name: Union[str, Column], days: int) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.dt.offset_by(f"-{days}d"))


def year(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.dt.year())


def month(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.dt.month())


def dayofmonth(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.dt.day())


def datediff(end: Union[str, Column], start: Union[str, Column]) -> Column:
    end_expr = end._expr if isinstance(end, Column) else pl.col(end)
    start_expr = start._expr if isinstance(start, Column) else pl.col(start)
    # Polars subtraction results in a Duration, we need to extract days
    return Column((end_expr - start_expr).dt.total_days())


def add_months(col_name: Union[str, Column], months: int) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.dt.offset_by(f"{months}mo"))


def last_day(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.dt.month_end())


def months_between(date1: Union[str, Column], date2: Union[str, Column]) -> Column:
    expr1 = date1._expr if isinstance(date1, Column) else pl.col(date1)
    expr2 = date2._expr if isinstance(date2, Column) else pl.col(date2)
    # Simplified: (Y1-Y2)*12 + (M1-M2)
    diff = (expr1.dt.year() - expr2.dt.year()) * 12 + (
        expr1.dt.month() - expr2.dt.month()
    )
    return Column(diff.cast(pl.Float64))


def when(condition: Column, value: Any) -> "WhenClause":
    return WhenClause().when(condition, value)


class WhenClause:
    def __init__(self):
        self._cases = []
        self._otherwise = None

    def when(self, condition: Column, value: Any) -> "WhenClause":
        val_expr = value._expr if isinstance(value, Column) else pl.lit(value)
        self._cases.append((condition._expr, val_expr))
        return self

    def otherwise(self, value: Any) -> Column:
        val_expr = value._expr if isinstance(value, Column) else pl.lit(value)

        # Build polars when-then-otherwise
        expr = None
        for cond, val in reversed(self._cases):
            if expr is None:
                expr = pl.when(cond).then(val).otherwise(val_expr)
            else:
                expr = pl.when(cond).then(val).otherwise(expr)

        return Column(expr)


def count(col_name: Union[str, Column]) -> Column:
    base_expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    c = Column(base_expr.count())
    c._is_agg, c._agg_op, c._base_expr = True, "count", base_expr
    return c


def sum(col_name: Union[str, Column]) -> Column:
    base_expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    c = Column(base_expr.sum())
    c._is_agg, c._agg_op, c._base_expr = True, "sum", base_expr
    return c


def avg(col_name: Union[str, Column]) -> Column:
    base_expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    c = Column(base_expr.mean())
    c._is_agg, c._agg_op, c._base_expr = True, "mean", base_expr
    return c


def min(col_name: Union[str, Column]) -> Column:
    base_expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    c = Column(base_expr.min())
    c._is_agg, c._agg_op, c._base_expr = True, "min", base_expr
    return c


def max(col_name: Union[str, Column]) -> Column:
    base_expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    c = Column(base_expr.max())
    c._is_agg, c._agg_op, c._base_expr = True, "max", base_expr
    return c


def countDistinct(col_name: Union[str, Column]) -> Column:
    base_expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    c = Column(base_expr.n_unique())
    c._is_agg, c._agg_op, c._base_expr = True, "n_unique", base_expr
    return c


def first(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.first())


def last(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.last())


# Phase 1: Basic Math & String Functions
def abs(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.abs())


def ceil(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.ceil())


def floor(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.floor())


def sqrt(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.sqrt())


def exp(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.exp())


def log(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.log())


def upper(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.to_uppercase())


def lower(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.to_lowercase())


def length(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.len_chars())


def initcap(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.to_titlecase())


def reverse(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # reverse handles both strings and lists in Polars
    return Column(expr.str.reverse())


def lpad(col_name: Union[str, Column], length: int, pad: str = " ") -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.pad_start(length, pad))


def rpad(col_name: Union[str, Column], length: int, pad: str = " ") -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.pad_end(length, pad))


def concat(*cols: Union[str, Column]) -> Column:
    exprs = []
    for c in cols:
        if isinstance(c, Column):
            exprs.append(c._expr)
        else:
            exprs.append(pl.lit(c))
    return Column(pl.concat_str(exprs))


def instr(col_name: Union[str, Column], substring: str) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # Polars find returns null if not found or -1?
    # Let's use pl.when to match Spark's 1-indexed and 0 if not found
    pos = expr.str.find(substring)
    return Column(
        pl.when(pos.is_null() | (pos == -1)).then(pl.lit(0)).otherwise(pos + 1)
    )


def locate(substring: str, col_name: Union[str, Column], pos: int = 1) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # Support for optional start pos? Polars find doesn't take start pos directly.
    # We could slice the string, but let's keep it simple for now and ignore 'pos'
    p = expr.str.find(substring)
    return Column(pl.when(p.is_null() | (p == -1)).then(pl.lit(0)).otherwise(p + 1))


def format_number(col_name: Union[str, Column], d: int) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # Simplified: round and cast to string.
    # Real Spark format_number adds thousands separators.
    return Column(expr.round(d).cast(pl.Utf8))


# Advanced Aggregates
def skewness(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.skew())


def kurtosis(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.kurtosis())


def stddev(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.std())


def stddev_samp(col_name: Union[str, Column]) -> Column:
    return stddev(col_name)


def stddev_pop(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    # std(ddof=0) for population stddev
    return Column(expr.std(ddof=0))


def variance(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.var())


def var_samp(col_name: Union[str, Column]) -> Column:
    return variance(col_name)


def var_pop(col_name: Union[str, Column]) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.var(ddof=0))


# More Aggregates
def corr(col1: Union[str, Column], col2: Union[str, Column]) -> Column:
    expr1 = col1._expr if isinstance(col1, Column) else pl.col(col1)
    expr2 = col2._expr if isinstance(col2, Column) else pl.col(col2)
    return Column(pl.corr(expr1, expr2))


def covar_samp(col1: Union[str, Column], col2: Union[str, Column]) -> Column:
    expr1 = col1._expr if isinstance(col1, Column) else pl.col(col1)
    expr2 = col2._expr if isinstance(col2, Column) else pl.col(col2)
    return Column(pl.cov(expr1, expr2))


def covar_pop(col1: Union[str, Column], col2: Union[str, Column]) -> Column:
    expr1 = col1._expr if isinstance(col1, Column) else pl.col(col1)
    expr2 = col2._expr if isinstance(col2, Column) else pl.col(col2)
    return Column(pl.cov(expr1, expr2, ddof=0))


# More Strings
def translate(col_name: Union[str, Column], matching: str, replacement: str) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    trans_table = str.maketrans(matching, replacement)
    return Column(
        expr.map_elements(
            lambda x: x.translate(trans_table) if x is not None else None,
            return_dtype=pl.Utf8,
        )
    )


# Type Conversion


def to_date(col_name: Union[str, Column], format: Optional[str] = None) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.to_date(format=format))


def to_timestamp(col_name: Union[str, Column], format: Optional[str] = None) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.str.to_datetime(format=format))


# Collection Functions V2 Extension
def array_contains(col_name: Union[str, Column], value: Any) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.list.contains(value))


def sort_array(col_name: Union[str, Column], asc: bool = True) -> Column:
    expr = col_name._expr if isinstance(col_name, Column) else pl.col(col_name)
    return Column(expr.list.sort(descending=not asc))
