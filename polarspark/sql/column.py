import polars as pl
from typing import Union, Any, TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .window import Window

class Column:
    def __init__(self, expr: pl.Expr):
        self._expr = expr
        self._is_simple_col = False
        self._col_name = None
        self._df_ref = None
        self._table_name = None
        self._is_op = False
        self._op = None
        self._left = None
        self._right = None

    def _resolve(self, left_ref: int, right_ref: int, left_alias: str = None, right_alias: str = None,
                 left_cols: Optional[List[str]] = None, right_cols: Optional[List[str]] = None) -> pl.Expr:
        if self._is_op:
            if self._op == "alias":
                return self._left._resolve(left_ref, right_ref, left_alias, right_alias, left_cols, right_cols).alias(self._right)
            if self._op == "getField":
                return self._left._resolve(left_ref, right_ref, left_alias, right_alias, left_cols, right_cols).struct.field(self._right)
            if self._op == "cast":
                return self._left._resolve(left_ref, right_ref, left_alias, right_alias, left_cols, right_cols).cast(self._right)
            
            l = self._left._resolve(left_ref, right_ref, left_alias, right_alias, left_cols, right_cols) if isinstance(self._left, Column) else pl.lit(self._left)
            r = self._right._resolve(left_ref, right_ref, left_alias, right_alias, left_cols, right_cols) if isinstance(self._right, Column) else pl.lit(self._right)
            # ... ops ...
            ops = {
                "+": lambda a, b: a + b, "-": lambda a, b: a - b, "*": lambda a, b: a * b, "/": lambda a, b: a / b,
                "%": lambda a, b: a % b, "&": lambda a, b: a & b, "|": lambda a, b: a | b,
                "==": lambda a, b: a == b, "!=": lambda a, b: a != b,
                ">": lambda a, b: a > b, ">=": lambda a, b: a >= b, "<": lambda a, b: a < b, "<=": lambda a, b: a <= b,
            }
            if self._op in ops:
                return ops[self._op](l, r)
        
        # Resolve by ID or by Alias
        is_right = (right_ref is not None and self._df_ref == right_ref) or \
                   (right_alias and self._table_name == right_alias)
        
        if self._is_simple_col:
            if is_right:
                if left_cols and self._col_name in left_cols:
                    return pl.col(self._col_name + "_right")
                return pl.col(self._col_name)
            
            # Not right side. Could be left side or a struct access.
            if self._table_name:
                # If table_name is in left_cols, it's a struct!
                if left_cols and self._table_name in left_cols:
                    return pl.col(self._table_name).struct.field(self._col_name)
                # If it matches an alias, it's just the column
                if (left_alias and self._table_name == left_alias):
                    return pl.col(self._col_name)
                # Fallback: if we have no schema info, we can't be sure, but let's assume table prefix
                # and return the column name.
                return pl.col(self._col_name)

            return pl.col(self._col_name)
        return self._expr



    def over(self, window: "Window") -> "Column":

        expr = self._expr
        if hasattr(self, "_is_rank"):
            rank_method = self._rank_method
            if window._order_by:
                first_col = window._order_by[0]
                rank_col = pl.col(first_col) if isinstance(first_col, str) else first_col
                if rank_method == "percent_rank":
                    r = rank_col.rank(method="min")
                    cnt = pl.len()
                    expr = (r - 1) / (cnt - 1)
                elif rank_method == "cume_dist":
                    r = rank_col.rank(method="max")
                    cnt = pl.len()
                    expr = r / cnt
                elif rank_method == "ntile":
                    n = self._ntile_n
                    r = pl.int_range(0, pl.len())
                    expr = (r * n // pl.len()) + 1
                else:
                    expr = rank_col.rank(method=rank_method)
            else:
                expr = pl.lit(1)
        
        if hasattr(self, "_is_row_number"):
            expr = pl.int_range(1, pl.len() + 1, dtype=pl.Int64)

        if window._order_by:
            expr = expr.sort_by(window._order_by)
        if window._partition_by:
            expr = expr.over(window._partition_by)
            
        return Column(expr)

    def alias(self, name: str) -> "Column":
        return self._init_op(Column(self._expr.alias(name)), "alias", self, name)


    def getField(self, name: str) -> "Column":
        return self._init_op(Column(self._expr.struct.field(name)), "getField", self, name)

    def cast(self, dataType: str) -> "Column":
        mapping = {
            "string": pl.Utf8, "int": pl.Int64, "long": pl.Int64,
            "double": pl.Float64, "float": pl.Float32, "boolean": pl.Boolean,
        }
        pl_type = mapping.get(dataType.lower(), dataType)
        return self._init_op(Column(self._expr.cast(pl_type)), "cast", self, pl_type)


    def _init_op(self, res, op, left, right):
        res._is_op, res._op, res._left, res._right = True, op, left, right
        return res

    def __add__(self, other: Any) -> "Column":
        return self._init_op(Column(self._expr + (other._expr if isinstance(other, Column) else pl.lit(other))), "+", self, other)

    def __sub__(self, other: Any) -> "Column":
        return self._init_op(Column(self._expr - (other._expr if isinstance(other, Column) else pl.lit(other))), "-", self, other)

    def __mul__(self, other: Any) -> "Column":
        return self._init_op(Column(self._expr * (other._expr if isinstance(other, Column) else pl.lit(other))), "*", self, other)

    def __truediv__(self, other: Any) -> "Column":
        return self._init_op(Column(self._expr / (other._expr if isinstance(other, Column) else pl.lit(other))), "/", self, other)

    def __mod__(self, other: Any) -> "Column":
        return self._init_op(Column(self._expr % (other._expr if isinstance(other, Column) else pl.lit(other))), "%", self, other)

    def __and__(self, other: Any) -> "Column":
        return self._init_op(Column(self._expr & (other._expr if isinstance(other, Column) else pl.lit(other))), "&", self, other)

    def __or__(self, other: Any) -> "Column":
        return self._init_op(Column(self._expr | (other._expr if isinstance(other, Column) else pl.lit(other))), "|", self, other)

    def __eq__(self, other: Any) -> "Column":
        res = self._init_op(Column(self._expr == (other._expr if isinstance(other, Column) else pl.lit(other))), "==", self, other)
        if self._is_simple_col and isinstance(other, Column) and other._is_simple_col:
            res._is_equi_join_cond = True
            res._l_col, res._r_col = self._col_name, other._col_name
            res._l_ref, res._r_ref = self._df_ref, other._df_ref
            res._l_table, res._r_table = self._table_name, other._table_name
        return res


    def __ne__(self, other: Any) -> "Column":
        return self._init_op(Column(self._expr != (other._expr if isinstance(other, Column) else pl.lit(other))), "!=", self, other)

    def __gt__(self, other: Any) -> "Column":
        return self._init_op(Column(self._expr > (other._expr if isinstance(other, Column) else pl.lit(other))), ">", self, other)

    def __ge__(self, other: Any) -> "Column":
        return self._init_op(Column(self._expr >= (other._expr if isinstance(other, Column) else pl.lit(other))), ">=", self, other)

    def __lt__(self, other: Any) -> "Column":
        return self._init_op(Column(self._expr < (other._expr if isinstance(other, Column) else pl.lit(other))), "<", self, other)

    def __le__(self, other: Any) -> "Column":
        return self._init_op(Column(self._expr <= (other._expr if isinstance(other, Column) else pl.lit(other))), "<=", self, other)

    def isNull(self) -> "Column": return Column(self._expr.is_null())
    def isNotNull(self) -> "Column": return Column(self._expr.is_not_null())
    def asc(self) -> "Column": return self
    def desc(self) -> "Column": return Column(self._expr) # Polars handle sort order in sort()
    def isin(self, *cols: Any) -> "Column":
        if len(cols) == 1 and isinstance(cols[0], (list, set)): vals = list(cols[0])
        else: vals = list(cols)
        return Column(self._expr.is_in(vals))
    def when(self, condition: "Column", value: Any) -> "Column":
        from .functions import when
        return when(condition, value)
    def otherwise(self, value: Any) -> "Column":
        # This is tricky because Column(pl.Expr) doesn't know it's in a when().
        # But our when() returns a Column.
        return Column(self._expr.otherwise(value._expr if isinstance(value, Column) else pl.lit(value)))

    def __getattr__(self, name: str) -> "Column":
        if name.startswith("_"):
            raise AttributeError(f"'Column' object has no attribute '{name}'")
        return self.getField(name)

