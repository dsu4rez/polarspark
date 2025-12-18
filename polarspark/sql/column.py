import polars as pl
from typing import Union, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .window import Window

class Column:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def over(self, window: "Window") -> "Column":
        expr = self._expr
        
        # Check for rank placeholders
        is_rank = False
        rank_method = "min"
        if hasattr(expr, "meta"):
            # This is complex to check in Polars without evaluating.
            # I'll use a simpler check: if it's a literal string with a special prefix.
            pass
        
        # Simplified: Check if expression is one of our placeholders
        try:
            lit_val = expr.meta.output_name() # Not always available
        except:
            lit_val = None

        # Let's use a more robust way: check a custom attribute we set in functions.py
        if hasattr(self, "_is_rank"):
            rank_method = self._rank_method
            # Spark requires order by for most ranking functions.
            if window._order_by:
                # Use the first order by column
                first_col = window._order_by[0]
                rank_col = pl.col(first_col) if isinstance(first_col, str) else first_col
                
                if rank_method == "percent_rank":
                    # (rank - 1) / (count - 1)
                    r = rank_col.rank(method="min")
                    cnt = pl.len()
                    expr = (r - 1) / (cnt - 1)
                elif rank_method == "cume_dist":
                    # rank / count
                    r = rank_col.rank(method="max") # Spark uses max rank for cume_dist
                    cnt = pl.len()
                    expr = r / cnt
                elif rank_method == "ntile":
                    # ntile(n)
                    n = self._ntile_n
                    # row_number-like rank
                    r = pl.int_range(0, pl.len())
                    expr = (r * n // pl.len()) + 1
                else:
                    expr = rank_col.rank(method=rank_method)
            else:
                expr = pl.lit(1)
        
        # Special case for row_number() if it's our placeholder
        if hasattr(self, "_is_row_number"):
            expr = pl.int_range(1, pl.len() + 1, dtype=pl.Int64)

        if window._order_by:
            expr = expr.sort_by(window._order_by)
        
        if window._partition_by:
            expr = expr.over(window._partition_by)

            
        return Column(expr)




    def alias(self, name: str) -> "Column":
        return Column(self._expr.alias(name))

    def getField(self, name: str) -> "Column":
        # Polars handles struct field access via .struct.field()
        return Column(self._expr.struct.field(name))

    def cast(self, dataType: str) -> "Column":

        # Simplified mapping
        mapping = {
            "string": pl.Utf8,
            "int": pl.Int64,
            "long": pl.Int64,
            "double": pl.Float64,
            "float": pl.Float32,
            "boolean": pl.Boolean,
        }
        pl_type = mapping.get(dataType.lower(), dataType)
        return Column(self._expr.cast(pl_type))

    def __add__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr + other_expr)

    def __sub__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr - other_expr)

    def __mul__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr * other_expr)

    def __truediv__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr / other_expr)

    def __mod__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr % other_expr)

    def __and__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr & other_expr)

    def __or__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr | other_expr)

    def __eq__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr == other_expr)

    def __ne__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr != other_expr)

    def __gt__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr > other_expr)

    def __ge__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr >= other_expr)

    def __lt__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr < other_expr)

    def __le__(self, other: Any) -> "Column":
        other_expr = other._expr if isinstance(other, Column) else pl.lit(other)
        return Column(self._expr <= other_expr)

    def desc(self) -> "Column":
        return Column(self._expr.sort(descending=True))

    def asc(self) -> "Column":
        return Column(self._expr.sort(descending=False))

    def isNull(self) -> "Column":
        return Column(self._expr.is_null())

    def isNotNull(self) -> "Column":
        return Column(self._expr.is_not_null())

    def isin(self, *vals: Any) -> "Column":
        if len(vals) == 1 and isinstance(vals[0], (list, tuple, set)):
            vals = list(vals[0])
        else:
            vals = list(vals)
        return Column(self._expr.is_in(vals))

